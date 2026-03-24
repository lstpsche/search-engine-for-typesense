# frozen_string_literal: true

module SearchEngine
  class Base
    module IndexMaintenance
      # Schema lifecycle helpers (ensure/apply/drop/prune).
      module Schema
        extend ActiveSupport::Concern

        class_methods do
          def schema
            SearchEngine::Schema.compile(self)
          end

          def current_schema
            client = SearchEngine.client
            logical = respond_to?(:collection) ? collection.to_s : name.to_s
            physical = client.resolve_alias(logical) || logical
            client.retrieve_collection_schema(physical)
          end

          def schema_diff
            client = SearchEngine.client
            res = SearchEngine::Schema.diff(self, client: client)
            res[:diff]
          end

          def update_collection!
            client = SearchEngine.client

            status_word = SearchEngine::Logging::Color.bold('analyzing diff for in-place update...')
            puts "Update Collection — #{status_word}"
            updated = SearchEngine::Schema.update!(self, client: client)

            if updated
              status_word = SearchEngine::Logging::Color.apply('schema updated in-place (PATCH)', :green)
              puts "Update Collection — #{status_word}"
            else
              msg = SearchEngine::Logging::Color.dim(
                'Update Collection — in-place update not possible (no changes or incompatible)'
              )
              puts(msg)
            end
            updated
          end

          def drop_collection!
            client = SearchEngine.client
            logical = respond_to?(:collection) ? collection.to_s : name.to_s

            # Resolve alias with a safer timeout for control-plane operations
            alias_target = client.resolve_alias(logical, timeout_ms: 10_000)
            physical = if alias_target && !alias_target.to_s.strip.empty?
                         alias_target.to_s
                       else
                         live = client.retrieve_collection_schema(logical, timeout_ms: 10_000)
                         live ? logical : nil
                       end

            if physical.nil?
              puts(SearchEngine::Logging::Color.dim('Drop Collection — skip (not present)'))
              return
            end

            puts
            header = SearchEngine::Logging::Color.header(%(>>>>>> Dropping Collection "#{logical}"))
            puts(header)
            status_word = SearchEngine::Logging::Color.bold('processing')
            puts("Drop Collection — #{status_word} (logical=#{logical} physical=#{physical})")
            # Use an extended timeout to accommodate large collection drops
            client.delete_collection(physical, timeout_ms: 60_000)
            puts("Drop Collection — #{SearchEngine::Logging::Color.apply('done', :green)}")
            puts(SearchEngine::Logging::Color.header(%(>>>>>> Dropped Collection "#{logical}")))
            nil
          end

          def recreate_collection!
            client = SearchEngine.client
            logical = respond_to?(:collection) ? collection.to_s : name.to_s

            alias_target = client.resolve_alias(logical)
            physical = if alias_target && !alias_target.to_s.strip.empty?
                         alias_target.to_s
                       else
                         live = client.retrieve_collection_schema(logical)
                         live ? logical : nil
                       end

            if physical
              status_word = SearchEngine::Logging::Color.apply('dropping existing', :yellow)
              puts("Recreate Collection — #{status_word} (logical=#{logical} physical=#{physical})")
              client.delete_collection(physical)
            else
              msg = SearchEngine::Logging::Color.dim('Recreate Collection — no existing collection (skip drop)')
              puts(msg)
            end

            schema = SearchEngine::Schema.compile(self)
            status_word = SearchEngine::Logging::Color.bold('creating collection with schema')
            puts("Recreate Collection — #{status_word} (logical=#{logical})")
            client.create_collection(schema)
            puts("Recreate Collection — #{SearchEngine::Logging::Color.apply('done', :green)}")
            nil
          end

          def __se_retention_cleanup!(_logical:, _client:)
            SearchEngine::Schema.prune_history!(self)
          end

          def __se_schema_missing?(diff)
            opts = diff[:collection_options]
            opts.is_a?(Hash) && opts[:live] == :missing
          end

          def __se_schema_drift?(diff)
            added = Array(diff[:added_fields])
            removed = Array(diff[:removed_fields])
            changed = (diff[:changed_fields] || {}).to_h
            coll_opts = (diff[:collection_options] || {}).to_h
            stale_refs = Array(diff[:stale_references])
            added.any? || removed.any? || !changed.empty? || !coll_opts.empty? || stale_refs.any?
          end
        end
      end
    end
  end
end
