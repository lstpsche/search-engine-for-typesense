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
            client = (SearchEngine.config.respond_to?(:client) && SearchEngine.config.client) || SearchEngine::Client.new
            logical = respond_to?(:collection) ? collection.to_s : name.to_s
            physical = client.resolve_alias(logical) || logical
            client.retrieve_collection_schema(physical)
          end

          def schema_diff
            client = (SearchEngine.config.respond_to?(:client) && SearchEngine.config.client) || SearchEngine::Client.new
            res = SearchEngine::Schema.diff(self, client: client)
            res[:diff]
          end

          def drop_collection!
            client = (SearchEngine.config.respond_to?(:client) && SearchEngine.config.client) || SearchEngine::Client.new
            logical = respond_to?(:collection) ? collection.to_s : name.to_s

            alias_target = client.resolve_alias(logical)
            physical = if alias_target && !alias_target.to_s.strip.empty?
                         alias_target.to_s
                       else
                         live = client.retrieve_collection_schema(logical)
                         live ? logical : nil
                       end

            if physical.nil?
              puts('Drop Collection — skip (not present)')
              return
            end

            puts
            puts(%(>>>>>> Dropping Collection "#{logical}"))
            puts("Drop Collection — processing (logical=#{logical} physical=#{physical})")
            client.delete_collection(physical)
            puts('Drop Collection — done')
            puts(%(>>>>>> Dropped Collection "#{logical}"))
            nil
          end

          def recreate_collection!
            client = (SearchEngine.config.respond_to?(:client) && SearchEngine.config.client) || SearchEngine::Client.new
            logical = respond_to?(:collection) ? collection.to_s : name.to_s

            alias_target = client.resolve_alias(logical)
            physical = if alias_target && !alias_target.to_s.strip.empty?
                         alias_target.to_s
                       else
                         live = client.retrieve_collection_schema(logical)
                         live ? logical : nil
                       end

            if physical
              puts("Recreate Collection — dropping existing (logical=#{logical} physical=#{physical})")
              client.delete_collection(physical)
            else
              puts('Recreate Collection — no existing collection (skip drop)')
            end

            schema = SearchEngine::Schema.compile(self)
            puts("Recreate Collection — creating collection with schema (logical=#{logical})")
            client.create_collection(schema)
            puts('Recreate Collection — done')
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
            added.any? || removed.any? || !changed.empty? || !coll_opts.empty?
          end
        end
      end
    end
  end
end
