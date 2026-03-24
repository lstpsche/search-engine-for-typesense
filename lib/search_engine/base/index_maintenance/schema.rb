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
            step = SearchEngine::Logging::StepLine.new('Update Collection')
            step.update('analyzing diff')
            updated = SearchEngine::Schema.update!(self, client: client)

            if updated
              step.finish('updated in-place (PATCH)')
            else
              step.skip('no changes or incompatible')
            end
            updated
          ensure
            step&.close
          end

          def drop_collection!
            client = SearchEngine.client
            logical = respond_to?(:collection) ? collection.to_s : name.to_s

            alias_target = client.resolve_alias(logical, timeout_ms: 10_000)
            physical = if alias_target && !alias_target.to_s.strip.empty?
                         alias_target.to_s
                       else
                         live = client.retrieve_collection_schema(logical, timeout_ms: 10_000)
                         live ? logical : nil
                       end

            step = SearchEngine::Logging::StepLine.new('Drop Collection')
            if physical.nil?
              step.skip('not present')
              return
            end

            puts
            puts(SearchEngine::Logging::Color.header(%(>>>>>> Dropping Collection "#{logical}")))
            step.update("dropping (logical=#{logical} physical=#{physical})")
            client.delete_collection(physical, timeout_ms: 60_000)
            step.finish('done')
            puts(SearchEngine::Logging::Color.header(%(>>>>>> Dropped Collection "#{logical}")))
            nil
          ensure
            step&.close
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

            step = SearchEngine::Logging::StepLine.new('Recreate Collection')
            if physical
              step.update("dropping existing (logical=#{logical} physical=#{physical})")
              client.delete_collection(physical)
            else
              step.update("creating (logical=#{logical})")
            end

            schema = SearchEngine::Schema.compile(self)
            step.update("creating with schema (logical=#{logical})")
            client.create_collection(schema)
            step.finish('done')
            nil
          ensure
            step&.close
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
