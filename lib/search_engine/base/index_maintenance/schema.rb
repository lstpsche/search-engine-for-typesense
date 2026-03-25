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
            has_alias = alias_target && !alias_target.to_s.strip.empty?

            physicals = __se_list_all_physicals(logical, client)
            bare_schema = client.retrieve_collection_schema(logical, timeout_ms: 10_000)

            step = SearchEngine::Logging::StepLine.new('Drop Collection')
            if !has_alias && physicals.empty? && bare_schema.nil?
              step.skip('not present')
              return
            end

            puts
            puts(SearchEngine::Logging::Color.header(%(>>>>>> Dropping Collection "#{logical}")))

            physicals.each do |name|
              step.update("dropping physical #{name}")
              client.delete_collection(name, timeout_ms: 60_000)
            end

            if bare_schema && !physicals.include?(logical)
              step.update("dropping bare collection #{logical}")
              client.delete_collection(logical, timeout_ms: 60_000)
            end

            if has_alias
              step.update("deleting alias #{logical}")
              client.delete_alias(logical)
            end

            step.finish("done (physicals=#{physicals.size})")
            puts(SearchEngine::Logging::Color.header(%(>>>>>> Dropped Collection "#{logical}")))
            nil
          ensure
            step&.close
          end

          # @return [Array<String>] physical collection names matching the logical pattern
          def __se_list_all_physicals(logical, client)
            meta_timeout = begin
              t = SearchEngine.config.timeout_ms.to_i
              [t, 10_000].max
            rescue StandardError
              10_000
            end

            all_collections = Array(client.list_collections(timeout_ms: meta_timeout))
            names = all_collections.map { |c| (c[:name] || c['name']).to_s }
            re = /\A#{Regexp.escape(logical)}_\d{8}_\d{6}_\d{3}\z/
            names.select { |n| re.match?(n) }
          rescue StandardError
            []
          end

          private :__se_list_all_physicals

          def recreate_collection!
            client = SearchEngine.client
            logical = respond_to?(:collection) ? collection.to_s : name.to_s

            alias_target = client.resolve_alias(logical)
            has_alias = alias_target && !alias_target.to_s.strip.empty?
            physicals = __se_list_all_physicals(logical, client)
            bare_schema = client.retrieve_collection_schema(logical)

            step = SearchEngine::Logging::StepLine.new('Recreate Collection')
            if has_alias || physicals.any? || bare_schema
              step.update("dropping existing (logical=#{logical})")
              physicals.each { |name| client.delete_collection(name) }
              client.delete_collection(logical) if bare_schema && !physicals.include?(logical)
              client.delete_alias(logical) if has_alias
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

          # Drop orphaned physical collections for this model's logical collection.
          # @return [Hash] { dropped: Array<String>, kept: Array<String>, total_scanned: Integer }
          def prune_orphans!
            logical = respond_to?(:collection) ? collection.to_s : name.to_s
            SearchEngine::Schema.prune_orphans!(logical: logical)
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
