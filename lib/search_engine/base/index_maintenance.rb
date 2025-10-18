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

          def __se_retention_cleanup!(*_)
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
require 'active_support/concern'
require 'search_engine/base/index_maintenance/cleanup'
require 'search_engine/base/index_maintenance/lifecycle'
require 'search_engine/base/index_maintenance/schema'

module SearchEngine
  class Base
    # Index lifecycle helpers: applying schema, indexing, retention cleanup.
    module IndexMaintenance
      extend ActiveSupport::Concern

      include IndexMaintenance::Cleanup
      include IndexMaintenance::Lifecycle
      include IndexMaintenance::Schema

      class_methods do
        # ---------------------- Preflight dependencies ----------------------
        # Recursively ensure/index direct and transitive belongs_to dependencies
        # before indexing the current collection.
        # @param mode [Symbol] :ensure (only missing) or :index (missing + drift)
        # @param client [SearchEngine::Client]
        # @param visited [Set<String>, nil]
        # @return [void]
        def __se_preflight_dependencies!(mode:, client:, visited: nil)
          return unless mode

          visited ||= Set.new
          current = __se_current_collection_name
          return if current.to_s.strip.empty?
          return if visited.include?(current)

          visited.add(current)

          configs = __se_fetch_joins_config
          deps = __se_belongs_to_dependencies(configs)
          return if deps.empty?

          puts
          puts(%(>>>>>> Preflight Dependencies (mode: #{mode})))

          deps.each do |cfg|
            dep_coll = (cfg[:collection] || cfg['collection']).to_s
            next if __se_skip_dep?(dep_coll, visited)

            dep_klass = __se_resolve_dep_class(dep_coll)

            if dep_klass.nil?
              puts(%(  "#{dep_coll}" → skipped (unregistered)))
              visited.add(dep_coll)
              next
            end

            # Recurse first to ensure deeper dependencies are handled
            __se_preflight_recurse(dep_klass, mode, client, visited)

            diff = __se_diff_for(dep_klass, client)
            missing, drift = __se_dependency_status(diff, dep_klass)

            __se_handle_preflight_action(mode, dep_coll, missing, drift, dep_klass, client)

            visited.add(dep_coll)
          end

          puts('>>>>>> Preflight Done')
        end

        # @return [String] current collection logical name; empty string when unavailable
        def __se_current_collection_name
          respond_to?(:collection) ? (collection || '').to_s : name.to_s
        rescue StandardError
          name.to_s
        end

        # @return [Hash] raw joins configuration or empty hash on errors
        def __se_fetch_joins_config
          joins_config || {}
        rescue StandardError
          {}
        end

        # @param configs [Hash]
        # @return [Array<Hash>] only belongs_to-type dependency configs
        def __se_belongs_to_dependencies(configs)
          values = begin
            configs.values
          rescue StandardError
            []
          end
          values.select { |c| (c[:kind] || c['kind']).to_s == 'belongs_to' }
        end

        # @param dep_coll [String]
        # @param visited [Set<String>]
        # @return [Boolean]
        def __se_skip_dep?(dep_coll, visited)
          dep_coll.to_s.strip.empty? || visited.include?(dep_coll)
        end

        # @param dep_coll [String]
        # @return [Class, nil]
        def __se_resolve_dep_class(dep_coll)
          SearchEngine.collection_for(dep_coll)
        rescue StandardError
          nil
        end

        # @param dep_klass [Class]
        # @param mode [Symbol]
        # @param client [SearchEngine::Client]
        # @param visited [Set<String>]
        # @return [void]
        def __se_preflight_recurse(dep_klass, mode, client, visited)
          dep_klass.__se_preflight_dependencies!(mode: mode, client: client, visited: visited)
        rescue StandardError
          # ignore recursion errors to not block main flow
        end

        # @param dep_klass [Class]
        # @param client [SearchEngine::Client]
        # @return [Hash]
        def __se_diff_for(dep_klass, client)
          SearchEngine::Schema.diff(dep_klass, client: client)[:diff] || {}
        rescue StandardError
          {}
        end

        # @param diff [Hash]
        # @param dep_klass [Class]
        # @return [Array(Boolean, Boolean)]
        def __se_dependency_status(diff, dep_klass)
          missing = begin
            dep_klass.__se_schema_missing?(diff)
          rescue StandardError
            false
          end
          drift = begin
            dep_klass.__se_schema_drift?(diff)
          rescue StandardError
            false
          end
          [missing, drift]
        end

        # @param mode [Symbol]
        # @param dep_coll [String]
        # @param missing [Boolean]
        # @param drift [Boolean]
        # @param dep_klass [Class]
        # @param client [SearchEngine::Client]
        # @return [void]
        def __se_handle_preflight_action(mode, dep_coll, missing, drift, dep_klass, client)
          case mode.to_s
          when 'ensure'
            if missing
              puts(%(  "#{dep_coll}" → ensure (missing) → indexate))
              # Avoid nested preflight to prevent redundant recursion cycles
              dep_klass.indexate(client: client)
            else
              puts(%(  "#{dep_coll}" → present (skip)))
            end
          when 'index'
            if missing || drift
              reason = missing ? 'missing' : 'drift'
              puts(%(  "#{dep_coll}" → index (#{reason}) → indexate))
              # Avoid nested preflight to prevent redundant recursion cycles
              dep_klass.indexate(client: client)
            else
              puts(%(  "#{dep_coll}" → in_sync (skip)))
            end
          else
            puts(%(  "#{dep_coll}" → skipped (unknown mode: #{mode})))
          end
        end
        private :__se_current_collection_name,
                :__se_fetch_joins_config,
                :__se_belongs_to_dependencies,
                :__se_skip_dep?,
                :__se_resolve_dep_class,
                :__se_preflight_recurse,
                :__se_diff_for,
                :__se_dependency_status,
                :__se_handle_preflight_action
      end

      class_methods do
        # ----------------------------- Helpers ---------------------------
        # rubocop:disable Metrics/PerceivedComplexity
        def __se_cascade_after_indexation!(context: :full)
          puts
          puts(%(>>>>>> Cascade Referencers))
          results = SearchEngine::Cascade.cascade_reindex!(source: self, ids: nil, context: context)
          outcomes = Array(results[:outcomes])
          if outcomes.empty?
            puts('  none')
          else
            outcomes.each do |o|
              coll = o[:collection] || o['collection']
              mode = (o[:mode] || o['mode']).to_s
              case mode
              when 'partial'
                puts(%(  Referencer "#{coll}" → partial reindex))
              when 'full'
                puts(%(  Referencer "#{coll}" → full reindex))
              when 'skipped_unregistered'
                puts(%(  Referencer "#{coll}" → skipped (unregistered)))
              when 'skipped_cycle'
                puts(%(  Referencer "#{coll}" → skipped (cycle)))
              else
                puts(%(  Referencer "#{coll}" → #{mode}))
              end
            end
          end
          puts('>>>>>> Cascade Done')
        rescue StandardError => error
          # Provide more verbose error output for debugging
          base = "Cascade — error=#{error.class}: #{error.message.to_s[0, 200]}"
          if error.respond_to?(:status) || error.respond_to?(:body)
            status = begin
              error.respond_to?(:status) ? error.status : nil
            rescue StandardError
              nil
            end
            body_preview = begin
              b = error.respond_to?(:body) ? error.body : nil
              if b.is_a?(String)
                b[0, 500]
              elsif b.is_a?(Hash)
                b.inspect[0, 500]
              else
                b.to_s[0, 500]
              end
            rescue StandardError
              nil
            end
            warn([base, ("status=#{status}" if status), ("body=#{body_preview}" if body_preview)].compact.join(' '))
          else
            warn(base)
          end
        end
        # rubocop:enable Metrics/PerceivedComplexity

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
      class_methods do
        def __se_extract_sample_error(summary)
          failed = begin
            summary.respond_to?(:failed_total) ? summary.failed_total.to_i : 0
          rescue StandardError
            0
          end
          return nil if failed <= 0

          batches = begin
            summary.respond_to?(:batches) ? summary.batches : nil
          rescue StandardError
            nil
          end
          return nil unless batches.is_a?(Array)

          batches.each do |b|
            next unless b.is_a?(Hash)

            samples = b[:errors_sample] || b['errors_sample']
            next if samples.nil?

            Array(samples).each do |m|
              s = m.to_s
              return s unless s.strip.empty?
            end
          end
          nil
        end
      end

      class_methods do
        def __se_index_partitions!(into:)
          compiled = SearchEngine::Partitioner.for(self)
          if compiled
            parts = Array(compiled.partitions)
            max_p = compiled.max_parallel.to_i
            return __se_index_partitions_seq!(parts, into) if max_p <= 1 || parts.size <= 1

            __se_index_partitions_parallel!(parts, into, max_p)
          else
            summary = SearchEngine::Indexer.rebuild_partition!(self, partition: nil, into: into)
            sample_err = __se_extract_sample_error(summary)
            puts(
              "  single → status=#{summary.status} docs=#{summary.docs_total} " \
              "failed=#{summary.failed_total} batches=#{summary.batches_total} " \
              "duration_ms=#{summary.duration_ms_total}" \
              "#{sample_err ? " sample_error=#{sample_err.inspect}" : ''}"
            )
            summary.status
          end
        end
      end

      class_methods do
        # Sequential processing of partition list
        def __se_index_partitions_seq!(parts, into)
          agg = :ok
          parts.each do |part|
            summary = SearchEngine::Indexer.rebuild_partition!(self, partition: part, into: into)
            puts(SearchEngine::Logging::PartitionProgress.line(part, summary))
            begin
              st = summary.status
              if st == :failed
                agg = :failed
              elsif st == :partial && agg == :ok
                agg = :partial
              end
            rescue StandardError
              agg = :failed
            end
          end
          agg
        end
      end

      class_methods do
        # Parallel processing via bounded thread pool
        def __se_index_partitions_parallel!(parts, into, max_p)
          require 'concurrent-ruby'
          pool = Concurrent::FixedThreadPool.new(max_p)
          ctx = SearchEngine::Instrumentation.context
          mtx = Mutex.new
          agg = :ok
          begin
            parts.each do |part|
              pool.post do
                SearchEngine::Instrumentation.with_context(ctx) do
                  summary = SearchEngine::Indexer.rebuild_partition!(self, partition: part, into: into)
                  mtx.synchronize do
                    puts(SearchEngine::Logging::PartitionProgress.line(part, summary))
                    begin
                      st = summary.status
                      if st == :failed
                        agg = :failed
                      elsif st == :partial && agg == :ok
                        agg = :partial
                      end
                    rescue StandardError
                      agg = :failed
                    end
                  end
                end
              rescue StandardError => error
                mtx.synchronize do
                  warn("  partition=#{part.inspect} → error=#{error.class}: #{error.message.to_s[0, 200]}")
                  agg = :failed
                end
              end
            end
          ensure
            pool.shutdown
            # Wait up to 1 hour, then force-kill and wait a bit more to ensure cleanup
            pool.wait_for_termination(3600) || pool.kill
            pool.wait_for_termination(60)
          end
          agg
        end
      end

      class_methods do
        # Single non-partitioned pass helper
        def __se_index_single!(into)
          summary = SearchEngine::Indexer.rebuild_partition!(self, partition: nil, into: into)
          sample_err = __se_extract_sample_error(summary)
          puts(
            "  single → status=#{summary.status} docs=#{summary.docs_total} " \
            "failed=#{summary.failed_total} batches=#{summary.batches_total} " \
            "duration_ms=#{summary.duration_ms_total}" \
            "#{sample_err ? " sample_error=#{sample_err.inspect}" : ''}"
          )
        end
      end

      class_methods do
        def __se_retention_cleanup!(logical:, client:)
          keep = begin
            local = respond_to?(:schema_retention) ? (schema_retention || {}) : {}
            lk = local[:keep_last]
            lk.nil? ? SearchEngine.config.schema.retention.keep_last : Integer(lk)
          rescue StandardError
            SearchEngine.config.schema.retention.keep_last
          end
          keep = 0 if keep.nil? || keep.to_i.negative?

          alias_target = client.resolve_alias(logical)
          names = Array(client.list_collections).map { |c| (c[:name] || c['name']).to_s }
          re = /^#{Regexp.escape(logical)}_\d{8}_\d{6}_\d{3}$/
          physicals = names.select { |n| re.match?(n) }

          ordered = physicals.sort_by do |n|
            ts = __se_extract_timestamp(logical, n)
            seq = __se_extract_sequence(n)
            [-ts, -seq]
          end

          candidates = ordered.reject { |n| n == alias_target }
          to_drop = candidates.drop(keep)
          to_drop.each { |n| client.delete_collection(n) }
          to_drop
        end
        private :__se_retention_cleanup!
      end

      class_methods do
        def __se_extract_timestamp(logical, name)
          base = name.to_s.delete_prefix("#{logical}_")
          parts = base.split('_')
          return 0 unless parts.size == 3

          (parts[0] + parts[1]).to_i
        end

        def __se_extract_sequence(name)
          name.to_s.split('_').last.to_i
        end
      end
    end
  end
end
