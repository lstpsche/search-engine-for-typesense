# frozen_string_literal: true

require 'active_support/concern'
require 'search_engine/base/index_maintenance/cleanup'
require 'search_engine/base/index_maintenance/lifecycle'
require 'search_engine/base/index_maintenance/schema'
require 'search_engine/logging/color'

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
              puts(%(  "#{dep_coll}" → ensure (missing) → index_collection))
              # Avoid nested preflight to prevent redundant recursion cycles
              dep_klass.index_collection(client: client)
            else
              puts(%(  "#{dep_coll}" → present (skip)))
            end
          when 'index'
            if missing || drift
              reason = missing ? 'missing' : 'drift'
              puts(%(  "#{dep_coll}" → index (#{reason}) → index_collection))
              # Avoid nested preflight to prevent redundant recursion cycles
              dep_klass.index_collection(client: client)
            else
              puts(%(  "#{dep_coll}" → in_sync (skip)))
            end
          else
            puts(%(  "#{dep_coll}" → skipped (unknown mode: #{mode})))
          end
        end

        def __se_log_batches_from_summary(batches)
          return unless batches.is_a?(Array)

          batches.each_with_index do |batch_stats, idx|
            batch_number = idx + 1
            batch_status = __se_batch_status_from_stats(batch_stats)
            status_color = SearchEngine::Logging::Color.for_status(batch_status)

            prefix = batch_number == 1 ? '  single → ' : '          '
            line = +prefix
            line << SearchEngine::Logging::Color.apply("status=#{batch_status}", status_color) << ' '
            docs_count = batch_stats[:docs_count] || batch_stats['docs_count'] || 0
            line << "docs=#{docs_count}" << ' '
            success_count = (batch_stats[:success_count] || batch_stats['success_count'] || 0).to_i
            success_str = "success=#{success_count}"
            line << (
              success_count.positive? ? SearchEngine::Logging::Color.apply(success_str, :green) : success_str
            ) << ' '
            failed_count = (batch_stats[:failure_count] || batch_stats['failure_count'] || 0).to_i
            failed_str = "failed=#{failed_count}"
            line << (
              failed_count.positive? ? SearchEngine::Logging::Color.apply(failed_str, :red) : failed_str
            ) << ' '
            line << "batch=#{batch_number} "
            duration_ms = batch_stats[:duration_ms] || batch_stats['duration_ms'] || 0.0
            line << "duration_ms=#{duration_ms}"

            # Extract sample error from batch stats
            sample_err = __se_extract_batch_sample_error(batch_stats)
            line << " sample_error=#{sample_err.inspect}" if sample_err

            puts(line)
          end
        end

        def __se_batch_status_from_stats(stats)
          success_count = (stats[:success_count] || stats['success_count'] || 0).to_i
          failure_count = (stats[:failure_count] || stats['failure_count'] || 0).to_i

          if failure_count.positive? && success_count.positive?
            :partial
          elsif failure_count.positive?
            :failed
          else
            :ok
          end
        end

        def __se_extract_batch_sample_error(stats)
          samples = stats[:errors_sample] || stats['errors_sample']
          return nil unless samples.is_a?(Array) && samples.any?

          samples.each do |msg|
            s = msg.to_s
            return s unless s.strip.empty?
          end
          nil
        end

        private :__se_current_collection_name,
                :__se_fetch_joins_config,
                :__se_belongs_to_dependencies,
                :__se_skip_dep?,
                :__se_resolve_dep_class,
                :__se_preflight_recurse,
                :__se_diff_for,
                :__se_dependency_status,
                :__se_handle_preflight_action,
                :__se_log_batches_from_summary,
                :__se_batch_status_from_stats,
                :__se_extract_batch_sample_error
      end

      class_methods do
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
            # Log batches individually if there are multiple batches
            __se_log_batches_from_summary(summary.batches) if summary.batches_total.to_i > 1
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
                    # Log batches individually if there are multiple batches
                    __se_log_batches_from_summary(summary.batches) if summary.batches_total.to_i > 1
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
          SearchEngine::Indexer.rebuild_partition!(self, partition: nil, into: into)
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

          meta_timeout = begin
            t = SearchEngine.config.timeout_ms.to_i
            t = 5_000 if t <= 0
            t < 10_000 ? 10_000 : t
          rescue StandardError
            10_000
          end

          alias_target = client.resolve_alias(logical, timeout_ms: meta_timeout)
          names = Array(client.list_collections(timeout_ms: meta_timeout)).map { |c| (c[:name] || c['name']).to_s }
          re = /^#{Regexp.escape(logical)}_\d{8}_\d{6}_\d{3}$/
          physicals = names.select { |n| re.match?(n) }

          ordered = physicals.sort_by do |n|
            ts = __se_extract_timestamp(logical, n)
            seq = __se_extract_sequence(n)
            [-ts, -seq]
          end

          candidates = ordered.reject { |n| n == alias_target }
          to_drop = candidates.drop(keep)
          to_drop.each { |n| client.delete_collection(n, timeout_ms: 60_000) }
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
