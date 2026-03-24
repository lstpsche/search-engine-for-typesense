# frozen_string_literal: true

require 'active_support/concern'
require 'search_engine/base/index_maintenance/cleanup'
require 'search_engine/base/index_maintenance/lifecycle'
require 'search_engine/base/index_maintenance/schema'
require 'search_engine/logging/color'
require 'search_engine/logging/batch_line'
require 'search_engine/logging/step_line'

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
        # @param depth [Integer] recursion depth for logging
        # @return [void]
        def __se_preflight_dependencies!(mode:, client:, visited: nil, depth: 0)
          return unless mode

          visited ||= Set.new
          current = __se_current_collection_name
          return if current.to_s.strip.empty?
          return if visited.include?(current)

          visited.add(current)

          configs = __se_fetch_joins_config
          deps = __se_belongs_to_dependencies(configs)
          return if deps.empty?

          indent = '  ' * depth
          puts if depth.zero?
          header = SearchEngine::Logging::Color.header(
            %(#{indent}>>>>>> Preflight Dependencies (mode: #{mode}, collection: "#{current}"))
          )
          puts(header)

          deps.each do |cfg|
            dep_coll = (cfg[:collection] || cfg['collection']).to_s
            next if __se_skip_dep?(dep_coll, visited)

            dep_klass = __se_resolve_dep_class(dep_coll)

            if dep_klass.nil?
              puts(SearchEngine::Logging::Color.dim(%(#{indent}  "#{dep_coll}" → skipped (unregistered))))
              visited.add(dep_coll)
              next
            end

            diff = __se_diff_for(dep_klass, client)
            missing, drift = __se_dependency_status(diff, dep_klass)

            should_index = case mode.to_s
                           when 'ensure' then missing
                           when 'index' then missing || drift
                           else false
                           end

            # Only recurse when we are about to index this dependency.
            __se_preflight_recurse(dep_klass, mode, client, visited, depth + 1) if should_index

            __se_handle_preflight_action(mode, dep_coll, missing, drift, dep_klass, client, indent: "#{indent}  ")

            visited.add(dep_coll)
          end

          puts(SearchEngine::Logging::Color.header(%(#{indent}>>>>>> Preflight Done (collection: "#{current}"))))
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
        # @param depth [Integer]
        # @return [void]
        def __se_preflight_recurse(dep_klass, mode, client, visited, depth)
          dep_klass.__se_preflight_dependencies!(mode: mode, client: client, visited: visited, depth: depth)
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
        # @param indent [String]
        # @return [void]
        def __se_handle_preflight_action(mode, dep_coll, missing, drift, dep_klass, client, indent: '  ')
          case mode.to_s
          when 'ensure'
            if missing
              status_word = SearchEngine::Logging::Color.apply('ensure (missing)', :yellow)
              puts(%(#{indent}"#{dep_coll}" → #{status_word} → index_collection))
              # Avoid nested preflight to prevent redundant recursion cycles
              SearchEngine::Instrumentation.with_context(bulk_suppress_cascade: true) do
                dep_klass.index_collection(client: client)
              end
            else
              puts(SearchEngine::Logging::Color.dim(%(#{indent}"#{dep_coll}" → present (skip))))
            end
          when 'index'
            if missing || drift
              reason = missing ? 'missing' : 'drift'
              status_word = SearchEngine::Logging::Color.apply("index (#{reason})", :yellow)
              puts(%(#{indent}"#{dep_coll}" → #{status_word} → index_collection))
              # Avoid nested preflight to prevent redundant recursion cycles
              SearchEngine::Instrumentation.with_context(bulk_suppress_cascade: true) do
                dep_klass.index_collection(client: client)
              end
            else
              puts(SearchEngine::Logging::Color.dim(%(#{indent}"#{dep_coll}" → in_sync (skip))))
            end
          else
            puts(SearchEngine::Logging::Color.dim(%(#{indent}"#{dep_coll}" → skipped (unknown mode: #{mode}))))
          end
        end

        def __se_log_batches_from_summary(batches)
          return unless batches.is_a?(Array)

          batches.each_with_index do |batch_stats, idx|
            puts(SearchEngine::Logging::BatchLine.format(batch_stats, idx + 1, indifferent: true))
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
                :__se_handle_preflight_action,
                :__se_log_batches_from_summary
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
          stale_refs = Array(diff[:stale_references])
          added.any? || removed.any? || !changed.empty? || !coll_opts.empty? || stale_refs.any?
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
            __se_build_index_result([summary])
          end
        end

        # Aggregate an array of Indexer::Summary structs into a single result hash.
        # @param summaries [Array<SearchEngine::Indexer::Summary>]
        # @return [Hash] { status:, docs_total:, success_total:, failed_total:, sample_error: }
        def __se_build_index_result(summaries)
          docs = 0
          success = 0
          failed = 0
          sample_error = nil

          Array(summaries).each do |s|
            docs += s.docs_total.to_i
            success += s.success_total.to_i
            failed += s.failed_total.to_i
            sample_error ||= __se_extract_sample_error(s)
          end

          status = if failed.positive? && success.zero?
                     :failed
                   elsif failed.positive?
                     :partial
                   else
                     :ok
                   end

          { status: status, docs_total: docs, success_total: success, failed_total: failed, sample_error: sample_error }
        end

        private :__se_build_index_result
      end

      class_methods do
        # Sequential processing of partition list
        def __se_index_partitions_seq!(parts, into)
          summaries = []
          parts.each do |part|
            summary = SearchEngine::Indexer.rebuild_partition!(self, partition: part, into: into)
            summaries << summary
            puts(SearchEngine::Logging::PartitionProgress.line(part, summary))
            __se_log_batches_from_summary(summary.batches) if summary.batches_total.to_i > 1
          end
          __se_build_index_result(summaries)
        end
      end

      class_methods do
        # Parallel processing via bounded thread pool
        def __se_index_partitions_parallel!(parts, into, max_p)
          require 'concurrent-ruby'
          pool = Concurrent::FixedThreadPool.new(max_p)
          cancelled = Concurrent::AtomicBoolean.new(false)
          ctx = SearchEngine::Instrumentation.context
          mtx = Mutex.new
          summaries = []
          partition_errors = []

          on_interrupt = lambda do
            warn("\n  Interrupted — stopping parallel partition workers…")
            cancelled.make_true
          end

          SearchEngine::InterruptiblePool.run(pool, on_interrupt: on_interrupt) do
            parts.each do |part|
              break if cancelled.true?

              pool.post do
                next if cancelled.true?

                SearchEngine::Instrumentation.with_context(ctx) do
                  summary = SearchEngine::Indexer.rebuild_partition!(self, partition: part, into: into)
                  mtx.synchronize do
                    summaries << summary
                    puts(SearchEngine::Logging::PartitionProgress.line(part, summary))
                    __se_log_batches_from_summary(summary.batches) if summary.batches_total.to_i > 1
                  end
                end
              rescue StandardError => error
                mtx.synchronize do
                  err_msg = "  partition=#{part.inspect} → error=#{error.class}: #{error.message.to_s[0, 200]}"
                  warn(SearchEngine::Logging::Color.apply(err_msg, :red))
                  partition_errors << "#{error.class}: #{error.message.to_s[0, 200]}"
                end
              end
            end
          end

          result = __se_build_index_result(summaries)
          if partition_errors.any?
            result[:status] = :failed
            result[:sample_error] ||= partition_errors.first
          end
          result
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
