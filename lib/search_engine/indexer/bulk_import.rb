# frozen_string_literal: true

require 'search_engine/logging/color'

module SearchEngine
  class Indexer
    # Orchestrates streaming JSONL bulk imports for partition rebuilds.
    #
    # Keeps the legacy semantics from {SearchEngine::Indexer#import!} while delegating
    # encoding and retry logic to dedicated helpers. Splits overlarge batches when
    # Typesense responds with HTTP 413, aggregates metrics, and returns a
    # {SearchEngine::Indexer::Summary} compatible with existing logging helpers.
    #
    # @since M8
    class BulkImport
      DEFAULT_ACTION = :upsert
      class << self
        # Execute a bulk import for the provided batches enumerable.
        #
        # @param klass [Class] a {SearchEngine::Base} subclass (used for metadata only)
        # @param into [String] physical collection name
        # @param enum [Enumerable] yields batch-like objects convertible to Array
        # @param batch_size [Integer, nil] soft guard for logging when exceeded
        # @param action [Symbol] :upsert (default), :create, or :update
        # @param log_batches [Boolean] whether to log each batch as it completes (default: true)
        # @param max_parallel [Integer] maximum parallel threads for batch processing (default: 1)
        # @return [SearchEngine::Indexer::Summary]
        # @raise [SearchEngine::Errors::InvalidParams]
        def call(klass:, into:, enum:, batch_size:, action: DEFAULT_ACTION, log_batches: true, max_parallel: 1)
          validate_args!(klass, into, enum, action)
          mp = max_parallel.to_i
          mp = 1 unless mp.positive?

          if mp > 1
            call_parallel(
              klass: klass, into: into, enum: enum, batch_size: batch_size, action: action,
              log_batches: log_batches, max_parallel: mp
            )
          else
            call_sequential(
              klass: klass, into: into, enum: enum, batch_size: batch_size, action: action,
              log_batches: log_batches
            )
          end
        end

        private

        # Process batches sequentially (one at a time).
        #
        # @param klass [Class] a {SearchEngine::Base} subclass (used for metadata only)
        # @param into [String] physical collection name
        # @param enum [Enumerable] yields batch-like objects convertible to Array
        # @param batch_size [Integer, nil] soft guard for logging when exceeded
        # @param action [Symbol] :upsert, :create, or :update
        # @param log_batches [Boolean] whether to log each batch as it completes
        # @return [SearchEngine::Indexer::Summary]
        def call_sequential(klass:, into:, enum:, batch_size:, action:, log_batches:)
          docs_enum = normalize_enum(enum)
          retry_policy = RetryPolicy.from_config(SearchEngine.config.indexer&.retries)
          client = SearchEngine.client
          buffer = +''
          next_index = sequence_generator

          batches = []
          docs_total = 0
          success_total = 0
          failed_total = 0
          failed_batches_total = 0
          batches_total = 0
          # Capture start time before processing any batches to measure total wall-clock duration
          started_at = monotonic_ms

          docs_enum.each do |raw_batch|
            stats_list = import_batch_with_handling(
              client: client,
              collection: into,
              raw_batch: raw_batch,
              action: action,
              retry_policy: retry_policy,
              buffer: buffer,
              next_index: next_index
            )

            stats_list.each do |stats|
              docs_total += stats[:docs_count].to_i
              success_total += stats[:success_count].to_i
              failed_total += stats[:failure_count].to_i
              failed_batches_total += 1 if stats[:failure_count].to_i.positive?
              batches_total += 1
              batches << stats
              validate_soft_batch_size!(batch_size, stats[:docs_count])
              log_batch(stats, batches_total) if log_batches
            end
          end

          # Calculate total duration as wall-clock time from start to finish (not sum of batch durations)
          total_duration_ms = (monotonic_ms - started_at).round(1)

          Summary.new(
            collection: klass.respond_to?(:collection) ? klass.collection : klass.name.to_s,
            status: status_from_counts(success_total, failed_total),
            batches_total: batches_total,
            docs_total: docs_total,
            success_total: success_total,
            failed_total: failed_total,
            failed_batches_total: failed_batches_total,
            duration_ms_total: total_duration_ms,
            batches: batches
          )
        end

        # Process batches in parallel using a thread pool.
        #
        # Materializes all batches upfront and processes them concurrently using
        # a bounded thread pool. Each thread gets its own Client instance and buffer
        # to avoid thread-safety issues. Thread-safe aggregation of stats is handled
        # via mutex synchronization.
        #
        # @param klass [Class] a {SearchEngine::Base} subclass (used for metadata only)
        # @param into [String] physical collection name
        # @param enum [Enumerable] yields batch-like objects convertible to Array
        # @param batch_size [Integer, nil] soft guard for logging when exceeded
        # @param action [Symbol] :upsert, :create, or :update
        # @param log_batches [Boolean] whether to log each batch as it completes
        # @param max_parallel [Integer] maximum number of parallel threads
        # @return [SearchEngine::Indexer::Summary]
        def call_parallel(klass:, into:, enum:, batch_size:, action:, log_batches:, max_parallel:)
          require 'concurrent-ruby'

          # Use producer-consumer pattern with bounded queue to avoid full materialization
          # Queue capacity = max_parallel * 2 to keep workers busy while producer fetches
          docs_enum = normalize_enum(enum)
          total_batches_estimate = estimate_total_batches(klass)
          queue_capacity = max_parallel * 2
          batch_queue = SizedQueue.new(queue_capacity)
          sentinel = Object.new # Unique object to signal completion

          retry_policy = RetryPolicy.from_config(SearchEngine.config.indexer&.retries)
          pool = Concurrent::FixedThreadPool.new(max_parallel)
          shared_state = initialize_shared_state
          producer_error = nil

          puts('  Starting parallel batch processing...') if log_batches
          started_at = monotonic_ms

          # Producer thread: fetch batches lazily and push to queue
          producer_thread = Thread.new do
            batch_count = 0
            docs_enum.each do |batch|
              batch_queue.push(batch)
              batch_count += 1
              # Log progress every 10 batches
              next unless log_batches && (batch_count % 10).zero?

              elapsed = (monotonic_ms - started_at).round(1)
              if total_batches_estimate
                puts("  Processed #{batch_count}/#{total_batches_estimate} batches... (#{elapsed}ms)")
              else
                puts("  Processed #{batch_count} batches... (#{elapsed}ms)")
              end
            end
          rescue StandardError => error
            producer_error = error
            warn("  Producer failed at batch #{batch_count}: #{error.class}: #{error.message.to_s[0, 200]}")
          ensure
            # Signal completion to all workers
            max_parallel.times { batch_queue.push(sentinel) }
          end

          # Worker threads: consume batches from queue
          begin
            process_batches_from_queue(
              batch_queue: batch_queue,
              sentinel: sentinel,
              into: into,
              action: action,
              retry_policy: retry_policy,
              batch_size: batch_size,
              log_batches: log_batches,
              pool: pool,
              shared_state: shared_state,
              max_parallel: max_parallel
            )
          ensure
            shutdown_pool(pool)
            producer_thread.join if producer_thread.alive?
          end

          raise producer_error if producer_error

          build_summary(klass, shared_state)
        end

        # Initialize shared state hash for parallel batch processing.
        #
        # Creates a hash containing counters, batches array, mutex, and timing
        # information that will be shared across threads and synchronized via mutex.
        #
        # @return [Hash] shared state hash with keys: :batches, :docs_total, :success_total,
        #   :failed_total, :batches_total, :idx_counter, :started_at, :mtx
        def initialize_shared_state
          {
            batches: [],
            docs_total: 0,
            success_total: 0,
            failed_total: 0,
            failed_batches_total: 0,
            batches_total: 0,
            idx_counter: -1,
            started_at: monotonic_ms,
            mtx: Mutex.new
          }
        end

        # Process batches from a queue using worker threads.
        #
        # Workers pull batches from the queue and process them concurrently.
        # Stops when sentinel is received. Uses a thread pool for concurrent processing.
        #
        # @param batch_queue [SizedQueue] thread-safe queue containing batches
        # @param sentinel [Object] unique object signaling queue completion
        # @param into [String] physical collection name
        # @param action [Symbol] :upsert, :create, or :update
        # @param retry_policy [SearchEngine::Indexer::RetryPolicy] retry policy instance
        # @param batch_size [Integer, nil] soft guard for logging when exceeded
        # @param log_batches [Boolean] whether to log each batch as it completes
        # @param pool [Concurrent::FixedThreadPool] thread pool instance
        # @param shared_state [Hash] shared state hash for thread-safe aggregation
        # @param max_parallel [Integer] number of worker threads to start
        # @return [void]
        def process_batches_from_queue(batch_queue:, sentinel:, into:, action:, retry_policy:, batch_size:,
                                       log_batches:, pool:, shared_state:, max_parallel:)
          max_parallel.times do
            pool.post do
              loop do
                batch = batch_queue.pop
                break if batch.equal?(sentinel)

                process_single_batch_parallel(
                  raw_batch: batch,
                  into: into,
                  action: action,
                  retry_policy: retry_policy,
                  batch_size: batch_size,
                  log_batches: log_batches,
                  shared_state: shared_state
                )
              end
            end
          end
        end

        # Process a single batch in a parallel thread.
        #
        # Executed within a thread pool worker thread. Each thread gets its own
        # Client instance and buffer to avoid thread-safety issues. The batch index
        # is assigned atomically via mutex synchronization. Stats are aggregated
        # thread-safely after processing.
        #
        # @param raw_batch [Object] batch object convertible to Array
        # @param into [String] physical collection name
        # @param action [Symbol] :upsert, :create, or :update
        # @param retry_policy [SearchEngine::Indexer::RetryPolicy] retry policy instance
        # @param batch_size [Integer, nil] soft guard for logging when exceeded
        # @param log_batches [Boolean] whether to log each batch as it completes
        # @param shared_state [Hash] shared state hash for thread-safe aggregation
        # @return [void]
        def process_single_batch_parallel(raw_batch:, into:, action:, retry_policy:, batch_size:, log_batches:,
                                          shared_state:)
          # Each thread gets its own resources
          thread_client = SearchEngine.client
          thread_buffer = +''
          thread_idx = shared_state[:mtx].synchronize { shared_state[:idx_counter] += 1 }

          begin
            stats_list = import_batch_with_handling(
              client: thread_client,
              collection: into,
              raw_batch: raw_batch,
              action: action,
              retry_policy: retry_policy,
              buffer: thread_buffer,
              next_index: -> { thread_idx }
            )

            shared_state[:mtx].synchronize do
              aggregate_stats(stats_list, shared_state, batch_size, log_batches)
            end
          rescue StandardError => error
            # Calculate document count for error stats (before any potential encoding errors)
            docs_count = begin
              BatchPlanner.to_array(raw_batch).size
            rescue StandardError
              0
            end

            # Create failure stats similar to import_batch_with_handling_internal error path
            failure_stat = failure_stats(thread_idx, docs_count, 0, error)

            shared_state[:mtx].synchronize do
              warn("  batch_index=#{thread_idx} → error=#{error.class}: #{error.message.to_s[0, 200]}")
              aggregate_stats([failure_stat], shared_state, batch_size, log_batches)
            end
          end
        end

        # Aggregate batch statistics thread-safely into shared state.
        #
        # Must be called within a mutex synchronization block. Updates counters,
        # appends to batches array, validates batch size, and optionally logs.
        #
        # @param stats_list [Array<Hash>] array of stats hashes from batch processing
        # @param shared_state [Hash] shared state hash to update (must be mutex-protected)
        # @param batch_size [Integer, nil] soft guard for logging when exceeded
        # @param log_batches [Boolean] whether to log each batch as it completes
        # @return [void]
        def aggregate_stats(stats_list, shared_state, batch_size, log_batches)
          stats_list.each do |stats|
            shared_state[:docs_total] += stats[:docs_count].to_i
            shared_state[:success_total] += stats[:success_count].to_i
            shared_state[:failed_total] += stats[:failure_count].to_i
            shared_state[:failed_batches_total] += 1 if stats[:failure_count].to_i.positive?
            shared_state[:batches_total] += 1
            shared_state[:batches] << stats
            validate_soft_batch_size!(batch_size, stats[:docs_count])
            log_batch(stats, shared_state[:batches_total]) if log_batches
          end
        end

        # Shutdown the thread pool gracefully with timeout.
        #
        # Shuts down the pool, waits up to 1 hour for completion, then force-kills
        # if necessary and waits an additional minute for cleanup.
        #
        # @param pool [Concurrent::FixedThreadPool] thread pool instance to shutdown
        # @return [void]
        def shutdown_pool(pool)
          pool.shutdown
          # Wait up to 1 hour, then force-kill and wait a bit more to ensure cleanup
          pool.wait_for_termination(3600) || pool.kill
          pool.wait_for_termination(60)
        end

        # Build a Summary object from aggregated shared state.
        #
        # Calculates total duration and constructs a Summary with all aggregated
        # statistics from parallel batch processing.
        #
        # @param klass [Class] a {SearchEngine::Base} subclass (used for metadata only)
        # @param shared_state [Hash] shared state hash containing aggregated statistics
        # @return [SearchEngine::Indexer::Summary]
        def build_summary(klass, shared_state)
          total_duration_ms = (monotonic_ms - shared_state[:started_at]).round(1)

          Summary.new(
            collection: klass.respond_to?(:collection) ? klass.collection : klass.name.to_s,
            status: status_from_counts(shared_state[:success_total], shared_state[:failed_total]),
            batches_total: shared_state[:batches_total],
            docs_total: shared_state[:docs_total],
            success_total: shared_state[:success_total],
            failed_total: shared_state[:failed_total],
            failed_batches_total: shared_state[:failed_batches_total],
            duration_ms_total: total_duration_ms,
            batches: shared_state[:batches]
          )
        end

        def validate_args!(klass, into, enum, action)
          unless klass.is_a?(Class) && klass.ancestors.include?(SearchEngine::Base)
            raise Errors::InvalidParams, 'klass must inherit from SearchEngine::Base'
          end

          raise Errors::InvalidParams, 'into must be a non-empty String' if into.nil? || into.to_s.strip.empty?

          raise Errors::InvalidParams, 'enum must be enumerable' unless enum.respond_to?(:each)

          valid_actions = %i[upsert create update]
          return if valid_actions.include?(action.to_sym)

          raise Errors::InvalidParams,
                "action must be one of :upsert, :create, :update (received #{action.inspect})"
        end

        def normalize_enum(enum)
          enum.is_a?(Enumerator) ? enum : enum.each
        end

        # Estimate total batch count for progress logging.
        #
        # Attempts to estimate batch count for ActiveRecord sources by counting records
        # and dividing by batch_size. Returns nil for other source types or when estimation fails.
        #
        # @param klass [Class] a {SearchEngine::Base} subclass
        # @return [Integer, nil] estimated total batch count or nil if not estimable
        def estimate_total_batches(klass)
          return nil unless klass.is_a?(Class)

          dsl = mapper_dsl_for_klass(klass)
          return nil unless dsl

          source_def = dsl[:source]
          return nil unless source_def
          return nil unless source_def[:type] == :active_record

          model = source_def.dig(:options, :model)
          return nil unless model.respond_to?(:count)

          batch_size = source_def.dig(:options, :batch_size)
          batch_size ||= SearchEngine.config.sources.active_record.batch_size
          batch_size = batch_size.to_i
          return nil unless batch_size.positive?

          begin
            total_records = model.count
            return nil unless total_records.positive?

            (total_records.to_f / batch_size).ceil
          rescue StandardError
            nil
          end
        end

        def mapper_dsl_for_klass(klass)
          return nil unless klass.instance_variable_defined?(:@__mapper_dsl__)

          klass.instance_variable_get(:@__mapper_dsl__)
        end

        # Import a single batch with error handling and recursive 413 splitting.
        #
        # Public wrapper that delegates to the internal method with batch_index set to nil,
        # indicating the index should be computed from next_index.
        #
        # @param client [SearchEngine::Client] client instance
        # @param collection [String] physical collection name
        # @param raw_batch [Object] batch object convertible to Array
        # @param action [Symbol] :upsert, :create, or :update
        # @param retry_policy [SearchEngine::Indexer::RetryPolicy] retry policy instance
        # @param buffer [String] mutable string buffer for JSONL encoding
        # @param next_index [Proc, Integer] proc that returns next index, or integer index
        # @return [Array<Hash>] array of stats hashes (may contain multiple entries on 413 split)
        def import_batch_with_handling(client:, collection:, raw_batch:, action:, retry_policy:, buffer:, next_index:)
          import_batch_with_handling_internal(
            client: client,
            collection: collection,
            raw_batch: raw_batch,
            action: action,
            retry_policy: retry_policy,
            buffer: buffer,
            next_index: next_index,
            batch_index: nil
          )
        end

        # Internal method for importing a batch with optional batch_index preservation.
        #
        # When batch_index is provided (non-nil), it is used directly, preserving the
        # original batch index for recursive splits on 413 errors. When batch_index is nil,
        # the index is computed from next_index (either by calling the proc or using the integer).
        #
        # @param client [SearchEngine::Client] client instance
        # @param collection [String] physical collection name
        # @param raw_batch [Object] batch object convertible to Array
        # @param action [Symbol] :upsert, :create, or :update
        # @param retry_policy [SearchEngine::Indexer::RetryPolicy] retry policy instance
        # @param buffer [String] mutable string buffer for JSONL encoding
        # @param next_index [Proc, Integer] proc that returns next index, or integer index
        # @param batch_index [Integer, nil] optional pre-computed batch index (for recursive splits)
        # @return [Array<Hash>] array of stats hashes (may contain multiple entries on 413 split)
        def import_batch_with_handling_internal(client:, collection:, raw_batch:, action:, retry_policy:, buffer:,
                                                next_index:, batch_index:)
          docs = BatchPlanner.to_array(raw_batch)
          return [] if docs.empty?

          docs_count, bytes_sent = BatchPlanner.encode_jsonl!(docs, buffer)
          jsonl = buffer.dup
          # Use provided batch_index if available (for recursive splits), otherwise compute from next_index
          idx = batch_index || (next_index.is_a?(Proc) ? next_index.call : next_index)

          started_at = monotonic_ms

          stats = ImportDispatcher.import_batch(
            client: client,
            collection: collection,
            action: action,
            jsonl: jsonl,
            docs_count: docs_count,
            bytes_sent: bytes_sent,
            batch_index: idx,
            retry_policy: retry_policy,
            dry_run: false
          )
          stats[:duration_ms] = (monotonic_ms - started_at).round(1)
          stats[:index] = idx
          [stats]
        rescue Errors::Api => error
          if error.status.to_i == 413 && docs.size > 1
            mid = docs.size / 2
            left = docs[0...mid]
            right = docs[mid..]
            # Preserve the original batch index for both recursive splits
            return import_batch_with_handling_internal(
              client: client,
              collection: collection,
              raw_batch: left,
              action: action,
              retry_policy: retry_policy,
              buffer: buffer,
              next_index: next_index,
              batch_index: idx
            ) + import_batch_with_handling_internal(
              client: client,
              collection: collection,
              raw_batch: right,
              action: action,
              retry_policy: retry_policy,
              buffer: buffer,
              next_index: next_index,
              batch_index: idx
            )
          end

          [failure_stats(idx, docs_count, bytes_sent, error)]
        end

        def failure_stats(idx, docs_count, bytes_sent, error)
          {
            index: idx,
            docs_count: docs_count,
            success_count: 0,
            failure_count: docs_count,
            attempts: 1,
            http_status: error&.status.to_i,
            duration_ms: 0.0,
            bytes_sent: bytes_sent,
            errors_sample: [safe_error_excerpt(error)]
          }
        end

        def safe_error_excerpt(error)
          cls = error&.class&.name
          msg = error&.message.to_s
          "#{cls}: #{msg[0, 200]}"
        end

        def sequence_generator
          idx = -1
          -> { idx += 1 }
        end

        def validate_soft_batch_size!(batch_size, docs_count)
          limit = batch_size&.to_i
          return unless limit&.positive?
          return if docs_count.to_i <= limit

          Kernel.warn("[search_engine] BulkImport batch exceeded soft limit: size=#{docs_count}, limit=#{limit}")
        end

        def status_from_counts(success_total, failed_total)
          if failed_total.positive? && success_total.positive?
            :partial
          elsif failed_total.positive?
            :failed
          else
            :ok
          end
        end

        def monotonic_ms
          SearchEngine::Instrumentation.monotonic_ms
        end

        def log_batch(stats, batch_number)
          batch_status = batch_status_from_stats(stats)
          status_color = SearchEngine::Logging::Color.for_status(batch_status)

          prefix = batch_number == 1 ? '  single → ' : '          '
          line = +prefix
          line << SearchEngine::Logging::Color.apply("status=#{batch_status}", status_color) << ' '
          line << "docs=#{stats[:docs_count]}" << ' '
          success_count = stats[:success_count].to_i
          success_str = "success=#{success_count}"
          line << (
            success_count.positive? ? SearchEngine::Logging::Color.bold(success_str) : success_str
          ) << ' '
          failed_count = stats[:failure_count].to_i
          failed_str = "failed=#{failed_count}"
          line << (failed_count.positive? ? SearchEngine::Logging::Color.apply(failed_str, :red) : failed_str) << ' '
          line << "batch=#{batch_number} "
          line << "duration_ms=#{stats[:duration_ms]}"

          # Extract sample error from batch stats
          sample_err = extract_batch_sample_error(stats)
          line << " sample_error=#{sample_err.inspect}" if sample_err

          puts(line)
        end

        def extract_batch_sample_error(stats)
          samples = stats[:errors_sample] || stats['errors_sample']
          return nil unless samples.is_a?(Array) && samples.any?

          samples.each do |msg|
            s = msg.to_s
            return s unless s.strip.empty?
          end
          nil
        end

        def batch_status_from_stats(stats)
          success_count = stats[:success_count].to_i
          failure_count = stats[:failure_count].to_i

          if failure_count.positive? && success_count.positive?
            :partial
          elsif failure_count.positive?
            :failed
          else
            :ok
          end
        end
      end
    end
  end
end
