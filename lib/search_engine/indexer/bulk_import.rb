# frozen_string_literal: true

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
        # @return [SearchEngine::Indexer::Summary]
        # @raise [SearchEngine::Errors::InvalidParams]
        def call(klass:, into:, enum:, batch_size:, action: DEFAULT_ACTION)
          validate_args!(klass, into, enum, action)

          docs_enum = normalize_enum(enum)
          retry_policy = RetryPolicy.from_config(SearchEngine.config.indexer&.retries)
          client = SearchEngine::Client.new
          buffer = +''
          next_index = sequence_generator

          batches = []
          docs_total = 0
          success_total = 0
          failed_total = 0
          batches_total = 0
          duration_ms_total = 0.0
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
              duration_ms_total += stats[:duration_ms].to_f
              batches_total += 1
              batches << stats
              validate_soft_batch_size!(batch_size, stats[:docs_count])
            end
          end

          Summary.new(
            collection: klass.respond_to?(:collection) ? klass.collection : klass.name.to_s,
            status: status_from_counts(success_total, failed_total),
            batches_total: batches_total,
            docs_total: docs_total,
            success_total: success_total,
            failed_total: failed_total,
            duration_ms_total: (monotonic_ms - started_at).round(1),
            batches: batches
          )
        end

        private

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

        def import_batch_with_handling(client:, collection:, raw_batch:, action:, retry_policy:, buffer:, next_index:)
          docs = BatchPlanner.to_array(raw_batch)
          return [] if docs.empty?

          docs_count, bytes_sent = BatchPlanner.encode_jsonl!(docs, buffer)
          jsonl = buffer.dup
          idx = next_index.call

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
            return import_batch_with_handling(
              client: client,
              collection: collection,
              raw_batch: left,
              action: action,
              retry_policy: retry_policy,
              buffer: buffer,
              next_index: next_index
            ) + import_batch_with_handling(
              client: client,
              collection: collection,
              raw_batch: right,
              action: action,
              retry_policy: retry_policy,
              buffer: buffer,
              next_index: next_index
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
      end
    end
  end
end
