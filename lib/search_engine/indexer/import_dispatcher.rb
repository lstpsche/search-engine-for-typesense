# frozen_string_literal: true

require 'json'

module SearchEngine
  class Indexer
    # Orchestrates a single batch import attempt and retry loop.
    #
    # Emits the same instrumentation as the monolithic Indexer, preserves
    # :upsert default action, and supports dry-run mode (serialize only).
    #
    # @since M8
    class ImportDispatcher
      # Result of a single import (may be used as an element of batches list)
      # Keys mirror existing payload structure.
      # @return [Hash]
      def self.import_batch(client:, collection:, action:, jsonl:, docs_count:, bytes_sent:, batch_index:,
                            retry_policy:, dry_run: false)
        # @param client [SearchEngine::Client]
        # @param collection [String] physical collection name to import into
        # @param action [Symbol, String] one of :upsert, :create, :update
        # @param jsonl [String] JSONL body
        # @param docs_count [Integer] number of docs encoded in jsonl
        # @param bytes_sent [Integer] payload size in bytes
        # @param batch_index [Integer, nil] batch sequence number
        # @param retry_policy [SearchEngine::Indexer::RetryPolicy]
        # @param dry_run [Boolean] when true, do not perform network call
        # @return [Hash] stats payload: { index:, docs_count:, success_count:, failure_count:, attempts:, http_status:, duration_ms:, bytes_sent:, errors_sample: [] }
        # @raise [SearchEngine::Errors::Api] when the underlying client raises an API error (propagated)
        # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/indexer
        if dry_run
          # Emit instrumentation parity without network
          http_status = 200
          success_count = docs_count
          failure_count = 0
          duration_ms = 0.0
          if defined?(ActiveSupport::Notifications)
            payload = base_payload(collection, batch_index, docs_count, bytes_sent)
            SearchEngine::Instrumentation.instrument('search_engine.indexer.batch_import', payload) do |ctx|
              ctx[:success_count] = success_count
              ctx[:failure_count] = failure_count
              ctx[:http_status] = http_status
            end
          end
          return build_stats(batch_index, docs_count, success_count, failure_count, 1, http_status, duration_ms,
                             bytes_sent, []
          )
        end

        attempt = 1
        loop do
          stats = perform_attempt(client, collection, action, jsonl, docs_count, bytes_sent, batch_index, attempt)
          return stats
        rescue StandardError => error
          if retry_policy.retry?(attempt, error)
            delay = retry_policy.next_delay(attempt, error)
            sleep(delay) if delay.positive?
            attempt += 1
            next
          end
          raise
        end
      rescue SearchEngine::Errors::Api => error
        # Let 413 handling and other mapping be owned by the caller for splitting
        raise error
      end

      class << self
        private

        def perform_attempt(client, collection, action, jsonl, docs_count, bytes_sent, idx, attempt)
          start = monotonic_ms
          success_count = 0
          failure_count = 0
          http_status = 200
          error_sample = []

          if defined?(ActiveSupport::Notifications)
            se_payload = base_payload(collection, idx, docs_count, bytes_sent).merge(attempts: attempt,
                                                                                     http_status: nil
                                                                                    )
            SearchEngine::Instrumentation.instrument('search_engine.indexer.batch_import', se_payload) do |ctx|
              raw = client.import_documents(collection: collection, jsonl: jsonl, action: action)
              success_count, failure_count, error_sample = parse_import_response(raw)
              http_status = 200
              ctx[:success_count] = success_count
              ctx[:failure_count] = failure_count
              ctx[:http_status] = http_status
            end
          else
            raw = client.import_documents(collection: collection, jsonl: jsonl, action: action)
            success_count, failure_count, error_sample = parse_import_response(raw)
          end

          duration = monotonic_ms - start
          build_stats(idx, docs_count, success_count, failure_count, attempt, http_status, duration, bytes_sent,
                      error_sample
          )
        end

        def base_payload(collection, idx, docs_count, bytes_sent)
          {
            collection: SearchEngine::Instrumentation.context[:collection] || collection,
            into: collection,
            batch_index: idx,
            docs_count: docs_count,
            success_count: nil,
            failure_count: nil,
            attempts: nil,
            http_status: nil,
            bytes_sent: bytes_sent,
            transient_retry: false,
            retry_after_s: nil,
            error_sample: nil
          }
        end

        def build_stats(idx, docs_count, success_count, failure_count, attempts, http_status, duration_ms, bytes_sent,
                        error_sample)
          {
            index: idx,
            docs_count: docs_count,
            success_count: success_count,
            failure_count: failure_count,
            attempts: attempts,
            http_status: http_status,
            duration_ms: duration_ms.round(1),
            bytes_sent: bytes_sent,
            errors_sample: Array(error_sample)[0, 5]
          }
        end

        def parse_import_response(raw)
          return parse_from_string(raw) if raw.is_a?(String)
          return parse_from_array(raw) if raw.is_a?(Array)

          [0, 0, []]
        end

        def parse_from_string(str)
          success = 0
          failure = 0
          samples = []
          str.each_line do |line|
            line = line.strip
            next if line.empty?

            h = safe_parse_json(line)
            unless h
              failure += 1
              samples << 'invalid-json-line'
              next
            end

            if truthy?(h['success'] || h[:success])
              success += 1
            else
              failure += 1
              msg = h['error'] || h[:error] || h['message'] || h[:message]
              samples << msg.to_s[0, 200] if msg
            end
          end
          [success, failure, samples[0, 5]]
        end

        def parse_from_array(arr)
          success = 0
          failure = 0
          samples = []
          arr.each do |h|
            if h.is_a?(Hash) && truthy?(h['success'] || h[:success])
              success += 1
            else
              failure += 1
              msg = h.is_a?(Hash) ? (h['error'] || h[:error] || h['message'] || h[:message]) : nil
              samples << msg.to_s[0, 200] if msg
            end
          end
          [success, failure, samples[0, 5]]
        end

        def safe_parse_json(line)
          JSON.parse(line)
        rescue StandardError
          nil
        end

        def truthy?(val)
          val == true || val.to_s.downcase == 'true'
        end

        def monotonic_ms
          SearchEngine::Instrumentation.monotonic_ms
        end
      end
    end
  end
end
