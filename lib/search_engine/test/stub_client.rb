# frozen_string_literal: true

require 'monitor'
require 'json'

module SearchEngine
  module Test
    # Test-only programmable stub client that mimics the public surface of
    # SearchEngine::Client for search and multi_search. It never performs I/O.
    #
    # Thread-safe: queues and captures are guarded by a Monitor.
    # Redaction: bodies and captured payloads are redacted for safe inspection.
    #
    # Usage:
    #   stub = SearchEngine::Test::StubClient.new
    #   stub.enqueue_response(:search, { 'hits' => [], 'found' => 0, 'out_of' => 0 })
    #   SearchEngine.configure { |c| c.client = stub }
    #
    # Queued responses are FIFO. You may enqueue Exceptions to simulate errors
    # or Procs that receive the captured request and return a response.
    #
    # @since M8
    # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/testing
    class StubClient
      Call = Struct.new(
        :timestamp,
        :correlation_id,
        :verb,
        :url,
        :body,
        :url_opts,
        :redacted_body,
        :redacted?,
        keyword_init: true
      )

      def initialize
        @lock = Monitor.new
        @queues = { search: [], multi_search: [] }
        @calls = { search: [], multi_search: [] }
      end

      # Enqueue a response for a given method. Accepts a Hash, Exception, or Proc.
      # @param method [Symbol] :search or :multi_search
      # @param value [Hash, Exception, Proc]
      # @return [void]
      # @since M8
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/testing#quick-start
      def enqueue_response(method, value)
        @lock.synchronize do
          queue_for(method) << value
        end
      end

      # Reset all internal state (queues and captures).
      # @since M8
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/testing#parallel-test-safety
      def reset!
        @lock.synchronize do
          @queues.each_value(&:clear)
          @calls.each_value(&:clear)
        end
      end

      # Return captured calls for search.
      # @return [Array<Call>]
      # @since M8
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/testing
      def search_calls
        @lock.synchronize { @calls[:search].dup }
      end

      # Return captured calls for multi_search.
      # @return [Array<Call>]
      # @since M8
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/testing
      def multi_search_calls
        @lock.synchronize { @calls[:multi_search].dup }
      end

      # All calls in chronological order.
      # @return [Array<Call>]
      # @since M8
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/testing
      def all_calls
        @lock.synchronize { (@calls[:search] + @calls[:multi_search]).sort_by(&:timestamp) }
      end

      # Public API: single search. Mirrors Client#search arity. Returns Result-like object.
      # @param collection [String]
      # @param params [Hash]
      # @param url_opts [Hash]
      # @since M8
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/testing
      def search(collection:, params:, url_opts: {})
        unless collection.is_a?(String) && !collection.strip.empty?
          raise ArgumentError, 'collection must be a non-empty String'
        end

        params_obj = SearchEngine::CompiledParams.from(params)
        params_hash = params_obj.to_h

        entry = capture(:search, url: compiled_url(collection), params: params_hash, url_opts: url_opts)
        payload = dequeue_or_default(:search, entry)
        wrap_single(payload)
      end

      # Public API: multi search. Mirrors top-level helper client usage: returns raw Hash from Typesense.
      # @param searches [Array<Hash>]
      # @param url_opts [Hash]
      # @since M8
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/testing
      def multi_search(searches:, url_opts: {})
        unless searches.is_a?(Array) && searches.all? { |h| h.is_a?(Hash) }
          raise ArgumentError, 'searches must be an Array of Hashes'
        end

        # Record a synthetic URL for multi endpoint; individual bodies are not posted here
        entry = capture(:multi_search, url: compiled_multi_url, params: searches, url_opts: url_opts)
        dequeue_or_default(:multi_search, entry).tap do |raw|
          return raw
        end
      end

      private

      def dequeue_or_default(method, entry)
        val = @lock.synchronize { queue_for(method).shift }
        case val
        when nil
          default_payload(method)
        when Proc
          val.call(entry)
        when Exception
          raise val
        else
          val
        end
      end

      def default_payload(method)
        if method == :search
          { 'hits' => [], 'found' => 0, 'out_of' => 0 }
        else
          { 'results' => [] }
        end
      end

      def queue_for(method)
        q = @queues[method]
        raise ArgumentError, "unknown method: #{method.inspect}" unless q

        q
      end

      def compiled_url(collection)
        cfg = SearchEngine.config
        proto = cfg.protocol.to_s.strip.presence
        base = [proto, "#{cfg.host}:#{cfg.port}"].compact.join('://')
        "#{base}/collections/#{collection}/documents/search"
      end

      def compiled_multi_url
        cfg = SearchEngine.config
        proto = cfg.protocol.to_s.strip.presence
        base = [proto, "#{cfg.host}:#{cfg.port}"].compact.join('://')
        "#{base}/multi_search"
      end

      def capture(method, url:, params:, url_opts: {})
        redacted = begin
          SearchEngine::Observability.redact(params)
        rescue StandardError
          params
        end
        corr = begin
          SearchEngine::Instrumentation.current_correlation_id
        rescue StandardError
          nil
        end
        entry = Call.new(
          timestamp: SearchEngine::Instrumentation.monotonic_ms,
          correlation_id: corr,
          verb: method,
          url: url,
          body: params,
          url_opts: SearchEngine::Observability.filtered_url_opts(url_opts),
          redacted_body: redacted,
          redacted?: true
        )
        @lock.synchronize { @calls[method] << entry }
        entry
      end

      def wrap_single(payload)
        # Mirror Client#search returning SearchEngine::Result
        SearchEngine::Result.new(payload, klass: nil)
      end
    end
  end
end
