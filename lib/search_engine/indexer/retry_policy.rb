# frozen_string_literal: true

module SearchEngine
  class Indexer
    # Encapsulates retryability rules and exponential backoff with jitter
    # for import attempts. Pure and deterministic except for jitter.
    #
    # Usage:
    #   policy = SearchEngine::Indexer::RetryPolicy.from_config(SearchEngine.config.indexer.retries)
    #   attempt = 1
    #   begin
    #     # perform
    #   rescue => error
    #     if policy.retry?(attempt, error)
    #       sleep(policy.next_delay(attempt, error))
    #       attempt += 1
    #       retry
    #     else
    #       raise
    #     end
    #   end
    #
    # @since M8
    class RetryPolicy
      # @return [Integer]
      attr_reader :attempts

      # @param attempts [Integer]
      # @param base [Float]
      # @param max [Float]
      # @param jitter_fraction [Float]
      def initialize(attempts:, base:, max:, jitter_fraction:)
        @attempts = Integer(attempts)
        @base = base.to_f
        @max = max.to_f
        @jitter_fraction = jitter_fraction.to_f
      end

      # Build a policy from a config-like Hash.
      # @param cfg [Hash]
      # @return [RetryPolicy]
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/indexer
      def self.from_config(cfg)
        c = cfg || {}
        new(
          attempts: (c[:attempts]&.to_i&.positive? ? c[:attempts].to_i : 3),
          base: (c[:base]&.to_f&.positive? ? c[:base].to_f : 0.5),
          max: (c[:max]&.to_f&.positive? ? c[:max].to_f : 5.0),
          jitter_fraction: (c[:jitter_fraction].to_f >= 0 ? c[:jitter_fraction].to_f : 0.2)
        )
      end

      # Whether we should retry for a given attempt and error.
      # @param attempt [Integer] 1-based attempt index
      # @param error [Exception]
      # @return [Boolean]
      def retry?(attempt, error)
        return false if attempt >= @attempts

        case error
        when SearchEngine::Errors::Timeout, SearchEngine::Errors::Connection
          true
        when SearchEngine::Errors::Api
          transient_status?(error.status.to_i)
        else
          false
        end
      end

      # Compute the delay in seconds before the next attempt.
      # @param attempt [Integer] 1-based attempt index
      # @param error [Exception]
      # @return [Float]
      def next_delay(attempt, _error)
        exp = [@base * (2 ** (attempt - 1)), @max].min
        jitter = exp * @jitter_fraction
        delta = random_in_range(-jitter..jitter)
        sleep_time = exp + delta
        sleep_time.positive? ? sleep_time : 0.0
      end

      # Whether an HTTP status code represents a transient/retryable error.
      #
      # Includes 404 to handle brief unavailability of freshly-created
      # collections during blue/green schema applies (Typesense cluster
      # propagation delay). A genuinely missing collection will still fail
      # after exhausting the retry budget.
      #
      # @param code [Integer]
      # @return [Boolean]
      def self.transient_status?(code)
        code == 404 || code == 429 || (code >= 500 && code <= 599)
      end

      private

      def transient_status?(code)
        self.class.transient_status?(code)
      end

      def random_in_range(range)
        # Use a thread-local RNG for low contention and testability
        rng = (Thread.current[:__se_retry_rng__] ||= Random.new)
        min = range.begin.to_f
        max = range.end.to_f
        # Honor exclusive-end ranges by shifting the upper bound down one
        # representable float step. Inclusive ranges preserve the original end.
        max = max.prev_float if range.exclude_end?
        span = max - min
        return min if span <= 0.0

        min + (rng.rand * span)
      end
    end
  end
end
