# frozen_string_literal: true

module SearchEngine
  # Operations namespace for convenience access to operational endpoints.
  #
  # Provides small wrappers around {SearchEngine::Client} for metrics, stats,
  # and health, emitting instrumentation and supporting dependency injection
  # via an optional client.
  module Operations
    class << self
      # Retrieve raw server metrics.
      #
      # @param client [SearchEngine::Client, nil] optional injected client
      # @return [Hash] raw payload from Typesense `/metrics.json`
      # @see SearchEngine::Client#metrics
      def metrics(client: nil)
        SearchEngine::Instrumentation.instrument('search_engine.operations.metrics', {}) do
          ts_client = client || configured_client || SearchEngine::Client.new
          ts_client.metrics
        end
      end

      # Retrieve raw server stats.
      #
      # @param client [SearchEngine::Client, nil] optional injected client
      # @return [Hash] raw payload from Typesense `/stats.json`
      # @see SearchEngine::Client#stats
      def stats(client: nil)
        SearchEngine::Instrumentation.instrument('search_engine.operations.stats', {}) do
          ts_client = client || configured_client || SearchEngine::Client.new
          ts_client.stats
        end
      end

      # Retrieve server health.
      #
      # @param client [SearchEngine::Client, nil] optional injected client
      # @return [Hash] Typesense health response
      # @see SearchEngine::Client#health
      def health(client: nil)
        SearchEngine::Instrumentation.instrument('search_engine.operations.health', {}) do
          ts_client = client || configured_client || SearchEngine::Client.new
          ts_client.health
        end
      end

      private

      def configured_client
        return unless SearchEngine.config.respond_to?(:client)

        SearchEngine.config.client
      rescue StandardError
        nil
      end
    end
  end
end
