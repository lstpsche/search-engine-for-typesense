# frozen_string_literal: true

module SearchEngine
  # Helpers for interacting with the Typesense cache operations API.
  #
  # Provides a simple facade over {SearchEngine::Client#clear_cache} that emits
  # instrumentation and allows caller-provided clients for dependency injection.
  module Cache
    class << self
      # Clear the Typesense server-side search cache.
      #
      # @param client [SearchEngine::Client, nil] optional injected client
      # @return [Hash] response payload with symbolized keys
      # @see SearchEngine::Client#clear_cache
      def clear(client: nil)
        SearchEngine::Instrumentation.instrument('search_engine.cache.clear', {}) do
          ts_client = client || SearchEngine.client
          ts_client.clear_cache
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
