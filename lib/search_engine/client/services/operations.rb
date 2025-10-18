# frozen_string_literal: true

module SearchEngine
  class Client
    module Services
      # Operational endpoints (health checks, cache management, API keys).
      class Operations < Base
        def health
          start = current_monotonic_ms
          path = Client::RequestBuilder::HEALTH_PATH

          result = with_exception_mapping(:get, path, {}, start) do
            typesense.health.retrieve
          end

          symbolize_keys_deep(result)
        ensure
          instrument(:get, path, (start ? (current_monotonic_ms - start) : 0.0), {})
        end

        def list_api_keys
          start = current_monotonic_ms
          path = '/keys'

          result = with_exception_mapping(:get, path, {}, start) do
            res = begin
              typesense.keys.retrieve
            rescue NoMethodError
              typesense.keys.list
            end
            if res.is_a?(Hash)
              Array(res[:keys] || res['keys'])
            else
              Array(res)
            end
          end

          symbolize_keys_deep(result)
        ensure
          instrument(:get, path, (start ? (current_monotonic_ms - start) : 0.0), {})
        end

        def clear_cache
          start = current_monotonic_ms
          path = '/operations/cache/clear'

          result = with_exception_mapping(:post, path, {}, start) do
            typesense.operations.perform('cache/clear')
          end

          instrument(:post, path, current_monotonic_ms - start, {})
          symbolize_keys_deep(result)
        end
      end
    end
  end
end
