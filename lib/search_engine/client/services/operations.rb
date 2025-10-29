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

        # Return raw cluster metrics from Typesense.
        #
        # Exposes the `/metrics.json` endpoint without symbolizing or coercing keys.
        # When the upstream client does not expose a dedicated endpoint, falls back
        # to a direct HTTP GET using the configured host/port/protocol and API key.
        #
        # @return [Hash] raw JSON object returned by Typesense `/metrics.json`
        # @see `https://typesense.org/docs/latest/api/cluster-operations.html#metrics`
        def metrics
          start = current_monotonic_ms
          path = '/metrics.json'

          with_exception_mapping(:get, path, {}, start) do
            ts = typesense
            if ts.respond_to?(:metrics) && ts.metrics.respond_to?(:retrieve)
              ts.metrics.retrieve
            elsif ts.respond_to?(:operations) && ts.operations.respond_to?(:perform)
              ts.operations.perform('metrics.json')
            else
              http_get_json(path)
            end
          end
        ensure
          instrument(:get, path, (start ? (current_monotonic_ms - start) : 0.0), {})
        end

        # Return raw server statistics from Typesense.
        #
        # Exposes the `/stats.json` endpoint without symbolizing or coercing keys.
        # When the upstream client does not expose a dedicated endpoint, falls back
        # to a direct HTTP GET using the configured host/port/protocol and API key.
        #
        # @return [Hash] raw JSON object returned by Typesense `/stats.json`
        # @see `https://typesense.org/docs/latest/api/cluster-operations.html#stats`
        def stats
          start = current_monotonic_ms
          path = '/stats.json'

          with_exception_mapping(:get, path, {}, start) do
            ts = typesense
            if ts.respond_to?(:stats) && ts.stats.respond_to?(:retrieve)
              ts.stats.retrieve
            elsif ts.respond_to?(:operations) && ts.operations.respond_to?(:perform)
              ts.operations.perform('stats.json')
            else
              http_get_json(path)
            end
          end
        ensure
          instrument(:get, path, (start ? (current_monotonic_ms - start) : 0.0), {})
        end

        private

        def http_get_json(path)
          require 'net/http'
          require 'uri'
          require 'json'

          proto = begin
            config.protocol.to_s.strip
          rescue StandardError
            'http'
          end
          proto = (proto.nil? || proto.empty?) ? 'http' : proto

          host = config.host
          port = config.port
          uri = URI.parse("#{proto}://#{host}:#{port}#{path}")

          http = Net::HTTP.new(uri.host, uri.port)
          http.use_ssl = (uri.scheme == 'https')

          timeout_s = begin
            t = config.timeout_ms.to_i
            t.positive? ? (t / 1000.0) : 2.0
          rescue StandardError
            2.0
          end
          http.open_timeout = timeout_s
          http.read_timeout = timeout_s

          req = Net::HTTP::Get.new(uri.request_uri)
          req['X-TYPESENSE-API-KEY'] = config.api_key

          res = http.request(req)
          code = res.code.to_i
          body = res.body.to_s
          if code >= 200 && code < 300
            JSON.parse(body)
          else
            raise SearchEngine::Errors::Api.new("typesense api error: #{code}", status: code, body: body)
          end
        end
      end
    end
  end
end
