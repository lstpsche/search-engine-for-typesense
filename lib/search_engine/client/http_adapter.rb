# frozen_string_literal: true

module SearchEngine
  class Client
    # HttpAdapter is a minimal transport wrapper around the injected Typesense::Client.
    #
    # Provides a very small surface to execute normalized requests produced by
    # {SearchEngine::Client::RequestBuilder}, delegating to the official Typesense
    # client without re-implementing low-level HTTP. It stays transport-only and
    # does not parse or coerce responses beyond returning a simple Response struct.
    #
    # @see SearchEngine::Client::RequestBuilder
    # @see `https://typesense.org/docs/latest/api/`
    class HttpAdapter
      Response = Struct.new(:status, :headers, :body, keyword_init: true)

      TYPESENSE_API_KEY_HEADER = 'X-TYPESENSE-API-KEY'

      # Initialize with an injected Typesense client instance.
      # @param typesense_client [Object] an instance compatible with Typesense::Client
      # @return [void]
      def initialize(typesense_client)
        @typesense = typesense_client
      end

      # Execute a normalized request produced by {SearchEngine::Client::RequestBuilder}.
      #
      # Delegates to the upstream Typesense client where possible, returning a
      # transport-level {Response}. This adapter does not perform parsing or
      # symbolization and intentionally exposes the raw upstream value in
      # {Response#body}.
      #
      # Supported forms (current usage):
      # - POST `/collections/:collection/documents/search` (with `common_params`)
      #
      # @param request [SearchEngine::Client::RequestBuilder::Request] normalized request
      # @return [Response] response wrapper with status, headers and raw body
      # @raise [ArgumentError] when the request path is not supported by this adapter
      # @see `https://typesense.org/docs/latest/api/documents.html#search-document`
      def perform(request)
        method = request.http_method.to_sym
        path = request.path.to_s
        if method == :post && path.match?(%r{\A/collections/[^/]+/documents/search\z})
          collection = path.split('/')[2]
          docs = @typesense.collections[collection].documents
          begin
            params = docs.method(:search).parameters
            supports_common = params.any? { |(kind, _)| %i[key keyreq keyrest].include?(kind) }
          rescue StandardError
            supports_common = false
          end
          raw = supports_common ? docs.search(request.body, common_params: request.params) : docs.search(request.body)
          # The official client returns parsed JSON (Hash). No headers/status exposed here.
          return Response.new(status: 200, headers: {}, body: raw)
        end

        # Fallback: attempt a generic call via low-level typesense endpoint access if available.
        # This keeps adapter permissive for future endpoints without adding Faraday here.
        raise ArgumentError, "Unsupported request path for adapter: #{request.http_method} #{request.path}"
      end
    end
  end
end
