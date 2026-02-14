# frozen_string_literal: true

module SearchEngine
  class Client
    # RequestBuilder maps compiled params to concrete Typesense requests.
    # It has no network concerns; it only assembles the request object.
    #
    # Produces a normalized {Request} that can be executed by
    # {SearchEngine::Client::HttpAdapter} or directly by the client methods.
    # Sanitizes internal-only keys before encoding to ensure the payload is
    # compatible with the Typesense API.
    #
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/client`
    # @see `https://typesense.org/docs/latest/api/`
    class RequestBuilder
      # Normalized request container used by the transport adapter.
      # @!attribute http_method [Symbol] one of :get, :post, :put, :delete
      # @!attribute path [String] absolute path (no host), e.g. "/collections/products/documents/search"
      # @!attribute params [Hash] URL/common params (e.g., { use_cache:, cache_ttl: })
      # @!attribute headers [Hash] per-request headers (adapter may add global ones)
      # @!attribute body [Hash, nil] JSON body as a Ruby Hash
      # @!attribute body_json [String, nil] Deterministic JSON representation of body
      Request = Struct.new(:http_method, :path, :params, :headers, :body, :body_json, keyword_init: true)

      COLLECTIONS_PREFIX = '/collections/'
      DOCUMENTS_SEARCH_SUFFIX = '/documents/search'
      # Additional endpoint fragments for internal reuse
      COLLECTIONS_ROOT = '/collections'
      DOCUMENTS_SUFFIX = '/documents'
      DOCUMENTS_IMPORT_SUFFIX = '/documents/import'
      ALIASES_PREFIX = '/aliases/'
      SYNONYMS_SUFFIX = '/synonyms'
      SYNONYMS_PREFIX = '/synonyms/'
      STOPWORDS_SUFFIX = '/stopwords'
      STOPWORDS_PREFIX = '/stopwords/'
      HEALTH_PATH = '/health'

      CONTENT_TYPE_JSON = 'application/json'
      DEFAULT_HEADERS_JSON = { 'Content-Type' => CONTENT_TYPE_JSON }.freeze

      # Centralized doc links used by client error mapping
      DOC_CLIENT_ERRORS = 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/client#errors'

      INTERNAL_ONLY_KEYS = %i[
        _join
        _selection
        _preset_mode
        _preset_pruned_keys
        _preset_conflicts
        _curation_conflict_type
        _curation_conflict_count
        _runtime_flags
        _hits
        _curation
      ].freeze

      # Build a single-search request for a collection.
      #
      # @param collection [String] logical collection name (alias)
      # @param compiled_params [SearchEngine::CompiledParams] sanitized, deterministic params
      # @param url_opts [Hash] URL/common params such as cache controls
      # @return [Request] normalized request ready for transport
      # @see `https://typesense.org/docs/latest/api/documents.html#search-document`
      def self.build_search(collection:, compiled_params:, url_opts: {})
        name = collection.to_s
        body_hash = sanitize_body_params(compiled_params.to_h)
        # Ensure deterministic ordering even after sanitization
        body_json = SearchEngine::CompiledParams.from(body_hash).to_json

        Request.new(
          http_method: :post,
          path: COLLECTIONS_PREFIX + name + DOCUMENTS_SEARCH_SUFFIX,
          params: url_opts || {},
          headers: DEFAULT_HEADERS_JSON,
          body: body_hash,
          body_json: body_json
        )
      end

      # Remove internal-only keys from the HTTP payload (copied from previous client behavior).
      #
      # @param params [Hash] possibly containing internal keys
      # @return [Hash] new Hash without internal-only keys
      def self.sanitize_body_params(params)
        payload = params.dup
        INTERNAL_ONLY_KEYS.each { |k| payload.delete(k) }
        payload
      end
      private_class_method :sanitize_body_params
    end
  end
end
