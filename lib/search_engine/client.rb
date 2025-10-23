# frozen_string_literal: true

require 'search_engine/client_options'
require 'search_engine/errors'
require 'search_engine/observability'
require 'search_engine/client/request_builder'
require 'search_engine/client/services'

module SearchEngine
  # Thin wrapper on top of the official `typesense` gem.
  #
  # Provides single-search and federated multi-search while enforcing that cache
  # knobs live in URL/common-params and not in per-search request bodies.
  class Client
    # @param config [SearchEngine::Config]
    # @param typesense_client [Object, nil] optional injected Typesense::Client (for tests)
    def initialize(config: SearchEngine.config, typesense_client: nil)
      @config = config
      @typesense = typesense_client
      @services = Services.build(self)
    end

    # Execute a single search against a collection.
    #
    # @param collection [String] collection name
    # @param params [Hash] Typesense search parameters (q, query_by, etc.)
    # @param url_opts [Hash] URL/common knobs (use_cache, cache_ttl)
    # @return [SearchEngine::Result] Wrapped response with hydrated hits
    # @raise [SearchEngine::Errors::InvalidParams, SearchEngine::Errors::*]
    # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Client`
    # @see `https://typesense.org/docs/latest/api/documents.html#search-document`
    def search(collection:, params:, url_opts: {})
      services.fetch(:search).call(collection: collection, params: params, url_opts: url_opts)
    end

    # Resolve a logical collection name that might be an alias to the physical collection name.
    #
    # @param logical_name [String]
    # @return [String, nil] physical collection name when alias exists; nil when alias not found
    # @raise [SearchEngine::Errors::*] on network or API errors other than 404
    # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Schema`
    # @see `https://typesense.org/docs/latest/api/aliases.html`
    def resolve_alias(logical_name)
      services.fetch(:collections).resolve_alias(logical_name)
    end

    # Retrieve the live schema for a physical collection name.
    #
    # @param collection_name [String]
    # @return [Hash, nil] schema hash when found; nil when collection not found (404)
    # @raise [SearchEngine::Errors::*] on other network or API errors
    # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Schema`
    # @see `https://typesense.org/docs/latest/api/collections.html`
    def retrieve_collection_schema(collection_name)
      services.fetch(:collections).retrieve_schema(collection_name)
    end

    # Upsert an alias to point to the provided physical collection (atomic server-side swap).
    # @param alias_name [String]
    # @param physical_name [String]
    # @return [Hash]
    # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Schema#lifecycle`
    # @see `https://typesense.org/docs/latest/api/aliases.html#upsert-an-alias`
    def upsert_alias(alias_name, physical_name)
      services.fetch(:collections).upsert_alias(alias_name, physical_name)
    end

    # Create a new physical collection with the given schema.
    # @param schema [Hash] Typesense schema body
    # @return [Hash] created collection schema
    # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Schema#lifecycle`
    # @see `https://typesense.org/docs/latest/api/collections.html#create-a-collection`
    def create_collection(schema)
      services.fetch(:collections).create(schema)
    end

    # Delete a physical collection by name.
    # @param name [String]
    # @return [Hash] Typesense delete response
    # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Schema#lifecycle`
    # @see `https://typesense.org/docs/latest/api/collections.html#delete-a-collection`
    def delete_collection(name)
      services.fetch(:collections).delete(name)
    end

    # List all collections.
    # @return [Array<Hash>] list of collection metadata
    # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Schema`
    # @see `https://typesense.org/docs/latest/api/collections.html#list-all-collections`
    def list_collections
      services.fetch(:collections).list
    end

    # Perform a server health check.
    # @return [Hash] Typesense health response (symbolized where applicable)
    # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Troubleshooting`
    # @see `https://typesense.org/docs/latest/api/cluster-operations.html#health`
    def health
      services.fetch(:operations).health
    end

    # --- Admin: API Keys ------------------------------------------------------

    # List API keys configured on the Typesense server.
    #
    # @return [Array<Hash>] list of keys with symbolized fields when possible
    # @see `https://typesense.org/docs/latest/api/api-keys.html#list-keys`
    def list_api_keys
      services.fetch(:operations).list_api_keys
    end

    # --- Admin: Synonyms ----------------------------------------------------
    # NOTE: We rely on the official client's endpoints; names are mapped here.

    # @param collection [String]
    # @param id [String]
    # @param terms [Array<String>]
    # @return [Hash]
    # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Synonyms-Stopwords`
    # @see `https://typesense.org/docs/latest/api/synonyms.html#upsert-a-synonym`
    def synonyms_upsert(collection:, id:, terms:)
      c = collection.to_s
      s = id.to_s
      list = Array(terms)
      ts = typesense
      start = current_monotonic_ms
      path = Client::RequestBuilder::COLLECTIONS_PREFIX + c + Client::RequestBuilder::SYNONYMS_PREFIX + s

      result = with_exception_mapping(:put, path, {}, start) do
        ts.collections[c].synonyms[s].upsert({ synonyms: list })
      end
      symbolize_keys_deep(result)
    ensure
      instrument(:put, path, (start ? (current_monotonic_ms - start) : 0.0), {})
    end

    # @return [Array<Hash>]
    # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Synonyms-Stopwords`
    # @see `https://typesense.org/docs/latest/api/synonyms.html#list-all-synonyms-of-a-collection`
    def synonyms_list(collection:)
      c = collection.to_s
      ts = typesense
      start = current_monotonic_ms
      path = Client::RequestBuilder::COLLECTIONS_PREFIX + c + Client::RequestBuilder::SYNONYMS_SUFFIX
      result = with_exception_mapping(:get, path, {}, start) do
        ts.collections[c].synonyms.retrieve
      end
      symbolize_keys_deep(result)
    ensure
      instrument(:get, path, (start ? (current_monotonic_ms - start) : 0.0), {})
    end

    # @return [Hash, nil]
    # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Synonyms-Stopwords`
    # @see `https://typesense.org/docs/latest/api/synonyms.html#retrieve-a-synonym`
    def synonyms_get(collection:, id:)
      c = collection.to_s
      s = id.to_s
      ts = typesense
      start = current_monotonic_ms
      path = Client::RequestBuilder::COLLECTIONS_PREFIX + c + Client::RequestBuilder::SYNONYMS_PREFIX + s
      result = with_exception_mapping(:get, path, {}, start) do
        ts.collections[c].synonyms[s].retrieve
      end
      symbolize_keys_deep(result)
    rescue Errors::Api => error
      return nil if error.status.to_i == 404

      raise
    ensure
      instrument(:get, path, (start ? (current_monotonic_ms - start) : 0.0), {})
    end

    # @return [Hash]
    # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Synonyms-Stopwords`
    # @see `https://typesense.org/docs/latest/api/synonyms.html#delete-a-synonym`
    def synonyms_delete(collection:, id:)
      c = collection.to_s
      s = id.to_s
      ts = typesense
      start = current_monotonic_ms
      path = Client::RequestBuilder::COLLECTIONS_PREFIX + c + Client::RequestBuilder::SYNONYMS_PREFIX + s
      result = with_exception_mapping(:delete, path, {}, start) do
        ts.collections[c].synonyms[s].delete
      end
      symbolize_keys_deep(result)
    ensure
      instrument(:delete, path, (start ? (current_monotonic_ms - start) : 0.0), {})
    end

    # --- Admin: Stopwords ---------------------------------------------------

    # @param collection [String]
    # @param id [String]
    # @param terms [Array<String>]
    # @return [Hash]
    # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Synonyms-Stopwords`
    # @see `https://typesense.org/docs/latest/api/stopwords.html#upsert-a-stopwords`
    def stopwords_upsert(collection:, id:, terms:)
      c = collection.to_s
      s = id.to_s
      list = Array(terms)
      ts = typesense
      start = current_monotonic_ms
      path = Client::RequestBuilder::COLLECTIONS_PREFIX + c + Client::RequestBuilder::STOPWORDS_PREFIX + s

      result = with_exception_mapping(:put, path, {}, start) do
        ts.collections[c].stopwords[s].upsert({ stopwords: list })
      end
      symbolize_keys_deep(result)
    ensure
      instrument(:put, path, (start ? (current_monotonic_ms - start) : 0.0), {})
    end

    # @return [Array<Hash>]
    # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Synonyms-Stopwords`
    # @see `https://typesense.org/docs/latest/api/stopwords.html#list-all-stopwords-of-a-collection`
    def stopwords_list(collection:)
      c = collection.to_s
      ts = typesense
      start = current_monotonic_ms
      path = Client::RequestBuilder::COLLECTIONS_PREFIX + c + Client::RequestBuilder::STOPWORDS_SUFFIX
      result = with_exception_mapping(:get, path, {}, start) do
        ts.collections[c].stopwords.retrieve
      end
      symbolize_keys_deep(result)
    ensure
      instrument(:get, path, (start ? (current_monotonic_ms - start) : 0.0), {})
    end

    # @return [Hash, nil]
    # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Synonyms-Stopwords`
    # @see `https://typesense.org/docs/latest/api/stopwords.html#retrieve-a-stopword`
    def stopwords_get(collection:, id:)
      c = collection.to_s
      s = id.to_s
      ts = typesense
      start = current_monotonic_ms
      path = Client::RequestBuilder::COLLECTIONS_PREFIX + c + Client::RequestBuilder::STOPWORDS_PREFIX + s
      result = with_exception_mapping(:get, path, {}, start) do
        ts.collections[c].stopwords[s].retrieve
      end
      symbolize_keys_deep(result)
    rescue Errors::Api => error
      return nil if error.status.to_i == 404

      raise
    ensure
      instrument(:get, path, (start ? (current_monotonic_ms - start) : 0.0), {})
    end

    # @return [Hash]
    # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Synonyms-Stopwords`
    # @see `https://typesense.org/docs/latest/api/stopwords.html#delete-a-stopword`
    def stopwords_delete(collection:, id:)
      c = collection.to_s
      s = id.to_s
      ts = typesense
      start = current_monotonic_ms
      path = Client::RequestBuilder::COLLECTIONS_PREFIX + c + Client::RequestBuilder::STOPWORDS_PREFIX + s
      result = with_exception_mapping(:delete, path, {}, start) do
        ts.collections[c].stopwords[s].delete
      end
      symbolize_keys_deep(result)
    ensure
      instrument(:delete, path, (start ? (current_monotonic_ms - start) : 0.0), {})
    end

    # -----------------------------------------------------------------------

    # Bulk import JSONL documents into a collection using Typesense import API.
    #
    # @param collection [String] physical collection name
    # @param jsonl [String] newline-delimited JSON payload
    # @param action [Symbol, String] one of :upsert, :create, :update (default: :upsert)
    # @return [Object] upstream return (String of JSONL statuses or Array of Hashes depending on gem version)
    # @raise [SearchEngine::Errors::*]
    # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Indexer`
    # @see `https://typesense.org/docs/latest/api/documents.html#import-documents`
    def import_documents(collection:, jsonl:, action: :upsert)
      services.fetch(:documents).import(collection: collection, jsonl: jsonl, action: action)
    end

    # Delete documents by filter from a collection.
    # @param collection [String] physical collection name
    # @param filter_by [String] Typesense filter string
    # @param timeout_ms [Integer, nil] optional read timeout override in ms
    # @return [Hash] response from Typesense client (symbolized)
    # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Indexer#stale-deletes`
    # @see `https://typesense.org/docs/latest/api/documents.html#delete-documents-by-query`
    def delete_documents_by_filter(collection:, filter_by:, timeout_ms: nil)
      services.fetch(:documents).delete_by_filter(collection: collection, filter_by: filter_by, timeout_ms: timeout_ms)
    end

    # Delete a single document by id from a collection.
    #
    # @param collection [String] physical collection name
    # @param id [String, #to_s] document id
    # @param timeout_ms [Integer, nil] optional read timeout override in ms
    # @return [Hash, nil] response from Typesense client (symbolized) or nil when 404
    # @see `https://typesense.org/docs/latest/api/documents.html#delete-a-document`
    def delete_document(collection:, id:, timeout_ms: nil)
      services.fetch(:documents).delete(collection: collection, id: id, timeout_ms: timeout_ms)
    end

    # Retrieve a single document by id from a collection.
    # @param collection [String]
    # @param id [String, #to_s]
    # @param timeout_ms [Integer, nil]
    # @return [Hash, nil] document hash or nil when 404
    # @see `https://typesense.org/docs/latest/api/documents.html#retrieve-a-document`
    def retrieve_document(collection:, id:, timeout_ms: nil)
      services.fetch(:documents).retrieve(collection: collection, id: id, timeout_ms: timeout_ms)
    end

    # Partially update a single document by id.
    #
    # @param collection [String] physical collection name
    # @param id [String, #to_s] document id
    # @param fields [Hash] partial fields to update
    # @param timeout_ms [Integer, nil] optional read timeout override in ms
    # @return [Hash] response from Typesense client (symbolized)
    # @see `https://typesense.org/docs/latest/api/documents.html#update-a-document`
    def update_document(collection:, id:, fields:, timeout_ms: nil)
      services.fetch(:documents).update(collection: collection, id: id, fields: fields, timeout_ms: timeout_ms)
    end

    # Partially update documents that match a filter.
    #
    # @param collection [String] physical collection name
    # @param filter_by [String] Typesense filter string
    # @param fields [Hash] partial fields to update
    # @param timeout_ms [Integer, nil] optional read timeout override in ms
    # @return [Hash] response from Typesense client (symbolized)
    # @see `https://typesense.org/docs/latest/api/documents.html#update-documents-by-query`
    def update_documents_by_filter(collection:, filter_by:, fields:, timeout_ms: nil)
      services.fetch(:documents).update_by_filter(collection: collection, filter_by: filter_by, fields: fields,
                                                  timeout_ms: timeout_ms
      )
    end

    # Create a single document in a collection.
    #
    # @param collection [String] physical collection name
    # @param document [Hash] Typesense document body
    # @return [Hash] created document as returned by Typesense (symbolized)
    # @raise [SearchEngine::Errors::InvalidParams, SearchEngine::Errors::*]
    # @see `https://typesense.org/docs/latest/api/documents.html#create-a-document`
    def create_document(collection:, document:)
      services.fetch(:documents).create(collection: collection, document: document)
    end

    # Execute a multi-search across multiple collections.
    #
    # @param searches [Array<Hash>] per-entry request bodies produced by Multi#to_payloads
    # @param url_opts [Hash] URL/common knobs (use_cache, cache_ttl)
    # @return [Hash] Raw Typesense multi-search response with key 'results'
    # @raise [SearchEngine::Errors::InvalidParams, SearchEngine::Errors::*]
    # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Multi-search-Guide`
    # @see `https://typesense.org/docs/latest/api/#multi-search`
    def multi_search(searches:, url_opts: {})
      services.fetch(:search).multi(searches: searches, url_opts: url_opts)
    end

    # Clear the Typesense server-side search cache.
    #
    # @return [Hash] response payload from Typesense (symbolized keys)
    # @see `https://typesense.org/docs/latest/api/cluster-operations.html#clear-cache`
    def clear_cache
      services.fetch(:operations).clear_cache
    end

    private

    attr_reader :config, :services

    def typesense
      @typesense ||= build_typesense_client
    end

    def typesense_for_import
      import_timeout = begin
        config.indexer&.timeout_ms
      rescue StandardError
        nil
      end
      if import_timeout&.to_i&.positive? && import_timeout.to_i != config.timeout_ms.to_i
        build_typesense_client_with_read_timeout(import_timeout.to_i / 1000.0)
      else
        typesense
      end
    end

    def build_typesense_client_with_read_timeout(read_timeout_seconds)
      require 'typesense'

      Typesense::Client.new(
        nodes: build_nodes,
        api_key: config.api_key,
        connection_timeout_seconds: (config.open_timeout_ms.to_i / 1000.0),
        read_timeout_seconds: read_timeout_seconds,
        num_retries: safe_retry_attempts,
        retry_interval_seconds: safe_retry_backoff,
        logger: safe_logger
      )
    end

    def build_typesense_client
      require 'typesense'

      Typesense::Client.new(
        nodes: build_nodes,
        api_key: config.api_key,
        connection_timeout_seconds: (config.open_timeout_ms.to_i / 1000.0),
        read_timeout_seconds: (config.timeout_ms.to_i / 1000.0),
        num_retries: safe_retry_attempts,
        retry_interval_seconds: safe_retry_backoff,
        logger: safe_logger
      )
    end

    def build_nodes
      [
        {
          host: config.host,
          port: config.port,
          protocol: config.protocol
        }
      ]
    end

    def safe_logger
      config.logger
    rescue StandardError
      nil
    end

    def safe_retry_attempts
      r = begin
        config.retries
      rescue StandardError
        nil
      end
      return 0 unless r.is_a?(Hash)

      v = r[:attempts]
      v = v.to_i if v.respond_to?(:to_i)
      v.is_a?(Integer) && v >= 0 ? v : 0
    end

    def safe_retry_backoff
      r = begin
        config.retries
      rescue StandardError
        nil
      end
      return 0.0 unless r.is_a?(Hash)

      v = r[:backoff]
      v = v.to_f if v.respond_to?(:to_f)
      v.is_a?(Float) && v >= 0.0 ? v : 0.0
    end

    def derive_cache_opts(url_opts)
      merged = ClientOptions.url_options_from_config(config)
      merged[:use_cache] = url_opts[:use_cache] if url_opts.key?(:use_cache) && !url_opts[:use_cache].nil?
      merged[:cache_ttl] = Integer(url_opts[:cache_ttl]) if url_opts.key?(:cache_ttl)
      merged
    end

    def validate_single!(collection, params)
      unless collection.is_a?(String) && !collection.strip.empty?
        raise Errors::InvalidParams, 'collection must be a non-empty String'
      end

      raise Errors::InvalidParams, 'params must be a Hash' unless params.is_a?(Hash)
    end

    def validate_multi!(searches)
      unless searches.is_a?(Array) && searches.all? { |s| s.is_a?(Hash) }
        raise Errors::InvalidParams, 'searches must be an Array of Hashes'
      end

      searches.each_with_index do |s, idx|
        unless s.key?(:collection) && s[:collection].is_a?(String) && !s[:collection].strip.empty?
          raise Errors::InvalidParams, "searches[#{idx}][:collection] must be a non-empty String"
        end
      end
    end

    def with_exception_mapping(method, path, cache_params, start_ms)
      yield
    rescue StandardError => error
      map_and_raise(error, method, path, cache_params, start_ms)
    end

    # Map network and API exceptions into stable SearchEngine errors, with
    # redaction and logging.
    def map_and_raise(error, method, path, cache_params, start_ms)
      if error.respond_to?(:http_code) || error.class.name.start_with?('Typesense::Error')
        status = if error.respond_to?(:http_code)
                   error.http_code
                 else
                   infer_typesense_status(error)
                 end
        body = parse_error_body(error)
        err = Errors::Api.new(
          "typesense api error: #{status}",
          status: status || 500,
          body: body,
          doc: Client::RequestBuilder::DOC_CLIENT_ERRORS,
          details: { http_status: status, body: body.is_a?(String) ? body[0, 120] : body }
        )
        instrument(method, path, current_monotonic_ms - start_ms, cache_params, error_class: err.class.name)
        raise err
      end

      if timeout_error?(error)
        instrument(method, path, current_monotonic_ms - start_ms, cache_params, error_class: Errors::Timeout.name)
        raise Errors::Timeout.new(
          error.message,
          doc: Client::RequestBuilder::DOC_CLIENT_ERRORS,
          details: { op: method, path: path }
        )
      end

      if connection_error?(error)
        instrument(method, path, current_monotonic_ms - start_ms, cache_params, error_class: Errors::Connection.name)
        raise Errors::Connection.new(
          error.message,
          doc: Client::RequestBuilder::DOC_CLIENT_ERRORS,
          details: { op: method, path: path }
        )
      end

      instrument(method, path, current_monotonic_ms - start_ms, cache_params, error_class: error.class.name)
      raise error
    end

    def timeout_error?(error)
      error.is_a?(::Timeout::Error) || error.class.name.include?('Timeout')
    end

    def connection_error?(error)
      return true if error.is_a?(SocketError) || error.is_a?(Errno::ECONNREFUSED) || error.is_a?(Errno::ETIMEDOUT)
      return true if error.class.name.include?('Connection')

      defined?(OpenSSL::SSL::SSLError) && error.is_a?(OpenSSL::SSL::SSLError)
    end

    # Infer HTTP status code from typesense-ruby error class names when http_code is unavailable.
    def infer_typesense_status(error)
      klass = error.class.name
      return 404 if klass.include?('ObjectNotFound')
      return 401 if klass.include?('RequestUnauthorized')
      return 403 if klass.include?('RequestForbidden')
      return 400 if klass.include?('RequestMalformed')
      return 409 if klass.include?('ObjectAlreadyExists')
      return 500 if klass.include?('ServerError')

      500
    end

    def instrument(method, path, duration_ms, cache_params, error_class: nil)
      return unless defined?(ActiveSupport::Notifications)

      ActiveSupport::Notifications.instrument(
        'search_engine.request',
        method: method,
        path: path,
        duration_ms: duration_ms,
        url_opts: Observability.filtered_url_opts(cache_params),
        error_class: error_class
      )
    end

    def log_success(method, path, start_ms, cache_params)
      return unless safe_logger

      elapsed = current_monotonic_ms - start_ms
      msg = +'search_engine '
      msg << method.to_s.upcase
      msg << ' '
      msg << path
      msg << ' completed in '
      msg << elapsed.round(1).to_s
      msg << 'ms'
      msg << ' cache='
      msg << (cache_params[:use_cache] ? 'true' : 'false')
      msg << ' ttl='
      msg << cache_params[:cache_ttl].to_s
      safe_logger.info(msg)
    rescue StandardError
      nil
    end

    def parse_error_body(error)
      return error.body if error.respond_to?(:body) && error.body
      return error.message if error.respond_to?(:message) && error.message

      nil
    end

    def current_monotonic_ms
      SearchEngine::Instrumentation.monotonic_ms
    end

    def symbolize_keys_deep(obj)
      case obj
      when Hash
        obj.each_with_object({}) do |(k, v), h|
          h[k.to_sym] = symbolize_keys_deep(v)
        end
      when Array
        obj.map { |e| symbolize_keys_deep(e) }
      else
        obj
      end
    end
  end
end
