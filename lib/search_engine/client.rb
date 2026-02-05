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
    REQUEST_ERROR_INSTRUMENTED_KEY = :__se_request_error_instrumented_queue__
    REQUEST_ERROR_INSTRUMENTED_MAX = 32
    REQUEST_ERROR_INSTRUMENTED_TTL_MS = 60_000.0

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
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/client`
    # @see `https://typesense.org/docs/latest/api/documents.html#search-document`
    def search(collection:, params:, url_opts: {})
      services.fetch(:search).call(collection: collection, params: params, url_opts: url_opts)
    end

    # Resolve a logical collection name that might be an alias to the physical collection name.
    #
    # @param logical_name [String]
    # @param timeout_ms [Integer, nil] optional read-timeout override in ms
    # @return [String, nil] physical collection name when alias exists; nil when alias not found
    # @raise [SearchEngine::Errors::*] on network or API errors other than 404
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/schema`
    # @see `https://typesense.org/docs/latest/api/aliases.html`
    def resolve_alias(logical_name, timeout_ms: nil)
      services.fetch(:collections).resolve_alias(logical_name, timeout_ms: timeout_ms)
    end

    # Retrieve the live schema for a physical collection name.
    #
    # @param collection_name [String]
    # @param timeout_ms [Integer, nil] optional read-timeout override in ms
    # @return [Hash, nil] schema hash when found; nil when collection not found (404)
    # @raise [SearchEngine::Errors::*] on other network or API errors
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/schema`
    # @see `https://typesense.org/docs/latest/api/collections.html`
    def retrieve_collection_schema(collection_name, timeout_ms: nil)
      services.fetch(:collections).retrieve_schema(collection_name, timeout_ms: timeout_ms)
    end

    # Upsert an alias to point to the provided physical collection (atomic server-side swap).
    # @param alias_name [String]
    # @param physical_name [String]
    # @return [Hash]
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/schema#lifecycle`
    # @see `https://typesense.org/docs/latest/api/aliases.html#upsert-an-alias`
    def upsert_alias(alias_name, physical_name)
      services.fetch(:collections).upsert_alias(alias_name, physical_name)
    end

    # Create a new physical collection with the given schema.
    # @param schema [Hash] Typesense schema body
    # @return [Hash] created collection schema
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/schema#lifecycle`
    # @see `https://typesense.org/docs/latest/api/collections.html#create-a-collection`
    def create_collection(schema)
      services.fetch(:collections).create(schema)
    end

    # Update a physical collection's schema in-place.
    # @param name [String]
    # @param schema [Hash] Typesense schema body with fields to add/drop
    # @return [Hash] updated collection schema
    # @see `https://typesense.org/docs/latest/api/collections.html#update-collection`
    def update_collection(name, schema)
      services.fetch(:collections).update(name, schema)
    end

    # Delete a physical collection by name.
    # @param name [String]
    # @param timeout_ms [Integer, nil] optional read-timeout override in ms; when nil, a safer
    #   default suitable for destructive operations is used (prefers indexer timeout, with
    #   a minimum of 30s).
    # @return [Hash] Typesense delete response
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/schema#lifecycle`
    # @see `https://typesense.org/docs/latest/api/collections.html#delete-a-collection`
    def delete_collection(name, timeout_ms: nil)
      effective_timeout_ms = begin
        if timeout_ms&.to_i&.positive?
          timeout_ms.to_i
        else
          # Prefer a longer timeout for potentially long-running delete operations
          idx = begin
            config.indexer&.timeout_ms
          rescue StandardError
            nil
          end
          base = idx.to_i.positive? ? idx.to_i : config.timeout_ms.to_i
          base < 30_000 ? 30_000 : base
        end
      rescue StandardError
        30_000
      end
      services.fetch(:collections).delete(name, timeout_ms: effective_timeout_ms)
    end

    # List all collections.
    # @param timeout_ms [Integer, nil] optional read-timeout override in ms
    # @return [Array<Hash>] list of collection metadata
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/schema`
    # @see `https://typesense.org/docs/latest/api/collections.html#list-all-collections`
    def list_collections(timeout_ms: nil)
      services.fetch(:collections).list(timeout_ms: timeout_ms)
    end

    # Perform a server health check.
    # @return [Hash] Typesense health response (symbolized where applicable)
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/troubleshooting`
    # @see `https://typesense.org/docs/latest/api/cluster-operations.html#health`
    def health
      services.fetch(:operations).health
    end

    # Retrieve server metrics (raw output).
    #
    # Returns the unmodified JSON object from the Typesense `/metrics.json`
    # endpoint. Keys are not symbolized to preserve the raw shape.
    #
    # @return [Hash] Raw payload from `/metrics.json`
    # @see `https://typesense.org/docs/latest/api/cluster-operations.html#metrics`
    def metrics
      services.fetch(:operations).metrics
    end

    # Retrieve server stats (raw output).
    #
    # Returns the unmodified JSON object from the Typesense `/stats.json`
    # endpoint. Keys are not symbolized to preserve the raw shape.
    #
    # @return [Hash] Raw payload from `/stats.json`
    # @see `https://typesense.org/docs/latest/api/cluster-operations.html#stats`
    def stats
      services.fetch(:operations).stats
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
    # We map per-collection synonym IDs to a dedicated synonym set.

    # @param collection [String]
    # @param id [String]
    # @param terms [Array<String>]
    # @return [Hash]
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/synonyms-stopwords`
    # @see `https://typesense.org/docs/latest/api/synonyms.html#upsert-a-synonym`
    def synonyms_upsert(collection:, id:, terms:)
      set = synonym_set_name_for_collection(collection)
      synonym_set_item_request(
        method: :put,
        set: set,
        id: id,
        body_data: Array(terms)
      )
    end

    # @return [Array<Hash>]
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/synonyms-stopwords`
    # @see `https://typesense.org/docs/latest/api/synonyms.html#list-all-synonyms-in-a-synonym-set`
    def synonyms_list(collection:)
      set = synonym_set_name_for_collection(collection)
      raw = synonym_set_item_request(
        method: :get,
        set: set
      )
      extract_synonym_items(raw)
    end

    # @return [Hash, nil]
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/synonyms-stopwords`
    # @see `https://typesense.org/docs/latest/api/synonyms.html#retrieve-a-synonym`
    def synonyms_get(collection:, id:)
      set = synonym_set_name_for_collection(collection)
      synonym_set_item_request(
        method: :get,
        set: set,
        id: id,
        return_nil_on_404: true
      )
    end

    # @return [Hash]
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/synonyms-stopwords`
    # @see `https://typesense.org/docs/latest/api/synonyms.html#delete-a-synonym`
    def synonyms_delete(collection:, id:)
      set = synonym_set_name_for_collection(collection)
      synonym_set_item_request(
        method: :delete,
        set: set,
        id: id
      )
    end

    # --- Admin: Stopwords ---------------------------------------------------

    # @param collection [String]
    # @param id [String]
    # @param terms [Array<String>]
    # @return [Hash]
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/synonyms-stopwords`
    # @see `https://typesense.org/docs/latest/api/stopwords.html#upsert-a-stopwords`
    def stopwords_upsert(collection:, id:, terms:)
      admin_resource_request(
        resource_type: :stopwords,
        method: :put,
        collection: collection,
        id: id,
        body_data: Array(terms)
      )
    end

    # @return [Array<Hash>]
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/synonyms-stopwords`
    # @see `https://typesense.org/docs/latest/api/stopwords.html#list-all-stopwords-of-a-collection`
    def stopwords_list(collection:)
      admin_resource_request(
        resource_type: :stopwords,
        method: :get,
        collection: collection
      )
    end

    # @return [Hash, nil]
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/synonyms-stopwords`
    # @see `https://typesense.org/docs/latest/api/stopwords.html#retrieve-a-stopword`
    def stopwords_get(collection:, id:)
      admin_resource_request(
        resource_type: :stopwords,
        method: :get,
        collection: collection,
        id: id,
        return_nil_on_404: true
      )
    end

    # @return [Hash]
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/synonyms-stopwords`
    # @see `https://typesense.org/docs/latest/api/stopwords.html#delete-a-stopword`
    def stopwords_delete(collection:, id:)
      admin_resource_request(
        resource_type: :stopwords,
        method: :delete,
        collection: collection,
        id: id
      )
    end

    # -----------------------------------------------------------------------

    # Bulk import JSONL documents into a collection using Typesense import API.
    #
    # @param collection [String] physical collection name
    # @param jsonl [String] newline-delimited JSON payload
    # @param action [Symbol, String] one of :upsert, :create, :update (default: :upsert)
    # @return [Object] upstream return (String of JSONL statuses or Array of Hashes depending on gem version)
    # @raise [SearchEngine::Errors::*]
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/indexer`
    # @see `https://typesense.org/docs/latest/api/documents.html#import-documents`
    def import_documents(collection:, jsonl:, action: :upsert)
      services.fetch(:documents).import(collection: collection, jsonl: jsonl, action: action)
    end

    # Delete documents by filter from a collection.
    # @param collection [String] physical collection name
    # @param filter_by [String] Typesense filter string
    # @param timeout_ms [Integer, nil] optional read timeout override in ms
    # @return [Hash] response from Typesense client (symbolized)
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/indexer#stale-deletes`
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
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/multi-search-guide`
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

    # Internal helper for synonyms and stopwords CRUD operations.
    #
    # @param resource_type [Symbol] :synonyms or :stopwords
    # @param method [Symbol] HTTP method :get, :put, or :delete
    # @param collection [String] collection name
    # @param id [String, nil] resource id (required for get/put/delete on specific resource)
    # @param body_data [Hash, nil] request body data (for put operations)
    # @param return_nil_on_404 [Boolean] return nil instead of raising on 404 (for get operations)
    # @return [Hash, Array<Hash>, nil] response data (symbolized keys) or nil if 404 and return_nil_on_404 is true
    # @raise [SearchEngine::Errors::Api] on API errors (unless 404 and return_nil_on_404 is true)
    def admin_resource_request(resource_type:, method:, collection:, id: nil, body_data: nil, return_nil_on_404: false)
      c = collection.to_s
      s = id.to_s if id
      ts = typesense
      start = current_monotonic_ms

      # Build path based on resource type and operation
      path = if s
               # For operations with id: /collections/{c}/synonyms/{id} or /collections/{c}/stopwords/{id}
               path_prefix = case resource_type
                             when :synonyms
                               Client::RequestBuilder::COLLECTIONS_PREFIX + c + Client::RequestBuilder::SYNONYMS_PREFIX
                             when :stopwords
                               Client::RequestBuilder::COLLECTIONS_PREFIX + c + Client::RequestBuilder::STOPWORDS_PREFIX
                             else
                               raise ArgumentError, "Unknown resource_type: #{resource_type.inspect}"
                             end
               path_prefix + s
             else
               # For list (get without id): /collections/{c}/synonyms or /collections/{c}/stopwords
               path_suffix = case resource_type
                             when :synonyms
                               Client::RequestBuilder::SYNONYMS_SUFFIX
                             when :stopwords
                               Client::RequestBuilder::STOPWORDS_SUFFIX
                             else
                               raise ArgumentError, "Unknown resource_type: #{resource_type.inspect}"
                             end
               Client::RequestBuilder::COLLECTIONS_PREFIX + c + path_suffix
             end

      # Build request body for put operations
      request_body = if method == :put && body_data
                       resource_key = resource_type == :synonyms ? :synonyms : :stopwords
                       { resource_key => body_data }
                     else
                       {}
                     end

      result = with_exception_mapping(method, path, {}, start) do
        execute_admin_resource_request(ts, c, s, resource_type, method, request_body)
      end
      symbolize_keys_deep(result)
    rescue Errors::Api => error
      return nil if return_nil_on_404 && error.status.to_i == 404

      raise
    ensure
      instrument(method, path, (start ? (current_monotonic_ms - start) : 0.0), {}, request_token: start)
    end

    # Internal helper for synonym set item CRUD.
    #
    # @param method [Symbol]
    # @param set [String] synonym set name
    # @param id [String, nil] synonym id
    # @param body_data [Array<String>, nil]
    # @param return_nil_on_404 [Boolean]
    # @return [Hash, Array<Hash>, nil]
    def synonym_set_item_request(method:, set:, id: nil, body_data: nil, return_nil_on_404: false)
      set_name = set.to_s
      sid = id.to_s if id
      start = current_monotonic_ms

      path = if sid
               "/synonym_sets/#{escape_segment(set_name)}/synonyms/#{escape_segment(sid)}"
             else
               "/synonym_sets/#{escape_segment(set_name)}/synonyms"
             end

      request_body = if method == :put && body_data
                       { synonyms: body_data }
                     else
                       {}
                     end

      result = with_exception_mapping(method, path, {}, start) do
        execute_synonym_set_request(typesense_api_call, method, path, request_body)
      end
      symbolize_keys_deep(result)
    rescue Errors::Api => error
      return nil if return_nil_on_404 && error.status.to_i == 404

      raise
    ensure
      instrument(method, path, (start ? (current_monotonic_ms - start) : 0.0), {}, request_token: start)
    end

    def execute_synonym_set_request(api_call, method, path, request_body)
      raise ArgumentError, 'Typesense client missing configuration' unless api_call

      case method
      when :get
        api_call.get(path)
      when :put
        api_call.put(path, request_body)
      when :delete
        api_call.delete(path)
      else
        raise ArgumentError, "Unsupported method: #{method.inspect}"
      end
    end

    def extract_synonym_items(raw)
      return [] if raw.nil?
      return raw if raw.is_a?(Array)

      if raw.is_a?(Hash)
        list = raw[:synonyms] || raw['synonyms'] || raw[:items] || raw['items'] || raw[:results] || raw['results']
        return Array(list) if list
      end

      Array(raw)
    end

    def synonym_set_name_for_collection(collection)
      "#{collection}_synonyms_index"
    end

    def typesense_api_call
      @typesense_api_call ||= begin
        require 'typesense'
        ts = typesense
        Typesense::ApiCall.new(ts.configuration) if ts.respond_to?(:configuration)
      end
    end

    def escape_segment(value)
      require 'uri'
      URI.encode_www_form_component(value.to_s)
    end

    # Execute the actual Typesense API call for admin resources.
    def execute_admin_resource_request(ts, collection, id, resource_type, method, request_body)
      case method
      when :get
        if id
          ts.collections[collection].public_send(resource_type)[id].retrieve
        else
          ts.collections[collection].public_send(resource_type).retrieve
        end
      when :put
        ts.collections[collection].public_send(resource_type)[id].upsert(request_body)
      when :delete
        ts.collections[collection].public_send(resource_type)[id].delete
      else
        raise ArgumentError, "Unsupported method: #{method.inspect}"
      end
    end

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
        # typesense-ruby uses a single connection timeout for both open+read
        connection_timeout_seconds: read_timeout_seconds,
        num_retries: safe_retry_attempts,
        retry_interval_seconds: safe_retry_backoff,
        logger: safe_logger,
        log_level: safe_typesense_log_level
      )
    end

    def build_typesense_client
      require 'typesense'

      Typesense::Client.new(
        nodes: build_nodes,
        api_key: config.api_key,
        # Single timeout governs both open/read in typesense-ruby
        connection_timeout_seconds: (config.timeout_ms.to_i / 1000.0),
        num_retries: safe_retry_attempts,
        retry_interval_seconds: safe_retry_backoff,
        logger: safe_logger,
        log_level: safe_typesense_log_level
      )
    rescue StandardError => error
      raise error
    end

    def build_nodes
      proto =
        begin
          config.protocol.to_s.strip.presence || 'http'
        rescue StandardError
          nil
        end
      [
        {
          host: config.host,
          port: config.port,
          protocol: proto
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

    def safe_typesense_log_level
      lvl_sym = begin
        SearchEngine.config.logging.level if SearchEngine.config.respond_to?(:logging) && SearchEngine.config.logging
      rescue StandardError
        nil
      end
      require 'logger'
      mapping = {
        debug: ::Logger::DEBUG,
        info: ::Logger::INFO,
        warn: ::Logger::WARN,
        error: ::Logger::ERROR,
        fatal: ::Logger::FATAL
      }
      resolved = mapping[lvl_sym.to_s.downcase.to_sym] || ::Logger::WARN
      # Clamp noisy Typesense debug unless explicitly enabled via env.
      if resolved == ::Logger::DEBUG && !debug_http_enabled_env?
        ::Logger::INFO
      else
        resolved
      end
    end

    def debug_http_enabled_env?
      val = ENV['SEARCH_ENGINE_DEBUG_HTTP']
      return false if val.nil?

      s = val.to_s.strip.downcase
      return false if s.empty?

      %w[1 true yes on].include?(s)
    rescue StandardError
      false
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
      duration_ms = current_monotonic_ms - start_ms

      if api_error?(error)
        return handle_api_error(
          error, method, path, cache_params, duration_ms, request_token: start_ms
        )
      end
      if timeout_error?(error)
        return handle_timeout_error(
          error, method, path, cache_params, duration_ms, request_token: start_ms
        )
      end
      if connection_error?(error)
        return handle_connection_error(
          error, method, path, cache_params, duration_ms, request_token: start_ms
        )
      end

      # Unmapped error: instrument and re-raise as-is
      instrument(method, path, duration_ms, cache_params, error_class: error.class.name, request_token: start_ms)
      mark_request_error_instrumented(method, path, start_ms)
      raise error
    end

    # Check if error is a Typesense API error.
    def api_error?(error)
      error.respond_to?(:http_code) || error.class.name.start_with?('Typesense::Error')
    end

    # Handle Typesense API errors by converting to SearchEngine::Errors::Api.
    def handle_api_error(error, method, path, cache_params, duration_ms, request_token: nil)
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
      instrument(method, path, duration_ms, cache_params, error_class: err.class.name, request_token: request_token)
      mark_request_error_instrumented(method, path, request_token)
      raise err
    end

    # Handle timeout errors by converting to SearchEngine::Errors::Timeout.
    def handle_timeout_error(error, method, path, cache_params, duration_ms, request_token: nil)
      instrument(
        method,
        path,
        duration_ms,
        cache_params,
        error_class: Errors::Timeout.name,
        request_token: request_token
      )
      mark_request_error_instrumented(method, path, request_token)
      raise Errors::Timeout.new(
        error.message,
        doc: Client::RequestBuilder::DOC_CLIENT_ERRORS,
        details: { op: method, path: path }
      )
    end

    # Handle connection errors by converting to SearchEngine::Errors::Connection.
    def handle_connection_error(error, method, path, cache_params, duration_ms, request_token: nil)
      instrument(
        method,
        path,
        duration_ms,
        cache_params,
        error_class: Errors::Connection.name,
        request_token: request_token
      )
      mark_request_error_instrumented(method, path, request_token)
      raise Errors::Connection.new(
        error.message,
        doc: Client::RequestBuilder::DOC_CLIENT_ERRORS,
        details: { op: method, path: path }
      )
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

    def instrument(method, path, duration_ms, cache_params, error_class: nil, request_token: nil)
      return unless defined?(ActiveSupport::Notifications)
      return if error_class.nil? && consume_request_error_instrumented?(method, path, request_token)

      ActiveSupport::Notifications.instrument(
        'search_engine.request',
        method: method,
        path: path,
        duration_ms: duration_ms,
        url_opts: Observability.filtered_url_opts(cache_params),
        error_class: error_class
      )
    end

    def mark_request_error_instrumented(method, path, request_token = nil)
      return nil if request_token.nil?

      queue = Thread.current[REQUEST_ERROR_INSTRUMENTED_KEY] ||= []
      now = current_monotonic_ms
      prune_request_error_instrumented_queue!(queue, now)
      queue << { method: method.to_sym, path: path.to_s, token: request_token, at: now }
      queue.shift while queue.size > REQUEST_ERROR_INSTRUMENTED_MAX
      nil
    rescue StandardError
      nil
    end

    def consume_request_error_instrumented?(method, path, request_token = nil)
      return false if request_token.nil?

      queue = Thread.current[REQUEST_ERROR_INSTRUMENTED_KEY]
      return false unless queue.is_a?(Array) && !queue.empty?

      now = current_monotonic_ms
      prune_request_error_instrumented_queue!(queue, now)
      idx = queue.index do |entry|
        entry[:method] == method.to_sym && entry[:path] == path.to_s && entry[:token] == request_token
      end
      return false unless idx

      queue.delete_at(idx)
      true
    rescue StandardError
      false
    end

    def prune_request_error_instrumented_queue!(queue, now_ms)
      queue.reject! do |entry|
        now_ms - entry[:at].to_f > REQUEST_ERROR_INSTRUMENTED_TTL_MS
      end
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
