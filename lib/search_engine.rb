# frozen_string_literal: true

require 'search_engine/version'
require 'search_engine/config'
require 'search_engine/errors'
require 'search_engine/registry'
require 'search_engine/relation'
require 'search_engine/relation/dx'
require 'search_engine/base'
require 'search_engine/result'
require 'search_engine/filters/sanitizer'
require 'search_engine/ast'
require 'search_engine/dsl/parser'
require 'search_engine/compiler'
require 'search_engine/multi'
require 'search_engine/client_options'
require 'search_engine/client'
require 'search_engine/multi_result'
require 'search_engine/observability'
require 'search_engine/instrumentation'
require 'search_engine/schema'
require 'search_engine/collection_resolver'
require 'search_engine/cascade'
require 'search_engine/indexer'
require 'search_engine/indexer/batch_planner'
require 'search_engine/indexer/import_dispatcher'
require 'search_engine/indexer/retry_policy'
require 'search_engine/indexer/bulk_import'
require 'search_engine/mapper'
require 'search_engine/sources'
require 'search_engine/partitioner'
require 'search_engine/dispatcher'
require 'search_engine/joins/guard'
require 'search_engine/admin'
require 'search_engine/ranking_plan'
require 'search_engine/hydration/selection_context'
require 'search_engine/hydration/materializers'
require 'search_engine/compiled_params'
require 'search_engine/deletion'
require 'search_engine/update'
require 'search_engine/stale_rules'
require 'search_engine/collections_graph'
require 'search_engine/bulk'
require 'search_engine/cache'
require 'search_engine/operations'
require 'search_engine/engine'
require 'search_engine/active_record_syncable'

# Top-level namespace for the SearchEngine gem.
# Provides Typesense integration points for Rails applications.
module SearchEngine
  class << self
    # Access the singleton configuration instance.
    # @return [SearchEngine::Config]
    def config
      @config ||= Config.new
    end

    # Configure the engine in a thread-safe manner.
    #
    # @yieldparam c [SearchEngine::Config]
    # @return [SearchEngine::Config]
    def configure
      raise ArgumentError, 'block required' unless block_given?

      config_mutex.synchronize do
        yield config
        config.validate!
      end
      config
    end

    # Convenience accessor for operational helpers.
    # @return [Module] {SearchEngine::Operations}
    def operations
      Operations
    end

    # Execute a federated multi-search using the Multi builder.
    #
    # Builds and executes a multi-search request, returning a
    # {SearchEngine::Multi::ResultSet} that maps results back to labels.
    # Enforces the configured {SearchEngine::Config#multi_search_limit} before
    # making any network calls.
    #
    # @param common [Hash] optional params merged into each per-search payload after relation compilation
    # @yieldparam m [SearchEngine::Multi] builder to add labeled relations
    # @return [SearchEngine::Multi::ResultSet]
    # @raise [ArgumentError] when the number of searches exceeds the configured limit
    # @example
    #   res = SearchEngine.multi_search(common: { query_by: SearchEngine.config.default_query_by }) do |m|
    #     m.add :products, Product.all.where(active: true).per(10)
    #     m.add :brands,   Brand.all.where('name:~rud').per(5)
    #   end
    #   res[:products].found
    # @note Emits "search_engine.multi_search" via ActiveSupport::Notifications with
    #   payload: { searches_count, labels, http_status, source: :multi }.
    def multi_search(common: {})
      raise ArgumentError, 'block required' unless block_given?

      builder = SearchEngine::Multi.new
      yield builder

      labels = builder.labels
      raw = execute_multi_search_internal(builder: builder, common: common, labels: labels, use_custom_client: false)

      # Typesense client returns symbolized keys; be resilient to both forms.
      # Expect: { results: [ { ... }, ... ] }
      list = Array(raw && (raw[:results] || raw['results']))
      pairs = build_label_result_pairs(list, labels, builder)

      SearchEngine::Multi::ResultSet.new(pairs)
    end

    # Execute a federated multi-search and return a MultiResult wrapper.
    #
    # Non-breaking: this is a convenience helper; {.multi_search} remains unchanged
    # and returns {SearchEngine::Multi::ResultSet}.
    #
    # @param common [Hash] optional params merged into each per-search payload after relation compilation
    # @yieldparam m [SearchEngine::Multi] builder to add labeled relations
    # @return [SearchEngine::MultiResult]
    # @raise [ArgumentError] when the number of searches exceeds the configured limit
    # @raise [SearchEngine::Errors::Api] when Typesense returns a non-2xx status
    # @example
    #   mr = SearchEngine.multi_search_result(common: { query_by: SearchEngine.config.default_query_by }) do |m|
    #     m.add :products, Product.all.per(10)
    #     m.add :brands,   Brand.all.per(5)
    #   end
    #   mr[:products].found
    # @note Emits "search_engine.multi_search" via ActiveSupport::Notifications with
    #   payload: { searches_count, labels, http_status, source: :multi }.
    def multi_search_result(common: {})
      raise ArgumentError, 'block required' unless block_given?

      builder = SearchEngine::Multi.new
      yield builder

      labels = builder.labels
      raw = execute_multi_search_internal(builder: builder, common: common, labels: labels, use_custom_client: false)

      list = Array(raw && (raw[:results] || raw['results']))
      SearchEngine::MultiResult.new(labels: labels, raw_results: list, klasses: builder.klasses)
    end

    # Execute a federated multi-search and return the raw response.
    #
    # This helper mirrors {.multi_search} but returns the raw Hash returned by
    # the underlying client. It enforces the configured limit and augments API
    # error messages with label context when possible.
    #
    # @param common [Hash] optional params merged into each per-search payload after relation compilation
    # @yieldparam m [SearchEngine::Multi] builder to add labeled relations
    # @return [Hash] Raw Typesense multi-search response
    # @raise [ArgumentError] when the number of searches exceeds the configured limit
    # @raise [SearchEngine::Errors::Api] when Typesense returns a non-2xx status
    # @note Emits "search_engine.multi_search" via ActiveSupport::Notifications with
    #   payload: { searches_count, labels, http_status, source: :multi }.
    def multi_search_raw(common: {})
      raise ArgumentError, 'block required' unless block_given?

      builder = SearchEngine::Multi.new
      yield builder

      labels = builder.labels
      execute_multi_search_internal(builder: builder, common: common, labels: labels, use_custom_client: true)
    rescue Errors::Api => error
      raise augment_multi_api_error(error, labels)
    end

    # Build and render a graph of Typesense collections and their interconnections.
    #
    # Discovers collections and field-level references from Typesense (with a
    # registry fallback) and returns a Hash with nodes, edges, cycles, isolated
    # nodes, and ready-to-print ASCII renderings. The diagram prefers Unicode
    # box-drawing characters and falls back to ASCII when requested.
    #
    # @param style [Symbol] :unicode (default) or :ascii
    # @param width [Integer, nil] maximum diagram width; auto-detected when nil
    # @param client [SearchEngine::Client, nil] optional client instance
    # @return [Hash] { nodes:, edges:, cycles:, isolated:, ascii:, ascii_compact:, stats: { ... } }
    # @example
    #   g = SearchEngine.collections_graph
    #   puts g[:ascii]
    def collections_graph(style: :unicode, width: nil, client: nil)
      ts_client = client || (SearchEngine.config.respond_to?(:client) && SearchEngine.config.client) || SearchEngine::Client.new
      SearchEngine::CollectionsGraph.build(client: ts_client, style: style, width: width)
    end

    private

    def config_mutex
      @config_mutex ||= Mutex.new
    end

    # Internal method that executes multi-search request with common setup and error handling.
    # Returns the raw Typesense response.
    #
    # @param builder [SearchEngine::Multi] configured builder instance
    # @param common [Hash] optional params merged into each per-search payload
    # @param labels [Array<Symbol>] ordered labels for the search list
    # @param use_custom_client [Boolean] whether to check config.client for custom client instance
    # @return [Hash] raw Typesense multi-search response
    # @raise [ArgumentError] when the number of searches exceeds the configured limit
    # @raise [SearchEngine::Errors::Api] when Typesense returns a non-2xx status
    def execute_multi_search_internal(builder:, common:, labels:, use_custom_client:)
      count = builder.labels.size
      limit = SearchEngine.config.multi_search_limit
      enforce_multi_limit!(count, limit)

      payloads = builder.to_payloads(common: common)
      url_opts = SearchEngine::ClientOptions.url_options_from_config(SearchEngine.config)

      client_obj = if use_custom_client
                     (SearchEngine.config.respond_to?(:client) && SearchEngine.config.client) || SearchEngine::Client.new
                   else
                     SearchEngine::Client.new
                   end

      if defined?(ActiveSupport::Notifications)
        se_payload = build_multi_event_payload(count, labels, url_opts)
        begin
          SearchEngine::Instrumentation.instrument('search_engine.multi_search', se_payload) do |ctx|
            client_obj.multi_search(searches: payloads, url_opts: url_opts).tap do
              ctx[:http_status] = 200
            end
          rescue Errors::Api => error
            ctx[:http_status] = error.status
            raise
          end
        rescue Errors::Api => error
          raise augment_multi_api_error(error, labels)
        end
      else
        begin
          client_obj.multi_search(searches: payloads, url_opts: url_opts)
        rescue Errors::Api => error
          raise augment_multi_api_error(error, labels)
        end
      end
    end

    def enforce_multi_limit!(count, limit)
      return unless count > limit

      raise ArgumentError,
            "multi_search: #{count} searches exceed limit (#{limit}). " \
            'Increase `SearchEngine.config.multi_search_limit` if necessary.'
    end

    def build_multi_event_payload(count, labels, url_opts)
      {
        searches_count: count,
        labels: labels.map(&:to_s),
        http_status: nil,
        source: :multi,
        url_opts: Observability.filtered_url_opts(url_opts)
      }
    end

    def build_label_result_pairs(list, labels, builder)
      pairs = []
      list.each_with_index do |item, idx|
        label = labels[idx]
        klass = builder.klasses[idx]
        result = SearchEngine::Result.new(item, klass: klass)
        pairs << [label, result]
      end
      pairs
    end

    # Build an API error with additional label context when possible.
    # @param error [SearchEngine::Errors::Api]
    # @param labels [Array<Symbol>] ordered labels for the search list
    # @return [SearchEngine::Errors::Api]
    def augment_multi_api_error(error, labels)
      body = error.body
      failing_index = nil
      failing_status = nil

      if body.is_a?(Hash)
        results = body['results'] || body[:results]
        if results.is_a?(Array)
          results.each_with_index do |item, idx|
            status = item.is_a?(Hash) ? (item['status'] || item[:status] || item['code'] || item[:code]) : nil
            next if status.nil? || status.to_i == 200

            failing_index = idx
            failing_status = status.to_i
            break
          end
        end
      end

      if failing_index && labels[failing_index]
        label = labels[failing_index]
        msg = "Multi-search failed for label :#{label} (status #{failing_status})."
        return Errors::Api.new(msg, status: error.status, body: error.body)
      end

      # Fallback: summarize
      codes = []
      if body.is_a?(Hash)
        results = body['results'] || body[:results]
        if results.is_a?(Array)
          results.each do |item|
            codes << (item['status'] || item[:status] || item['code'] || item[:code])
          end
        end
      end
      Errors::Api.new("Multi-search failed (statuses: #{codes.compact.join(', ')})", status: error.status,
body: error.body
      )
    end
  end
end
