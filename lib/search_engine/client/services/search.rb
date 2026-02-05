# frozen_string_literal: true

module SearchEngine
  class Client
    module Services
      # Handles single and multi search workflows while keeping observability hooks intact.
      class Search < Base
        # Execute a single search request.
        # @param collection [String]
        # @param params [Hash]
        # @param url_opts [Hash]
        # @return [SearchEngine::Result]
        def call(collection:, params:, url_opts: {})
          params_obj = SearchEngine::CompiledParams.from(params)
          validate_single!(collection, params_obj.to_h)

          cache_params = derive_cache_opts(url_opts)
          start = current_monotonic_ms
          payload = sanitize_body_params(params_obj.to_h)
          path = [Client::RequestBuilder::COLLECTIONS_PREFIX, collection.to_s, Client::RequestBuilder::DOCUMENTS_SEARCH_SUFFIX].join

          result = instrumented_search(collection, params_obj, cache_params, path, payload, start)
          duration = current_monotonic_ms - start
          instrument(:post, path, duration, cache_params)
          log_success(:post, path, start, cache_params)

          klass = begin
            SearchEngine.collection_for(collection)
          rescue ArgumentError
            nil
          end
          SearchEngine::Result.new(result, klass: klass)
        end

        # Execute a multi search request.
        # @param searches [Array<Hash>]
        # @param url_opts [Hash]
        # @return [Hash]
        def multi(searches:, url_opts: {})
          validate_multi!(searches)

          cache_params = derive_cache_opts(url_opts)
          start = current_monotonic_ms
          path = '/multi_search'
          bodies = Array(searches).map { |s| sanitize_body_params(s) }

          result = with_exception_mapping(:post, path, cache_params, start) do
            typesense.multi_search.perform({ searches: bodies }, common_params: cache_params)
          end

          instrument(:post, path, current_monotonic_ms - start, cache_params)
          symbolize_keys_deep(result)
        end

        private

        def instrumented_search(collection, params_obj, cache_params, path, payload, start)
          if defined?(ActiveSupport::Notifications)
            se_payload = build_search_event_payload(
              collection: collection,
              params: params_obj.to_h,
              cache_params: cache_params
            )
            result = nil
            SearchEngine::Instrumentation.instrument('search_engine.search', se_payload) do |ctx|
              ctx[:params_preview] = SearchEngine::Instrumentation.redact(params_obj.to_h)
              result = with_exception_mapping(:post, path, cache_params, start) do
                docs = typesense.collections[collection].documents
                documents_search(docs, payload, cache_params)
              end
              ctx[:status] = :ok
            rescue Errors::Api => error
              ctx[:status] = error.status
              ctx[:error_class] = error.class.name
              raise
            rescue Errors::Error => error
              ctx[:status] = :error
              ctx[:error_class] = error.class.name
              raise
            end
            result
          else
            with_exception_mapping(:post, path, cache_params, start) do
              docs = typesense.collections[collection].documents
              documents_search(docs, payload, cache_params)
            end
          end
        end

        def sanitize_body_params(params)
          Client::RequestBuilder.send(:sanitize_body_params, params)
        end

        def build_search_event_payload(collection:, params:, cache_params: {})
          sel = params[:_selection].is_a?(Hash) ? params[:_selection] : {}
          base = {
            collection: collection,
            params: Observability.redact(params),
            url_opts: Observability.filtered_url_opts(cache_params),
            status: nil,
            error_class: nil,
            retries: nil,
            selection_include_count: sel[:include_count],
            selection_exclude_count: sel[:exclude_count],
            selection_nested_assoc_count: sel[:nested_assoc_count]
          }
          base.merge(build_preset_segment(params)).merge(build_curation_segment(params))
        end

        def build_preset_segment(params)
          preset_mode = params[:_preset_mode]
          pruned = Array(params[:_preset_pruned_keys]).map { |k| k.respond_to?(:to_sym) ? k.to_sym : k }.grep(Symbol)
          locked_count = begin
            config.presets.locked_domains.size
          rescue StandardError
            nil
          end
          {
            preset_name: params[:preset],
            preset_mode: preset_mode,
            preset_pruned_keys_count: pruned.empty? ? nil : pruned.size,
            preset_locked_domains_count: locked_count,
            preset_pruned_keys: pruned.empty? ? nil : pruned
          }
        end

        def build_curation_segment(params)
          curation = params[:_curation].is_a?(Hash) ? params[:_curation] : {}

          pinned = params[:pinned_hits]
          hidden = params[:hidden_hits]
          tags   = params[:curation_tags] || params[:override_tags]

          pinned_count =
            case pinned
            when String then pinned.split(',').count
            when Array  then pinned.size
            else 0
            end

          hidden_count =
            case hidden
            when String then hidden.split(',').count
            when Array  then hidden.size
            else 0
            end

          has_tags =
            case tags
            when String then !tags.to_s.strip.empty?
            when Array  then !tags.empty?
            else false
            end

          out = {
            curation_pinned_count: pinned_count,
            curation_hidden_count: hidden_count,
            curation_has_override_tags: has_tags
          }

          if params.key?(:filter_curated_hits) || curation.key?(:filter_curated_hits)
            out[:curation_filter_flag] =
              params.key?(:filter_curated_hits) ? params[:filter_curated_hits] : curation[:filter_curated_hits]
          end

          out[:curation_conflict_type] = curation[:_conflict_type] || params[:_curation_conflict_type]
          out[:curation_conflict_count] = curation[:_conflict_count] || params[:_curation_conflict_count]

          out.compact
        end

        def documents_search(docs, payload, common_params)
          if documents_search_supports_common_params?(docs)
            docs.search(payload, common_params: common_params)
          else
            docs.search(payload)
          end
        end

        def documents_search_supports_common_params?(docs)
          m = docs.method(:search)
          params = m.parameters
          params.any? { |(kind, _)| %i[key keyreq keyrest].include?(kind) }
        rescue StandardError
          false
        end
      end
    end
  end
end
