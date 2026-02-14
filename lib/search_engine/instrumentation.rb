# frozen_string_literal: true

module SearchEngine
  # Lightweight wrapper over ActiveSupport::Notifications that standardizes
  # event names, payload shaping, and timing helpers. Provides a thread-local
  # context to propagate dispatch metadata (e.g., mode, job_id) from the
  # dispatcher/job into lower layers such as the indexer.
  #
  # Public API is intentionally small:
  # - {.instrument(event, payload = {}) { |ctx| }}: emit an event (yields mutable ctx)
  # - {.time(event, base_payload = {}) { |payload| }}: measure duration_ms
  # - {.with_context(hash) { }}: set per-thread context for nested calls
  # - {.context}: current shallow context Hash (dup)
  #
  # All payloads are duped, nil values are pruned, and keys are symbolized.
  module Instrumentation
    THREAD_KEY = :__search_engine_context__
    THREAD_CORRELATION_KEY = :__search_engine_correlation_id__

    CATALOG = {
      'search_engine.search' => {
        required: %i[collection],
        optional: %i[
          labels status duration_ms correlation_id params_preview http_status url_opts
          error_class error_message selection_include_count selection_exclude_count
          selection_nested_assoc_count preset_name preset_mode preset_pruned_keys
          preset_pruned_keys_count preset_locked_domains_count curation_pinned_count
          curation_hidden_count curation_has_override_tags curation_filter_flag
          curation_conflict_type curation_conflict_count
        ]
      },
      'search_engine.multi_search' => {
        required: %i[searches_count],
        optional: %i[labels status duration_ms correlation_id url_opts http_status]
      },
      'search_engine.compile' => {
        required: %i[collection klass node_count source],
        optional: %i[duration_ms]
      },
      'search_engine.grouping.compile' => {
        required: %i[field],
        optional: %i[collection limit missing_values duration_ms]
      },
      'search_engine.joins.compile' => {
        required: %i[collection],
        optional: %i[join_count assocs used_in include_len filter_len sort_len duration_ms has_joins]
      },
      'search_engine.selection.compile' => {
        required: %i[],
        optional: %i[include_count exclude_count nested_assoc_count]
      },
      'search_engine.preset.apply' => {
        required: %i[preset_name],
        optional: %i[preset_mode preset_pruned_keys preset_pruned_keys_count preset_locked_domains_count]
      },
      'search_engine.preset.conflict' => {
        required: %i[type count],
        optional: %i[limit]
      },
      'search_engine.curation.compile' => {
        required: %i[],
        optional: %i[pinned_count hidden_count has_override_tags filter_curated_hits]
      },
      'search_engine.curation.conflict' => {
        required: %i[type count],
        optional: %i[limit]
      },
      'search_engine.schema.diff' => {
        required: %i[collection],
        optional: %i[fields_changed_count added_count removed_count in_sync duration_ms]
      },
      'search_engine.schema.apply' => {
        required: %i[collection],
        optional: %i[physical_new new_physical alias_swapped dropped_count retention_deleted_count status duration_ms]
      },
      'search_engine.indexer.partition_start' => {
        required: %i[collection into partition],
        optional: %i[dispatch_mode job_id timestamp]
      },
      'search_engine.indexer.partition_finish' => {
        required: %i[collection into partition batches_total docs_total success_total failed_total status],
        optional: %i[duration_ms]
      },
      'search_engine.indexer.batch_import' => {
        required: %i[collection into batch_index docs_count],
        optional: %i[
          success_count failure_count attempts http_status bytes_sent duration_ms
          transient_retry error_sample
        ]
      },
      'search_engine.indexer.delete_stale' => {
        required: %i[collection into partition filter_hash status],
        optional: %i[deleted_count duration_ms reason]
      },
      'search_engine.result.grouped_parsed' => { required: %i[collection groups_count], optional: %i[] },
      'search_engine.joins.declared' => { required: %i[model name collection], optional: %i[] },
      'search_engine.relation.group_by_updated' => {
        required: %i[collection field],
        optional: %i[limit missing_values]
      },
      'search_engine.facet.compile' => {
        required: %i[],
        optional: %i[collection fields_count queries_count max_facet_values sort_flags conflicts duration_ms]
      },
      'search_engine.highlight.compile' => {
        required: %i[],
        optional: %i[collection fields_count full_fields_count affix_tokens snippet_threshold tag_kind duration_ms]
      },
      'search_engine.synonyms.apply' => {
        required: %i[],
        optional: %i[collection use_synonyms use_stopwords source duration_ms]
      },
      'search_engine.geo.compile' => {
        required: %i[],
        optional: %i[collection filters_count shapes sort_mode radius_bucket duration_ms]
      },
      'search_engine.vector.compile' => {
        required: %i[],
        optional: %i[collection query_vector_present dims hybrid_weight ann_params_present duration_ms]
      },
      'search_engine.hits.limit' => {
        required: %i[],
        optional: %i[collection early_limit validate_max applied_strategy triggered total_hits duration_ms]
      }
    }.freeze

    def self.catalog
      CATALOG
    end

    # Emit an event with a shaped payload and yield a mutable context.
    # Stamps correlation_id, status, error_class/error_message, and duration_ms.
    # @param event [String]
    # @param payload [Hash]
    # @yieldparam ctx [Hash]
    # @return [Object] block result when provided
    def self.instrument(event, payload = {})
      started = monotonic_ms
      ctx = shape_payload(payload)
      ctx[:correlation_id] ||= (Thread.current[THREAD_CORRELATION_KEY] ||= generate_correlation_id)

      if defined?(ActiveSupport::Notifications)
        result = nil
        ActiveSupport::Notifications.instrument(event, ctx) do
          result = yield(ctx) if block_given?
          ctx[:status] = :ok unless ctx.key?(:status)
          result
        rescue StandardError => error
          fill_error_context!(ctx, error)
          raise
        ensure
          ctx[:duration_ms] = (monotonic_ms - started).round(1)
        end
        return result
      end

      # Fallback path when AS::N is unavailable
      begin
        result = yield(ctx) if block_given?
        ctx[:status] = :ok unless ctx.key?(:status)
        result
      rescue StandardError => error
        fill_error_context!(ctx, error)
        raise
      ensure
        ctx[:duration_ms] = (monotonic_ms - started).round(1)
      end
    end

    # Known events
    #
    # - "search_engine.joins.compile": emitted once per relation compile summarizing JOIN usage.
    #   Payload keys (nil/empty omitted):
    #   - :collection [String]
    #   - :join_count [Integer]
    #   - :assocs [Array<String>]
    #   - :used_in [Hash{Symbol=>Array<String>}] include/filter/sort association usage
    #   - :include_len [Integer]
    #   - :filter_len [Integer]
    #   - :sort_len [Integer]
    #   - :duration_ms [Float]
    #   - :has_joins [Boolean]
    # - "search_engine.selection.compile": emitted once per relation compile summarizing field selection counts.
    #   Payload keys:
    #   - :include_count [Integer] total effective include fields (root + nested after precedence)
    #   - :exclude_count [Integer] total excluded fields (root + nested)
    #   - :nested_assoc_count [Integer] associations with any selection state (include or exclude)
    # - "search_engine.preset.apply": emitted once per relation compile when a preset is present.
    #   Payload keys (keys-only; values redacted elsewhere):
    #   - :preset_name [String] effective namespaced preset
    #   - :mode [Symbol] one of :merge, :only, :lock
    #   - :locked_domains [Array<Symbol>] configured locked domains for :lock mode
    #   - :pruned_keys [Array<Symbol>] keys removed by the chosen mode
    # - "search_engine.curation.compile": emitted once per relation compile when curation state is present.
    #   Payload keys:
    #   - :pinned_count [Integer]
    #   - :hidden_count [Integer]
    #   - :has_override_tags [Boolean]
    #   - :filter_curated_hits [true,false,nil]
    # - "search_engine.curation.conflict": emitted when overlaps or limits are detected; at most once per compile.
    #   Payload keys:
    #   - :type [Symbol] one of :overlap, :limit_exceeded
    #   - :count [Integer]
    #   - :limit [Integer, optional] present when type==:limit_exceeded
    #   # See also: https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/presets#observability
    # Measure a block and attach duration_ms to payload.
    # @param event [String]
    # @param base_payload [Hash]
    # @yieldreturn [Object] result of the block
    # @return [Object]
    def self.time(event, base_payload = {})
      started = monotonic_ms
      result = nil
      instrument(event, base_payload.merge(started_at_ms: started)) do
        result = yield(base_payload)
      end
      result

      # NOTE: ActiveSupport::Notifications already attaches duration; callers
      # should prefer event.duration on the subscriber side. We still provide
      # started_at_ms in the payload for completeness.
    end

    # Set a shallow thread-local context for nested calls.
    # @param ctx [Hash]
    # @yield block executed with context applied
    # @return [Object] result of the block
    def self.with_context(ctx)
      prev = Thread.current[THREAD_KEY]
      Thread.current[THREAD_KEY] = (prev || {}).merge(ctx || {})
      yield
    ensure
      Thread.current[THREAD_KEY] = prev
    end

    # Current shallow context hash.
    # @return [Hash]
    def self.context
      (Thread.current[THREAD_KEY] || {}).dup
    end

    # Apply/propagate correlation id for the current execution (thread/fiber-local)
    # and restore the previous value afterwards.
    # @param id [String, nil]
    # @yieldreturn [Object]
    def self.with_correlation_id(id = nil)
      previous = Thread.current[THREAD_CORRELATION_KEY]
      Thread.current[THREAD_CORRELATION_KEY] = id || previous || generate_correlation_id
      yield
    ensure
      Thread.current[THREAD_CORRELATION_KEY] = if previous.nil?
                                                 nil
                                               else
                                                 previous
                                               end
    end

    # @return [String, nil]
    def self.current_correlation_id
      Thread.current[THREAD_CORRELATION_KEY]
    end

    # Redact a payload-like structure (delegates to Observability)
    def self.redact(value)
      SearchEngine::Observability.redact(value)
    end

    # Monotonic clock in milliseconds.
    # @return [Float]
    def self.monotonic_ms
      Process.clock_gettime(Process::CLOCK_MONOTONIC, :float_millisecond)
    rescue StandardError
      (Time.now.to_f * 1000.0)
    end

    # Internal: normalize and prune a payload hash.
    def self.shape_payload(payload)
      shaped = {}
      (payload || {}).each do |k, v|
        next if v.nil?

        shaped[k.to_sym] = v
      end
      # Attach current context values without overriding explicit keys
      ctx = context
      ctx.each do |k, v|
        shaped[k] = v unless shaped.key?(k)
      end
      shaped
    end
    private_class_method :shape_payload

    def self.generate_correlation_id
      require 'securerandom'
      SecureRandom.urlsafe_base64(8)
    end
    private_class_method :generate_correlation_id

    def self.fill_error_context!(ctx, error)
      ctx[:status] = :error unless ctx.key?(:status)
      ctx[:error_class] ||= error.class.name
      ctx[:error_message] ||= SearchEngine::Observability.truncate_message(
        error.message,
        SearchEngine.config.observability&.max_message_length || 200
      )
    end
  end
end
