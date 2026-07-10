# frozen_string_literal: true

module SearchEngine
  # ActiveJob to rebuild a single partition using the same orchestration as inline.
  #
  # Arguments:
  # - collection_class_name [String]
  # - partition [Object] (JSON-serializable)
  # - into [String, nil]
  # - metadata [Hash]
  # - run_id [String, nil]
  # - partition_key [String, nil]
  class IndexPartitionJob < ::ActiveJob::Base
    queue_as do
      cfg = SearchEngine.config.indexer
      (cfg&.queue_name || 'search_index').to_s
    end

    # Handle transient errors with exponential backoff based on Indexer config.
    rescue_from(SearchEngine::Errors::Timeout) { |error| retry_if_possible(error) }
    rescue_from(SearchEngine::Errors::Connection) { |error| retry_if_possible(error) }
    rescue_from(SearchEngine::Errors::PartitionImportFailed) { |error| retry_if_possible(error) }
    rescue_from(SearchEngine::Errors::Api) do |error|
      if SearchEngine::Indexer::RetryPolicy.transient_status?(error.status.to_i)
        retry_if_possible(error)
      else
        instrument_error(error)
        raise
      end
    end

    # Perform a single-partition rebuild.
    # @param collection_class_name [String]
    # @param partition [Object]
    # @param into [String, nil]
    # @param metadata [Hash]
    # @param run_id [String, nil]
    # @param partition_key [String, nil]
    # @return [void]
    def perform(collection_class_name, partition, into: nil, metadata: {}, run_id: nil, partition_key: nil)
      payload = nil
      run_store = indexing_run_store(run_id)
      partition_key ||= SearchEngine::IndexingRun.partition_key(partition) if run_id
      klass = constantize_collection!(collection_class_name)
      payload = base_payload(klass, partition: partition, into: into)
      run_store&.mark_started(run_id: run_id, partition_key: partition_key, job_id: job_id)
      instrument_event('search_engine.dispatcher.job_started',
                       payload.merge(queue: queue_name, job_id: job_id, metadata: metadata)
                      )

      started = monotonic_ms
      summary = nil
      SearchEngine::Instrumentation.with_context(dispatch_mode: :active_job, job_id: job_id) do
        summary = SearchEngine::Indexer.rebuild_partition!(klass, partition: partition, into: into)
      end
      raise_for_failed_summary!(summary, run_store, run_id, partition_key)
      duration = (monotonic_ms - started).round(1)
      run_store&.mark_succeeded(run_id: run_id, partition_key: partition_key, summary: summary)

      instrument_event(
        'search_engine.dispatcher.job_finished',
        payload.merge(queue: queue_name, job_id: job_id, duration_ms: duration, status: summary.status,
                      metadata: metadata
        )
      )
      nil
    rescue SearchEngine::IndexingRunStore::StaleRun => error
      safe_payload = payload || error_payload(error)
      instrument_stale_run(
        error,
        payload: safe_payload.merge(metadata: metadata || {}),
        run_id: run_id,
        partition_key: partition_key
      )
      nil
    rescue StandardError => error
      safe_payload = payload || error_payload(error)
      safe_payload = safe_payload.merge(metadata: metadata || {})
      if retryable_error?(error)
        instrument_error(error, payload: safe_payload)
        raise
      end

      if error.is_a?(SearchEngine::Errors::PartitionImportFailed)
        mark_failed_for_run(run_store, run_id, partition_key, error, payload: safe_payload) if run_id && run_store
        instrument_error(error, payload: safe_payload)
        raise
      end

      if run_id && run_store
        mark_failed_for_run(run_store, run_id, partition_key, error, payload: safe_payload)
        instrument_error(error, payload: safe_payload)
        return nil
      end

      instrument_error(error, payload: safe_payload)
      raise
    end

    private

    def base_payload(klass, partition:, into:)
      {
        collection: (klass.respond_to?(:collection) ? klass.collection.to_s : klass.name.to_s),
        partition: partition,
        into: into
      }
    end

    def constantize_collection!(name)
      raise ArgumentError, 'collection_class_name must be a String' unless name.is_a?(String)

      klass = name.constantize
      unless klass.is_a?(Class) && klass.ancestors.include?(SearchEngine::Base)
        raise ArgumentError, 'collection_class_name must be a SearchEngine::Base subclass'
      end

      klass
    rescue NameError => error
      raise ArgumentError, "unknown collection class: #{name}", error.backtrace
    end

    def retry_if_possible(error)
      policy = retry_policy
      attempt_no = executions.to_i
      if attempt_no >= policy.attempts
        instrument_error(error)
        raise
      end

      wait_seconds = policy.next_delay(attempt_no + 1, error)
      instrument_event(
        'search_engine.dispatcher.job_error',
        error_payload(error).merge(queue: queue_name, job_id: job_id, retry_after_s: wait_seconds)
      )
      retry_job wait: wait_seconds
    end

    def indexing_run_store(run_id)
      return nil if run_id.nil?

      SearchEngine::IndexingRunStore.resolve
    end

    def retryable_error?(error)
      (transient_error?(error) || error.is_a?(SearchEngine::Errors::PartitionImportFailed)) &&
        executions.to_i < retry_policy.attempts
    end

    def transient_error?(error)
      return true if error.is_a?(SearchEngine::Errors::Timeout)
      return true if error.is_a?(SearchEngine::Errors::Connection)

      error.is_a?(SearchEngine::Errors::Api) &&
        SearchEngine::Indexer::RetryPolicy.transient_status?(error.status.to_i)
    end

    def summary_failed?(summary)
      value = if summary.respond_to?(:failed_total)
                summary.failed_total
              elsif summary.is_a?(Hash)
                summary[:failed_total] || summary['failed_total']
              end
      value.to_i.positive?
    end

    def raise_for_failed_summary!(summary, run_store, run_id, partition_key)
      return unless summary_failed?(summary)

      error = SearchEngine::Errors::PartitionImportFailed.new(summary)
      run_store&.record_attempt(
        run_id: run_id,
        partition_key: partition_key,
        summary: summary,
        error: error
      )
      raise error
    end

    def error_payload(error)
      {
        collection: arguments_dig_collection,
        partition: arguments[1],
        into: begin
          arguments_hash[:into]
        rescue StandardError
          nil
        end,
        error_class: error.class.name,
        message_truncated: error.message.to_s[0, 200]
      }
    end

    def instrument_error(error, payload: nil)
      instrument_event(
        'search_engine.dispatcher.job_error',
        (payload || {}).merge(queue: queue_name, job_id: job_id, error_class: error.class.name,
                              message_truncated: error.message.to_s[0, 200]
        )
      )
    end

    def instrument_stale_run(error, payload:, run_id:, partition_key:)
      instrument_error(
        error,
        payload: payload.merge(discarded: true, run_id: run_id, partition_key: partition_key)
      )
    end

    def mark_failed_for_run(run_store, run_id, partition_key, error, payload:)
      run_store.mark_failed(run_id: run_id, partition_key: partition_key, error: error)
    rescue SearchEngine::IndexingRunStore::StaleRun => error
      instrument_stale_run(error, payload: payload, run_id: run_id, partition_key: partition_key)
    end

    def instrument_event(event, payload)
      SearchEngine::Instrumentation.instrument(event, payload) {}
    end

    def retry_policy
      cfg = SearchEngine.config.indexer
      SearchEngine::Indexer::RetryPolicy.from_config(cfg&.retries)
    end

    def monotonic_ms
      SearchEngine::Instrumentation.monotonic_ms
    end

    def arguments_hash
      # ActiveJob stores keyword args in the last Hash argument when using perform(class, partition, into:, metadata:)
      args = arguments
      args.last.is_a?(Hash) ? args.last.symbolize_keys : {}
    end

    def arguments_dig_collection
      begin
        name = arguments[0].to_s
      rescue StandardError
        name = nil
      end
      name || 'unknown'
    end
  end
end
