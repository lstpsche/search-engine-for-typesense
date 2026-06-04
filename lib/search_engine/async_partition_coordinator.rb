# frozen_string_literal: true

module SearchEngine
  # Coordinates ActiveJob-backed partition indexing and waits for run completion.
  module AsyncPartitionCoordinator
    module_function

    # Enqueue one partition job per partition and return a lifecycle-compatible aggregate.
    # @param klass [Class] SearchEngine collection class
    # @param partitions [Array<Object>] logical partition values
    # @param into [String] target physical collection name
    # @param queue [String, nil] optional ActiveJob queue override
    # @param timeout_s [Numeric, nil] maximum wait time in seconds
    # @param poll_interval_s [Numeric, nil] polling interval in seconds
    # @param store [Object, nil] optional run store
    # @param ttl_s [Numeric, nil] run metadata TTL in seconds
    # @return [Hash] lifecycle result with :status, :docs_total, :success_total, :failed_total, :sample_error
    def call(klass:, partitions:, into:, queue: nil, timeout_s: nil, poll_interval_s: nil, store: nil, ttl_s: nil)
      cfg = SearchEngine.config.indexer
      partition_list = Array(partitions)
      run_id = SearchEngine::IndexingRun.generate_id
      queue_name = resolve_queue_name(queue, cfg)
      run_store = SearchEngine::IndexingRunStore.validate!(store || SearchEngine::IndexingRunStore.resolve)
      ttl = ttl_s || cfg.partition_run_ttl_s

      snapshot = create_run(
        store: run_store,
        run_id: run_id,
        klass: klass,
        into: into,
        partitions: partition_list,
        ttl_s: ttl
      )
      instrument('search_engine.indexing_run.started', event_payload(snapshot, queue: queue_name))
      enqueue_partitions(klass, partition_list, into: into, queue_name: queue_name, run_id: run_id)
      instrument('search_engine.indexing_run.enqueued', event_payload(snapshot, queue: queue_name))

      wait_for_completion(
        store: run_store,
        run_id: run_id,
        timeout_s: timeout_s.nil? ? cfg.partition_timeout_s : timeout_s,
        poll_interval_s: poll_interval_s.nil? ? cfg.partition_poll_interval_s : poll_interval_s
      )
    end

    def create_run(store:, run_id:, klass:, into:, partitions:, ttl_s:)
      store.create_run(
        run_id: run_id,
        collection: collection_name(klass),
        collection_class_name: klass.name,
        into: into,
        partitions: partitions,
        ttl_s: ttl_s
      )
    end
    private_class_method :create_run

    def enqueue_partitions(klass, partitions, into:, queue_name:, run_id:)
      partitions.each do |partition|
        SearchEngine::IndexPartitionJob
          .set(queue: queue_name)
          .perform_later(
            klass.name,
            partition,
            into: into,
            metadata: {},
            run_id: run_id,
            partition_key: SearchEngine::IndexingRun.partition_key(partition)
          )
      end
    end
    private_class_method :enqueue_partitions

    def wait_for_completion(store:, run_id:, timeout_s:, poll_interval_s:)
      deadline = timeout_s.nil? ? nil : monotonic_seconds + timeout_s.to_f

      loop do
        snapshot = store.snapshot(run_id: run_id)
        result = result_for_snapshot(snapshot)
        return finish_failure(snapshot, result) if invalid_snapshot_result?(result)
        return finish_success(snapshot, result) if result[:status] == :ok
        return finish_failure(snapshot, result) if failure_terminal?(snapshot)
        return timeout_result(store, run_id, snapshot) if deadline && monotonic_seconds >= deadline

        sleep([poll_interval_s.to_f, 0].max)
      end
    end
    private_class_method :wait_for_completion

    def result_for_snapshot(snapshot)
      SearchEngine::IndexingRun.aggregate_result(snapshot)
    end
    private_class_method :result_for_snapshot

    def invalid_snapshot_result?(result)
      result[:status] == :failed && result[:sample_error].to_s.start_with?('async partition indexing run snapshot')
    end
    private_class_method :invalid_snapshot_result?

    def finish_success(snapshot, result)
      instrument('search_engine.indexing_run.finished', event_payload(snapshot, result: result))
      result
    end
    private_class_method :finish_success

    def finish_failure(snapshot, result)
      result = failed_result(snapshot, result)
      instrument('search_engine.indexing_run.failed', event_payload(snapshot, result: result))
      result
    end
    private_class_method :finish_failure

    def timeout_result(store, run_id, snapshot)
      mark_non_terminal_failed(store, run_id, snapshot)
      snapshot = store.snapshot(run_id: run_id) || snapshot
      result = failed_result(
        snapshot,
        SearchEngine::IndexingRun.aggregate_result(snapshot),
        sample_error: "SearchEngine async partition indexing timed out for run #{run_id}"
      )
      instrument('search_engine.indexing_run.failed', event_payload(snapshot, result: result))
      result
    end
    private_class_method :timeout_result

    def mark_non_terminal_failed(store, run_id, snapshot)
      partitions = snapshot && snapshot[:partitions]
      return unless partitions.is_a?(Hash)

      partitions.each do |partition_key, entry|
        status = entry[:status].to_s
        next if %w[succeeded failed].include?(status)

        store.mark_failed(
          run_id: run_id,
          partition_key: partition_key,
          error: 'partition did not finish before timeout'
        )
      end
    end
    private_class_method :mark_non_terminal_failed

    def failed_result(snapshot, result, sample_error: nil)
      statuses = partition_statuses(snapshot)
      success_total = result[:success_total].to_i
      status = success_total.positive? || statuses.include?('succeeded') ? :partial : :failed

      result.merge(
        status: status,
        failed_total: [result[:failed_total].to_i, failed_partition_count(statuses)].max,
        sample_error: sample_error || result[:sample_error] || 'async partition indexing failed'
      )
    end
    private_class_method :failed_result

    def failure_terminal?(snapshot)
      partition_statuses(snapshot).include?('failed')
    end
    private_class_method :failure_terminal?

    def partition_statuses(snapshot)
      partitions = snapshot && snapshot[:partitions]
      return [] unless partitions.is_a?(Hash)

      partitions.values.map { |entry| entry[:status].to_s }
    end
    private_class_method :partition_statuses

    def failed_partition_count(statuses)
      statuses.count { |status| status == 'failed' }
    end
    private_class_method :failed_partition_count

    def resolve_queue_name(queue, cfg)
      (queue || cfg.partition_queue_name || cfg.queue_name || 'search_index').to_s
    end
    private_class_method :resolve_queue_name

    def collection_name(klass)
      klass.respond_to?(:collection) ? klass.collection.to_s : klass.name.to_s
    end
    private_class_method :collection_name

    def event_payload(snapshot, queue: nil, result: nil)
      payload = {
        run_id: snapshot && snapshot[:run_id],
        collection: snapshot && snapshot[:collection],
        collection_class_name: snapshot && snapshot[:collection_class_name],
        into: snapshot && snapshot[:into],
        total_partitions: snapshot && snapshot[:total_partitions],
        queue: queue
      }
      payload.merge!(result) if result
      payload
    end
    private_class_method :event_payload

    def instrument(event, payload)
      SearchEngine::Instrumentation.instrument(event, payload) {}
    end
    private_class_method :instrument

    def monotonic_seconds
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
    private_class_method :monotonic_seconds
  end
end
