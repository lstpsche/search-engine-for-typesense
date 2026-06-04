# frozen_string_literal: true

require 'test_helper'
require 'active_job'
require_relative '../app/search_engine/search_engine/index_partition_job'

class AsyncPartitionCoordinatorTest < Minitest::Test
  Summary = Struct.new(:docs_total, :success_total, :failed_total, :sample_error, keyword_init: true)

  class FakeRunStore
    attr_reader :created_runs, :expired_runs

    def initialize
      @created_runs = []
      @expired_runs = []
      @runs = {}
    end

    def create_run(run_id:, collection:, collection_class_name:, into:, partitions:, ttl_s:)
      run = {
        run_id: run_id,
        collection: collection,
        collection_class_name: collection_class_name,
        into: into,
        status: 'running',
        total_partitions: partitions.size,
        partitions: partitions.each_with_object({}) do |partition, hash|
          hash[SearchEngine::IndexingRun.partition_key(partition)] =
            SearchEngine::IndexingRun.partition_entry(partition)
        end,
        ttl_s: ttl_s
      }
      @created_runs << run
      @runs[run_id] = run
    end

    def mark_started(run_id:, partition_key:, job_id: nil)
      update_partition(run_id, partition_key) do |entry|
        entry[:status] = 'running'
        entry[:job_id] = job_id
      end
    end

    def mark_succeeded(run_id:, partition_key:, summary:)
      update_partition(run_id, partition_key) do |entry|
        entry[:status] = 'succeeded'
        entry[:docs_total] = summary.docs_total
        entry[:success_total] = summary.success_total
        entry[:failed_total] = summary.failed_total
        entry[:sample_error] = summary.sample_error
      end
    end

    def mark_failed(run_id:, partition_key:, error:)
      update_partition(run_id, partition_key) do |entry|
        entry[:status] = 'failed'
        entry[:failed_total] = [entry[:failed_total].to_i, 1].max
        entry[:sample_error] = error.to_s
      end
    end

    def snapshot(run_id:)
      @runs[run_id]
    end

    def expire(run_id:)
      @expired_runs << run_id
      @runs.delete(run_id)
    end

    private

    def update_partition(run_id, partition_key)
      run = @runs.fetch(run_id)
      entry = run[:partitions].fetch(partition_key)
      yield entry
      statuses = run[:partitions].values.map { |partition| partition[:status] }
      run[:status] = if statuses.include?('failed')
                       'failed'
                     elsif statuses.all? { |status| status == 'succeeded' }
                       'succeeded'
                     else
                       'running'
                     end
      run
    end
  end

  class FakeJobRelation
    attr_reader :enqueued

    def initialize(store:, summary:, fail_first: false)
      @store = store
      @summary = summary
      @fail_first = fail_first
      @enqueued = []
    end

    def set(queue:)
      @queue = queue
      self
    end

    def perform_later(*args, **kwargs)
      @enqueued << { queue: @queue, args: args, kwargs: kwargs }
      run_id = kwargs.fetch(:run_id)
      partition_key = kwargs.fetch(:partition_key)
      if @fail_first && @enqueued.size == 1
        @store.mark_failed(run_id: run_id, partition_key: partition_key, error: 'partition failed')
      else
        @store.mark_succeeded(run_id: run_id, partition_key: partition_key, summary: @summary)
      end
    end
  end

  def test_call_creates_run_enqueues_each_partition_and_returns_success_aggregate
    store = FakeRunStore.new
    summary = Summary.new(docs_total: 3, success_total: 3, failed_total: 0, sample_error: nil)
    jobs = FakeJobRelation.new(store: store, summary: summary)
    partitions = [{ shard: :north }, { shard: :south }]

    SearchEngine::IndexingRun.stub(:generate_id, 'run-1') do
      SearchEngine::IndexPartitionJob.stub(:set, ->(queue:) { jobs.set(queue: queue) }) do
        result = SearchEngine::AsyncPartitionCoordinator.call(
          klass: SearchEngine::Author,
          partitions: partitions,
          into: 'authors_20260604',
          queue: 'critical_search',
          timeout_s: 1,
          poll_interval_s: 0,
          store: store,
          ttl_s: 120
        )

        assert_equal(
          { status: :ok, docs_total: 6, success_total: 6, failed_total: 0, sample_error: nil },
          result
        )
      end
    end

    run = store.created_runs.first
    assert_equal 'run-1', run[:run_id]
    assert_equal 'authors', run[:collection]
    assert_equal 'SearchEngine::Author', run[:collection_class_name]
    assert_equal 'authors_20260604', run[:into]
    assert_equal 2, run[:total_partitions]
    assert_equal 120, run[:ttl_s]
    assert_empty store.expired_runs

    assert_equal 2, jobs.enqueued.size
    jobs.enqueued.each_with_index do |entry, index|
      assert_equal 'critical_search', entry[:queue]
      assert_equal(
        ['SearchEngine::Author', partitions[index]],
        entry[:args]
      )
      assert_equal 'authors_20260604', entry[:kwargs][:into]
      assert_equal 'run-1', entry[:kwargs][:run_id]
      assert_equal SearchEngine::IndexingRun.partition_key(partitions[index]), entry[:kwargs][:partition_key]
    end
  end

  def test_call_uses_partition_queue_config_fallback
    store = FakeRunStore.new
    summary = Summary.new(docs_total: 1, success_total: 1, failed_total: 0, sample_error: nil)
    jobs = FakeJobRelation.new(store: store, summary: summary)
    previous_queue = SearchEngine.config.indexer.partition_queue_name
    SearchEngine.config.indexer.partition_queue_name = 'configured_partition_queue'

    SearchEngine::IndexingRun.stub(:generate_id, 'run-queue') do
      SearchEngine::IndexPartitionJob.stub(:set, ->(queue:) { jobs.set(queue: queue) }) do
        SearchEngine::AsyncPartitionCoordinator.call(
          klass: SearchEngine::Author,
          partitions: [1],
          into: 'authors_20260604',
          poll_interval_s: 0,
          store: store
        )
      end
    end

    assert_equal 'configured_partition_queue', jobs.enqueued.first[:queue]
  ensure
    SearchEngine.config.indexer.partition_queue_name = previous_queue
  end

  def test_call_returns_failure_when_any_partition_fails
    store = FakeRunStore.new
    summary = Summary.new(docs_total: 2, success_total: 2, failed_total: 0, sample_error: nil)
    jobs = FakeJobRelation.new(store: store, summary: summary, fail_first: true)

    SearchEngine::IndexingRun.stub(:generate_id, 'run-failed') do
      SearchEngine::IndexPartitionJob.stub(:set, ->(queue:) { jobs.set(queue: queue) }) do
        result = SearchEngine::AsyncPartitionCoordinator.call(
          klass: SearchEngine::Author,
          partitions: [1, 2],
          into: 'authors_20260604',
          timeout_s: 1,
          poll_interval_s: 0,
          store: store
        )

        assert_equal :partial, result[:status]
        assert_equal 2, result[:success_total]
        assert_equal 1, result[:failed_total]
        assert_match(/partition failed/, result[:sample_error])
      end
    end
  end

  def test_call_returns_failed_result_on_timeout_and_marks_pending_partitions_failed
    store = FakeRunStore.new
    jobs = Object.new
    enqueued = []
    jobs.define_singleton_method(:set) do |queue:|
      @queue = queue
      self
    end
    jobs.define_singleton_method(:perform_later) { |*args, **kwargs| enqueued << [args, kwargs] }

    SearchEngine::IndexingRun.stub(:generate_id, 'run-timeout') do
      SearchEngine::IndexPartitionJob.stub(:set, ->(queue:) { jobs.set(queue: queue) }) do
        result = SearchEngine::AsyncPartitionCoordinator.call(
          klass: SearchEngine::Author,
          partitions: [1, 2],
          into: 'authors_20260604',
          timeout_s: 0,
          poll_interval_s: 0,
          store: store
        )

        assert_equal :failed, result[:status]
        assert_equal 2, result[:failed_total]
        assert_match(/timed out/, result[:sample_error])
      end
    end

    statuses = store.snapshot(run_id: 'run-timeout')[:partitions].values.map { |entry| entry[:status] }
    assert_equal %w[failed failed], statuses
    assert_equal 2, enqueued.size
  end

  def test_call_returns_failed_result_when_run_snapshot_disappears
    store = FakeRunStore.new
    jobs = Object.new
    jobs.define_singleton_method(:set) { |**_kwargs| self }
    jobs.define_singleton_method(:perform_later) do |*_args, **kwargs|
      store.expire(run_id: kwargs.fetch(:run_id))
    end

    SearchEngine::IndexingRun.stub(:generate_id, 'run-evicted') do
      SearchEngine::IndexPartitionJob.stub(:set, ->(queue:) { jobs.set(queue: queue) }) do
        result = SearchEngine::AsyncPartitionCoordinator.call(
          klass: SearchEngine::Author,
          partitions: [1, 2],
          into: 'authors_20260604',
          timeout_s: 1,
          poll_interval_s: 0,
          store: store
        )

        assert_equal :failed, result[:status]
        assert_equal 1, result[:failed_total]
        assert_match(/snapshot is missing/, result[:sample_error])
      end
    end
  end

  def test_call_passes_original_partition_payload_to_job
    store = FakeRunStore.new
    summary = Summary.new(docs_total: 1, success_total: 1, failed_total: 0, sample_error: nil)
    jobs = FakeJobRelation.new(store: store, summary: summary)
    partition = { region: :north, nested: { store_id: 1 } }

    SearchEngine::IndexingRun.stub(:generate_id, 'run-original-payload') do
      SearchEngine::IndexPartitionJob.stub(:set, ->(queue:) { jobs.set(queue: queue) }) do
        SearchEngine::AsyncPartitionCoordinator.call(
          klass: SearchEngine::Author,
          partitions: [partition],
          into: 'authors_20260604',
          timeout_s: 1,
          poll_interval_s: 0,
          store: store
        )
      end
    end

    assert_equal ['SearchEngine::Author', partition], jobs.enqueued.first[:args]
    assert_equal SearchEngine::IndexingRun.partition_key(partition), jobs.enqueued.first[:kwargs][:partition_key]
  end
end
