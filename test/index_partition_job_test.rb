# frozen_string_literal: true

require 'test_helper'
require 'active_job'
require 'ostruct'
require_relative '../app/search_engine/search_engine/index_partition_job'

class IndexPartitionJobTest < Minitest::Test
  Summary = Struct.new(:status, :docs_total, :success_total, :failed_total, keyword_init: true)

  class FakeRunStore
    attr_reader :calls

    def initialize(stale_on: nil)
      @calls = []
      @stale_on = stale_on
    end

    def mark_started(run_id:, partition_key:, job_id: nil)
      raise_stale!(:started) if @stale_on == :started

      @calls << [:started, run_id, partition_key, job_id]
    end

    def mark_succeeded(run_id:, partition_key:, summary:)
      raise_stale!(:succeeded) if @stale_on == :succeeded

      @calls << [:succeeded, run_id, partition_key, summary]
    end

    def mark_failed(run_id:, partition_key:, error:)
      raise_stale!(:failed) if @stale_on == :failed

      @calls << [:failed, run_id, partition_key, error]
    end

    private

    def raise_stale!(transition)
      raise SearchEngine::IndexingRunStore::StaleRun, "stale #{transition}"
    end
  end

  def test_perform_preserves_original_error_when_constantize_fails
    job = SearchEngine::IndexPartitionJob.new

    events = SearchEngine::Test.capture_events('search_engine.dispatcher.job_error') do
      error = assert_raises(ArgumentError) do
        job.perform('DefinitelyMissingSearchEngineClass', { shard: 1 }, metadata: nil)
      end
      assert_match(/unknown collection class/i, error.message)
    end

    assert_equal 1, events.length
    payload = events.first[:payload]
    assert_equal 'ArgumentError', payload[:error_class]
    assert_equal({}, payload[:metadata])
    assert_match(/unknown collection class/i, payload[:message_truncated])
  end

  def test_perform_without_run_id_keeps_legacy_behavior
    job = SearchEngine::IndexPartitionJob.new
    summary = Summary.new(status: :ok, docs_total: 3, success_total: 3, failed_total: 0)

    SearchEngine::IndexingRunStore.stub(:resolve, -> { raise 'run store should not resolve' }) do
      SearchEngine::Indexer.stub(:rebuild_partition!, summary) do
        assert_nil job.perform('SearchEngine::Author', { shard: 1 }, into: 'authors_v2', metadata: { source: 'test' })
      end
    end
  end

  def test_perform_reports_started_and_succeeded_when_run_id_is_present
    job = SearchEngine::IndexPartitionJob.new
    store = FakeRunStore.new
    partition = { shard: 1 }
    partition_key = SearchEngine::IndexingRun.partition_key(partition)
    summary = Summary.new(status: :ok, docs_total: 5, success_total: 5, failed_total: 0)

    SearchEngine::IndexingRunStore.stub(:resolve, store) do
      SearchEngine::Indexer.stub(:rebuild_partition!, summary) do
        assert_nil job.perform('SearchEngine::Author', partition, into: 'authors_v2', run_id: 'run-1')
      end
    end

    assert_equal :started, store.calls[0][0]
    assert_equal 'run-1', store.calls[0][1]
    assert_equal partition_key, store.calls[0][2]
    assert_equal [:succeeded, 'run-1', partition_key, summary], store.calls[1]
  end

  def test_perform_uses_explicit_partition_key
    job = SearchEngine::IndexPartitionJob.new
    store = FakeRunStore.new
    summary = Summary.new(status: :ok, docs_total: 1, success_total: 1, failed_total: 0)

    SearchEngine::IndexingRunStore.stub(:resolve, store) do
      SearchEngine::Indexer.stub(:rebuild_partition!, summary) do
        job.perform('SearchEngine::Author', { shard: 1 }, run_id: 'run-1', partition_key: 'custom-key')
      end
    end

    assert_equal 'custom-key', store.calls[0][2]
    assert_equal 'custom-key', store.calls[1][2]
  end

  def test_perform_marks_failed_for_terminal_error_and_does_not_retry_coordinated_run
    job = SearchEngine::IndexPartitionJob.new
    store = FakeRunStore.new
    error = SearchEngine::Errors::InvalidParams.new('broken mapper')

    SearchEngine::IndexingRunStore.stub(:resolve, store) do
      SearchEngine::Indexer.stub(:rebuild_partition!, ->(*) { raise error }) do
        assert_nil job.perform('SearchEngine::Author', { shard: 1 }, run_id: 'run-1', partition_key: 'p1')
      end
    end

    assert_equal :failed, store.calls.last[0]
    assert_equal 'run-1', store.calls.last[1]
    assert_equal 'p1', store.calls.last[2]
    assert_same error, store.calls.last[3]
  end

  def test_perform_without_run_id_raises_terminal_error
    job = SearchEngine::IndexPartitionJob.new
    error = SearchEngine::Errors::InvalidParams.new('broken mapper')

    SearchEngine::Indexer.stub(:rebuild_partition!, ->(*) { raise error }) do
      raised = assert_raises(SearchEngine::Errors::InvalidParams) do
        job.perform('SearchEngine::Author', { shard: 1 })
      end
      assert_same error, raised
    end
  end

  def test_perform_does_not_mark_failed_for_retryable_error_before_attempts_are_exhausted
    job = SearchEngine::IndexPartitionJob.new
    store = FakeRunStore.new
    error = SearchEngine::Errors::Timeout.new('timeout')

    SearchEngine::IndexingRunStore.stub(:resolve, store) do
      SearchEngine::Indexer.stub(:rebuild_partition!, ->(*) { raise error }) do
        assert_raises(SearchEngine::Errors::Timeout) do
          job.perform('SearchEngine::Author', { shard: 1 }, run_id: 'run-1', partition_key: 'p1')
        end
      end
    end

    refute(store.calls.any? { |call| call.first == :failed })
  end

  def test_perform_marks_failed_for_retryable_error_after_attempts_are_exhausted_and_does_not_retry_coordinated_run
    job = SearchEngine::IndexPartitionJob.new
    job.define_singleton_method(:executions) { SearchEngine::Indexer::RetryPolicy.from_config(nil).attempts }
    store = FakeRunStore.new
    error = SearchEngine::Errors::Timeout.new('timeout')

    SearchEngine::IndexingRunStore.stub(:resolve, store) do
      SearchEngine::Indexer.stub(:rebuild_partition!, ->(*) { raise error }) do
        assert_nil job.perform('SearchEngine::Author', { shard: 1 }, run_id: 'run-1', partition_key: 'p1')
      end
    end

    assert_equal :failed, store.calls.last[0]
    assert_same error, store.calls.last[3]
  end

  def test_perform_discards_stale_run_without_rebuilding_or_retrying
    job = SearchEngine::IndexPartitionJob.new
    store = FakeRunStore.new(stale_on: :started)
    rebuilt = false

    events = SearchEngine::Test.capture_events('search_engine.dispatcher.job_error') do
      SearchEngine::IndexingRunStore.stub(:resolve, store) do
        SearchEngine::Indexer.stub(:rebuild_partition!, ->(*) { rebuilt = true }) do
          assert_nil job.perform('SearchEngine::Author', { shard: 1 }, run_id: 'run-stale', partition_key: 'p1')
        end
      end
    end

    refute rebuilt
    assert_empty store.calls
    assert_equal 1, events.length
    payload = events.first[:payload]
    assert_equal true, payload[:discarded]
    assert_equal 'run-stale', payload[:run_id]
    assert_equal 'p1', payload[:partition_key]
    assert_equal 'SearchEngine::IndexingRunStore::StaleRun', payload[:error_class]
  end

  def test_perform_discards_when_mark_failed_finds_stale_run
    job = SearchEngine::IndexPartitionJob.new
    store = FakeRunStore.new(stale_on: :failed)
    error = SearchEngine::Errors::InvalidParams.new('broken mapper')

    events = SearchEngine::Test.capture_events('search_engine.dispatcher.job_error') do
      SearchEngine::IndexingRunStore.stub(:resolve, store) do
        SearchEngine::Indexer.stub(:rebuild_partition!, ->(*) { raise error }) do
          assert_nil job.perform('SearchEngine::Author', { shard: 1 }, run_id: 'run-stale', partition_key: 'p1')
        end
      end
    end

    stale_payload = events.map { |event| event[:payload] }.find { |payload| payload[:discarded] }
    refute_nil stale_payload
    assert_equal 'run-stale', stale_payload[:run_id]
    assert_equal 'p1', stale_payload[:partition_key]
  end

  def test_dispatcher_active_job_remains_fire_and_forget_for_direct_callers
    enqueued = []
    relation = Object.new
    relation.define_singleton_method(:set) do |queue:|
      @queue = queue
      self
    end
    relation.define_singleton_method(:perform_later) do |*args, **kwargs|
      enqueued << { queue: @queue, args: args, kwargs: kwargs }
      OpenStruct.new(job_id: 'direct-job-1')
    end

    SearchEngine.config.indexer.dispatch = :active_job
    SearchEngine::IndexPartitionJob.stub(:set, ->(queue:) { relation.set(queue: queue) }) do
      result = SearchEngine::Dispatcher.dispatch!(
        SearchEngine::Author,
        partition: { shard: 1 },
        into: 'authors_v2',
        queue: 'direct_search',
        metadata: { caller: 'direct' }
      )

      assert_equal :active_job, result[:mode]
      assert_equal 'direct-job-1', result[:job_id]
    end

    assert_equal 1, enqueued.size
    assert_equal 'direct_search', enqueued.first[:queue]
    assert_equal ['SearchEngine::Author', { shard: 1 }], enqueued.first[:args]
    assert_equal({ into: 'authors_v2', metadata: { caller: 'direct' } }, enqueued.first[:kwargs])
  ensure
    SearchEngine.config.indexer.dispatch = :inline
  end

  def test_perform_later_uses_active_job_instrumentation_without_name_collision
    previous_adapter = ActiveJob::Base.queue_adapter
    ActiveJob::Base.queue_adapter = :test
    ActiveJob::Base.queue_adapter.enqueued_jobs.clear

    job = SearchEngine::IndexPartitionJob
          .set(queue: 'partition_search')
          .perform_later('SearchEngine::Author', { shard: 1 }, into: 'authors_v2')

    assert_kind_of ActiveJob::Base, job
    assert_equal 1, ActiveJob::Base.queue_adapter.enqueued_jobs.size
  ensure
    ActiveJob::Base.queue_adapter = previous_adapter
  end
end
