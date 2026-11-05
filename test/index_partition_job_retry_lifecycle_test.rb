# frozen_string_literal: true

require 'test_helper'
require 'active_job'
require 'active_support/cache'

class IndexPartitionJobRetryLifecycleTest < Minitest::Test
  Summary = Struct.new(:status, :docs_total, :success_total, :failed_total, :sample_error, keyword_init: true)

  def setup
    @previous_adapter = ActiveJob::Base.queue_adapter
    @previous_retries = SearchEngine.config.indexer.retries
    ActiveJob::Base.queue_adapter = :test
    ActiveJob::Base.queue_adapter.enqueued_jobs.clear
    SearchEngine.config.indexer.retries = { attempts: 2, base: 0.0, max: 0.0, jitter_fraction: 0.0 }
    @store = SearchEngine::IndexingRunStore::RailsCache.new(cache: ActiveSupport::Cache::MemoryStore.new)
    @partition = { shard: 1 }
    @partition_key = SearchEngine::IndexingRun.partition_key(@partition)
    @store.create_run(
      run_id: 'run-retry-lifecycle',
      collection: 'authors',
      collection_class_name: 'SearchEngine::Author',
      into: 'authors_v2',
      partitions: [@partition],
      ttl_s: 60
    )
  end

  def teardown
    SearchEngine.config.indexer.retries = @previous_retries
    ActiveJob::Base.queue_adapter = @previous_adapter
  end

  def test_workers_only_read_run_metadata_and_their_own_partition
    run_id = 'run-many-partitions'
    partitions = (1..10).to_a
    @store.create_run(
      run_id: run_id, collection: 'authors', collection_class_name: 'SearchEngine::Author',
      into: 'authors_v2', partitions: partitions, ttl_s: 60
    )

    reads = SearchEngine::Test.capture_events(/\Acache_read(?:_multi)?\.active_support\z/) do
      SearchEngine::IndexingRunStore.stub(:resolve, @store) do
        SearchEngine::Indexer.stub(:rebuild_partition!, successful_summary) do
          partitions.each do |partition|
            SearchEngine::IndexPartitionJob.new.perform('SearchEngine::Author', partition, run_id: run_id)
          end
        end
      end
    end

    reads_by_key = reads.flat_map { |event| Array(event[:payload][:key]) }.tally
    assert_equal partitions.size * 2, reads_by_key.delete("search_engine:indexing_run:#{run_id}:meta")
    assert_equal partitions.size, reads_by_key.size
    assert_equal [2], reads_by_key.values.uniq
    assert_equal 'succeeded', @store.snapshot(run_id: run_id)[:status]
  end

  def test_perform_now_records_partial_attempt_enqueues_retry_and_retry_can_succeed
    summaries = [partial_summary, successful_summary]

    SearchEngine::IndexingRunStore.stub(:resolve, @store) do
      SearchEngine::Indexer.stub(:rebuild_partition!, ->(*) { summaries.shift }) do
        result = perform_now

        assert_kind_of SearchEngine::Errors::PartitionImportFailed, result
        assert_equal 1, ActiveJob::Base.queue_adapter.enqueued_jobs.size
        assert_running_partial_snapshot

        SearchEngine::IndexPartitionJob.execute(serialized_retry_job)
      end
    end

    entry = partition_entry
    assert_equal 'succeeded', @store.snapshot(run_id: 'run-retry-lifecycle')[:status]
    assert_equal 'succeeded', entry[:status]
    assert_equal 2, entry[:attempts]
    assert_equal 2, entry[:success_total]
    assert_equal 0, entry[:failed_total]
    assert_nil entry[:sample_error]
    assert_empty ActiveJob::Base.queue_adapter.enqueued_jobs
  end

  def test_exhaustion_retains_partial_summary_marks_terminal_and_surfaces_error
    SearchEngine.config.indexer.retries = { attempts: 1, base: 0.0, max: 0.0, jitter_fraction: 0.0 }

    error = assert_raises(SearchEngine::Errors::PartitionImportFailed) do
      SearchEngine::IndexingRunStore.stub(:resolve, @store) do
        SearchEngine::Indexer.stub(:rebuild_partition!, partial_summary) do
          perform_now
        end
      end
    end

    snapshot = @store.snapshot(run_id: 'run-retry-lifecycle')
    entry = snapshot[:partitions][@partition_key]
    assert_equal 'failed', snapshot[:status]
    assert_equal 'failed', entry[:status]
    assert_equal 1, entry[:attempts]
    assert_equal 2, entry[:docs_total]
    assert_equal 1, entry[:success_total]
    assert_equal 1, entry[:failed_total]
    assert_equal 'invalid field', entry[:sample_error]
    assert_match(/invalid field/, error.message)
    assert_empty ActiveJob::Base.queue_adapter.enqueued_jobs
  end

  private

  def perform_now
    SearchEngine::IndexPartitionJob.perform_now(
      'SearchEngine::Author',
      @partition,
      into: 'authors_v2',
      run_id: 'run-retry-lifecycle',
      partition_key: @partition_key
    )
  end

  def partial_summary
    Summary.new(
      status: :partial,
      docs_total: 2,
      success_total: 1,
      failed_total: 1,
      sample_error: 'invalid field'
    )
  end

  def successful_summary
    Summary.new(status: :ok, docs_total: 2, success_total: 2, failed_total: 0, sample_error: nil)
  end

  def serialized_retry_job
    ActiveJob::Base.queue_adapter.enqueued_jobs.shift.reject { |key, _value| key.is_a?(Symbol) }
  end

  def partition_entry
    @store.snapshot(run_id: 'run-retry-lifecycle')[:partitions][@partition_key]
  end

  def assert_running_partial_snapshot
    snapshot = @store.snapshot(run_id: 'run-retry-lifecycle')
    entry = snapshot[:partitions][@partition_key]
    assert_equal 'running', snapshot[:status]
    assert_equal 'running', entry[:status]
    assert_equal 1, entry[:attempts]
    assert_equal 2, entry[:docs_total]
    assert_equal 1, entry[:success_total]
    assert_equal 1, entry[:failed_total]
    assert_equal 'invalid field', entry[:sample_error]
  end
end
