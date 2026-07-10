# frozen_string_literal: true

require 'test_helper'
require 'active_support/cache'

class IndexingRunStoreTest < Minitest::Test
  Summary = Struct.new(:docs_total, :success_total, :failed_total, :sample_error, keyword_init: true)

  def setup
    @store = SearchEngine::IndexingRunStore::RailsCache.new(cache: ActiveSupport::Cache::MemoryStore.new)
  end

  def test_partition_keys_are_stable_for_equivalent_hashes
    left = { shard: 1, range: { to: 20, from: 10 } }
    right = { 'range' => { 'from' => 10, 'to' => 20 }, 'shard' => 1 }

    assert_equal SearchEngine::IndexingRun.partition_key(left), SearchEngine::IndexingRun.partition_key(right)
  end

  def test_create_run_serializes_partitions_as_json_safe_entries
    snapshot = @store.create_run(
      run_id: 'run-1',
      collection: 'products',
      collection_class_name: 'SearchEngine::Product',
      into: 'products_20260604',
      partitions: [{ shard: :north }, Time.utc(2026, 6, 4, 12, 0, 0)],
      ttl_s: 60
    )

    assert_equal 'run-1', snapshot[:run_id]
    assert_equal 'running', snapshot[:status]
    assert_equal 2, snapshot[:total_partitions]
    assert_equal 2, snapshot[:partitions].size
    assert(snapshot[:partitions].values.all? { |entry| entry[:status] == 'pending' })
    assert(snapshot[:partitions].values.all? { |entry| entry[:partition_display].is_a?(String) })
  end

  def test_started_succeeded_failed_transitions_and_aggregation
    partitions = [{ shard: 1 }, { shard: 2 }]
    @store.create_run(
      run_id: 'run-2',
      collection: 'products',
      collection_class_name: 'SearchEngine::Product',
      into: 'products_20260604',
      partitions: partitions,
      ttl_s: 60
    )
    first_key = SearchEngine::IndexingRun.partition_key(partitions.first)
    second_key = SearchEngine::IndexingRun.partition_key(partitions.last)

    @store.mark_started(run_id: 'run-2', partition_key: first_key, job_id: 'job-1')
    @store.mark_succeeded(
      run_id: 'run-2',
      partition_key: first_key,
      summary: Summary.new(docs_total: 10, success_total: 10, failed_total: 0, sample_error: nil)
    )
    failed_snapshot = @store.mark_failed(run_id: 'run-2', partition_key: second_key, error: RuntimeError.new('boom'))

    assert_equal 'failed', failed_snapshot[:status]
    assert_equal 'job-1', failed_snapshot[:partitions][first_key][:job_id]

    aggregate = SearchEngine::IndexingRun.aggregate_result(@store.snapshot(run_id: 'run-2'))
    assert_equal :partial, aggregate[:status]
    assert_equal 10, aggregate[:docs_total]
    assert_equal 10, aggregate[:success_total]
    assert_equal 1, aggregate[:failed_total]
    assert_match(/RuntimeError: boom/, aggregate[:sample_error])
  end

  def test_all_succeeded_aggregate_is_ok
    partitions = [1, 2]
    @store.create_run(
      run_id: 'run-3',
      collection: 'products',
      collection_class_name: 'SearchEngine::Product',
      into: 'products_20260604',
      partitions: partitions,
      ttl_s: 60
    )

    partitions.each do |partition|
      @store.mark_succeeded(
        run_id: 'run-3',
        partition_key: SearchEngine::IndexingRun.partition_key(partition),
        summary: { docs_total: 3, success_total: 3, failed_total: 0 }
      )
    end

    snapshot = @store.snapshot(run_id: 'run-3')
    assert_equal 'succeeded', snapshot[:status]
    assert_equal(
      { status: :ok, docs_total: 6, success_total: 6, failed_total: 0, sample_error: nil },
      SearchEngine::IndexingRun.aggregate_result(snapshot)
    )
  end

  def test_record_attempt_keeps_partition_running_and_does_not_fail_siblings
    partitions = [1, 2]
    snapshot = @store.create_run(
      run_id: 'run-retry',
      collection: 'products',
      collection_class_name: 'SearchEngine::Product',
      into: 'products_20260604',
      partitions: partitions,
      ttl_s: 60
    )
    first_key, second_key = snapshot[:partitions].keys
    summary = Summary.new(docs_total: 3, success_total: 2, failed_total: 1, sample_error: 'bad row')

    @store.mark_started(run_id: 'run-retry', partition_key: first_key, job_id: 'job-1')
    attempt_snapshot = @store.record_attempt(
      run_id: 'run-retry',
      partition_key: first_key,
      summary: summary,
      error: SearchEngine::Errors::PartitionImportFailed.new(summary)
    )

    first = attempt_snapshot[:partitions][first_key]
    second = attempt_snapshot[:partitions][second_key]
    assert_equal 'running', attempt_snapshot[:status]
    assert_equal 'running', first[:status]
    assert_equal 1, first[:attempts]
    assert_equal 3, first[:docs_total]
    assert_equal 2, first[:success_total]
    assert_equal 1, first[:failed_total]
    assert_equal 'bad row', first[:sample_error]
    assert_equal 'pending', second[:status]
    assert_equal 0, second[:failed_total]
  end

  def test_retry_can_overwrite_partial_attempt_with_success
    snapshot = @store.create_run(
      run_id: 'run-retry-success',
      collection: 'products',
      collection_class_name: 'SearchEngine::Product',
      into: 'products_20260604',
      partitions: [1],
      ttl_s: 60
    )
    key = snapshot[:partitions].keys.first
    partial = Summary.new(docs_total: 3, success_total: 2, failed_total: 1, sample_error: 'bad row')

    @store.mark_started(run_id: 'run-retry-success', partition_key: key, job_id: 'job-1')
    @store.record_attempt(
      run_id: 'run-retry-success',
      partition_key: key,
      summary: partial,
      error: SearchEngine::Errors::PartitionImportFailed.new(partial)
    )
    @store.mark_started(run_id: 'run-retry-success', partition_key: key, job_id: 'job-1')
    succeeded = @store.mark_succeeded(
      run_id: 'run-retry-success',
      partition_key: key,
      summary: Summary.new(docs_total: 3, success_total: 3, failed_total: 0, sample_error: nil)
    )

    entry = succeeded[:partitions][key]
    assert_equal 'succeeded', succeeded[:status]
    assert_equal 2, entry[:attempts]
    assert_equal 3, entry[:success_total]
    assert_equal 0, entry[:failed_total]
    assert_nil entry[:sample_error]
  end

  def test_terminal_partitions_reject_late_or_duplicate_job_transitions
    snapshot = @store.create_run(
      run_id: 'run-terminal',
      collection: 'products',
      collection_class_name: 'SearchEngine::Product',
      into: 'products_20260604',
      partitions: [1, 2],
      ttl_s: 60
    )
    succeeded_key, failed_key = snapshot[:partitions].keys
    success = Summary.new(docs_total: 1, success_total: 1, failed_total: 0, sample_error: nil)
    partial = Summary.new(docs_total: 1, success_total: 0, failed_total: 1, sample_error: 'bad row')
    partial_error = SearchEngine::Errors::PartitionImportFailed.new(partial)

    @store.mark_succeeded(run_id: 'run-terminal', partition_key: succeeded_key, summary: success)
    @store.mark_failed(run_id: 'run-terminal', partition_key: failed_key, error: 'terminal failure')

    assert_raises(SearchEngine::IndexingRunStore::StaleRun) do
      @store.mark_started(run_id: 'run-terminal', partition_key: succeeded_key, job_id: 'late-job')
    end
    assert_raises(SearchEngine::IndexingRunStore::StaleRun) do
      @store.mark_failed(run_id: 'run-terminal', partition_key: succeeded_key, error: 'late failure')
    end
    assert_raises(SearchEngine::IndexingRunStore::StaleRun) do
      @store.mark_started(run_id: 'run-terminal', partition_key: failed_key, job_id: 'retry-job')
    end
    assert_raises(SearchEngine::IndexingRunStore::StaleRun) do
      @store.record_attempt(
        run_id: 'run-terminal',
        partition_key: failed_key,
        summary: partial,
        error: partial_error
      )
    end
    assert_raises(SearchEngine::IndexingRunStore::StaleRun) do
      @store.mark_succeeded(run_id: 'run-terminal', partition_key: failed_key, summary: success)
    end
  end

  def test_concurrent_partition_updates_preserve_all_statuses
    partitions = (1..10).to_a
    @store.create_run(
      run_id: 'run-concurrent',
      collection: 'products',
      collection_class_name: 'SearchEngine::Product',
      into: 'products_20260604',
      partitions: partitions,
      ttl_s: 60
    )

    threads = partitions.map do |partition|
      Thread.new do
        @store.mark_succeeded(
          run_id: 'run-concurrent',
          partition_key: SearchEngine::IndexingRun.partition_key(partition),
          summary: { docs_total: 1, success_total: 1, failed_total: 0 }
        )
      end
    end
    threads.each(&:join)

    snapshot = @store.snapshot(run_id: 'run-concurrent')
    assert_equal 'succeeded', snapshot[:status]
    assert_equal partitions.size, snapshot[:partitions].size
    assert_equal(
      { status: :ok, docs_total: 10, success_total: 10, failed_total: 0, sample_error: nil },
      SearchEngine::IndexingRun.aggregate_result(snapshot)
    )
  end

  def test_missing_partition_cache_entry_fails_closed
    snapshot = @store.create_run(
      run_id: 'run-missing-partition',
      collection: 'products',
      collection_class_name: 'SearchEngine::Product',
      into: 'products_20260604',
      partitions: [1, 2],
      ttl_s: 60
    )
    first_key = snapshot[:partitions].keys.first
    @store.instance_variable_get(:@cache).delete(
      "search_engine:indexing_run:run-missing-partition:partition:#{first_key}"
    )

    aggregate = SearchEngine::IndexingRun.aggregate_result(@store.snapshot(run_id: 'run-missing-partition'))
    assert_equal :failed, aggregate[:status]
    assert_equal 1, aggregate[:failed_total]
    assert_match(/partition metadata missing/, aggregate[:sample_error])
  end

  def test_missing_run_and_partition_fail_loudly
    assert_raises(SearchEngine::IndexingRunStore::StaleRun) do
      @store.mark_started(run_id: 'missing', partition_key: 'p-missing')
    end

    @store.create_run(
      run_id: 'run-4',
      collection: 'products',
      collection_class_name: 'SearchEngine::Product',
      into: 'products_20260604',
      partitions: [1],
      ttl_s: 60
    )

    assert_raises(SearchEngine::IndexingRunStore::StaleRun) do
      @store.mark_started(run_id: 'run-4', partition_key: 'p-missing')
    end
  end

  def test_missing_partition_cache_entry_is_stale_for_mutations
    snapshot = @store.create_run(
      run_id: 'run-missing-mutation-partition',
      collection: 'products',
      collection_class_name: 'SearchEngine::Product',
      into: 'products_20260604',
      partitions: [1],
      ttl_s: 60
    )
    partition_key = snapshot[:partitions].keys.first
    @store.instance_variable_get(:@cache).delete(
      "search_engine:indexing_run:run-missing-mutation-partition:partition:#{partition_key}"
    )

    assert_raises(SearchEngine::IndexingRunStore::StaleRun) do
      @store.mark_succeeded(
        run_id: 'run-missing-mutation-partition',
        partition_key: partition_key,
        summary: { docs_total: 1, success_total: 1, failed_total: 0 }
      )
    end
  end

  def test_expire_removes_snapshot
    @store.create_run(
      run_id: 'run-5',
      collection: 'products',
      collection_class_name: 'SearchEngine::Product',
      into: 'products_20260604',
      partitions: [1],
      ttl_s: 60
    )

    @store.expire(run_id: 'run-5')

    assert_nil @store.snapshot(run_id: 'run-5')
  end

  def test_resolver_validates_configured_store_contract
    bad_store = Object.new
    config = Struct.new(:indexer).new(Struct.new(:partition_run_store).new(bad_store))

    error = assert_raises(ArgumentError) { SearchEngine::IndexingRunStore.resolve(config: config) }
    assert_match(/missing methods/, error.message)
  end
end
