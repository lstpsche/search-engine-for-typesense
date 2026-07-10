# frozen_string_literal: true

require 'test_helper'
require 'active_job'
require 'ostruct'
require_relative '../app/search_engine/search_engine/index_partition_job'

class AsyncPartitionLifecycleTest < Minitest::Test
  Summary = Struct.new(:status, :docs_total, :success_total, :failed_total, :sample_error, keyword_init: true)

  class PartitionedProduct < SearchEngine::Base
    collection 'async_partition_lifecycle_products'
    identify_by :id
    attribute :name, :string

    index do
      partitions { [1, 2] }
      partition_max_parallel 2
      partition_fetch { |partition| [[OpenStruct.new(id: partition, name: "Product #{partition}")]] }
      map { |record| { id: record.id, name: record.name } }
    end
  end

  class FakeClient
    attr_reader :created, :deleted, :upserts
    attr_accessor :alias_target

    def initialize(alias_target: nil)
      @alias_target = alias_target
      @collections = []
      @created = []
      @deleted = []
      @upserts = []
    end

    def resolve_alias(_logical)
      @alias_target
    end

    def upsert_alias(logical, physical)
      @upserts << [logical, physical]
      @alias_target = physical
      { name: logical, collection_name: physical }
    end

    def create_collection(schema)
      @created << schema[:name]
      @collections << { name: schema[:name] }
      schema
    end

    def delete_collection(name, timeout_ms: nil) # rubocop:disable Lint/UnusedMethodArgument -- matches real client signature
      @deleted << name
      @collections.reject! { |collection| (collection[:name] || collection['name']) == name }
      { name: name, status: 200 }
    end

    def list_collections(*)
      @collections
    end

    def retrieve_collection_schema(_name)
      nil
    end
  end

  class FakeRunStore
    def initialize
      @runs = {}
    end

    def create_run(run_id:, collection:, collection_class_name:, into:, partitions:, ttl_s:)
      @runs[run_id] = {
        run_id: run_id,
        collection: collection,
        collection_class_name: collection_class_name,
        into: into,
        status: 'running',
        total_partitions: partitions.size,
        ttl_s: ttl_s,
        partitions: partitions.each_with_object({}) do |partition, hash|
          hash[SearchEngine::IndexingRun.partition_key(partition)] =
            SearchEngine::IndexingRun.partition_entry(partition)
        end
      }
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

    def record_attempt(run_id:, partition_key:, summary:, error:)
      update_partition(run_id, partition_key) do |entry|
        entry[:status] = 'running'
        entry[:docs_total] = summary.docs_total
        entry[:success_total] = summary.success_total
        entry[:failed_total] = summary.failed_total
        entry[:sample_error] = summary.sample_error || error.to_s
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
      @runs.delete(run_id)
    end

    private

    def update_partition(run_id, partition_key)
      entry = @runs.fetch(run_id)[:partitions].fetch(partition_key)
      yield entry
    end
  end

  class CompletingJobRelation
    attr_reader :enqueued

    def initialize(store:, client:, failure: nil)
      @store = store
      @client = client
      @failure = failure
      @enqueued = []
    end

    def set(queue:)
      @queue = queue
      self
    end

    def perform_later(*_args, **kwargs)
      @enqueued << kwargs.merge(queue: @queue)
      run_id = kwargs.fetch(:run_id)
      partition_key = kwargs.fetch(:partition_key)
      @store.mark_started(run_id: run_id, partition_key: partition_key, job_id: "job-#{@enqueued.size}")

      if @failure && @enqueued.size == @failure.fetch(:partition_number)
        @store.mark_failed(run_id: run_id, partition_key: partition_key, error: @failure.fetch(:error))
      else
        raise 'alias swapped before async partition run completed' unless @client.upserts.empty?

        @store.mark_succeeded(
          run_id: run_id,
          partition_key: partition_key,
          summary: Summary.new(status: :ok, docs_total: 1, success_total: 1, failed_total: 0, sample_error: nil)
        )
      end
    end
  end

  class PendingJobRelation
    attr_reader :enqueued

    def initialize
      @enqueued = []
    end

    def set(queue:)
      @queue = queue
      self
    end

    def perform_later(*_args, **kwargs)
      @enqueued << kwargs.merge(queue: @queue)
    end
  end

  def setup
    SearchEngine.configure do |config|
      config.schema.retention.keep_last = 0
      config.indexer.partition_execution = :active_job
      config.indexer.partition_poll_interval_s = 0
      config.indexer.partition_timeout_s = 1
      config.indexer.partition_run_store = nil
    end
  end

  def teardown
    SearchEngine.configure do |config|
      config.schema.retention.keep_last = 0
      config.indexer.partition_execution = :inline
      config.indexer.partition_poll_interval_s = 2
      config.indexer.partition_timeout_s = nil
      config.indexer.partition_run_store = nil
    end
  end

  def test_successful_async_run_swaps_alias_only_after_all_partitions_succeed
    client = FakeClient.new
    store = FakeRunStore.new
    jobs = CompletingJobRelation.new(store: store, client: client)
    forced = 'async_partition_lifecycle_products_20260604_000001_001'

    result = with_async_lifecycle_stubs(store: store, jobs: jobs, forced: forced) do
      PartitionedProduct.index_collection(client: client, force_rebuild: true)
    end

    assert_equal :ok, result[:status]
    assert_equal [['async_partition_lifecycle_products', forced]], client.upserts
    assert_equal [forced], client.created
    assert_empty client.deleted
    assert_equal 2, jobs.enqueued.size
    assert(jobs.enqueued.all? { |job| job[:into] == forced })
  end

  def test_failed_async_partition_prevents_alias_swap_and_cleans_new_physical_collection
    client = FakeClient.new(alias_target: 'async_partition_lifecycle_products_20260603_000001_001')
    store = FakeRunStore.new
    jobs = CompletingJobRelation.new(
      store: store,
      client: client,
      failure: { partition_number: 2, error: 'partition 2 failed' }
    )
    forced = 'async_partition_lifecycle_products_20260604_000001_001'

    result = with_async_lifecycle_stubs(store: store, jobs: jobs, forced: forced) do
      PartitionedProduct.index_collection(client: client, force_rebuild: true)
    end

    assert_equal :partial, result[:status]
    assert_match(/partition 2 failed/, result[:sample_error])
    assert_empty client.upserts
    assert_equal 'async_partition_lifecycle_products_20260603_000001_001', client.alias_target
    assert_equal [forced], client.deleted
  end

  def test_async_timeout_prevents_alias_swap_and_reports_useful_failure
    client = FakeClient.new(alias_target: 'async_partition_lifecycle_products_20260603_000001_001')
    store = FakeRunStore.new
    jobs = PendingJobRelation.new
    forced = 'async_partition_lifecycle_products_20260604_000001_001'
    SearchEngine.config.indexer.partition_timeout_s = 0

    result = with_async_lifecycle_stubs(store: store, jobs: jobs, forced: forced) do
      PartitionedProduct.index_collection(client: client, force_rebuild: true)
    end

    assert_equal :failed, result[:status]
    assert_match(/timed out/, result[:sample_error])
    assert_empty client.upserts
    assert_equal 'async_partition_lifecycle_products_20260603_000001_001', client.alias_target
    assert_equal [forced], client.deleted
    assert_equal 2, jobs.enqueued.size
  end

  def test_inline_partition_path_remains_used_when_async_config_is_disabled
    client = FakeClient.new
    forced = 'async_partition_lifecycle_products_20260604_000001_001'
    calls = []
    result = { status: :ok, docs_total: 2, success_total: 2, failed_total: 0, sample_error: nil }
    SearchEngine.config.indexer.partition_execution = :inline

    parallel_indexer = lambda do |parts, into, max_parallel, _compiled|
      calls << [parts, into, max_parallel]
      result
    end

    SearchEngine::AsyncPartitionCoordinator.stub(:call, ->(**) { flunk 'async coordinator should not be called' }) do
      PartitionedProduct.stub(:__se_index_partitions_parallel!, parallel_indexer) do
        SearchEngine::Schema.stub(:generate_physical_name, forced) do
          PartitionedProduct.index_collection(client: client, force_rebuild: true)
        end
      end
    end

    assert_equal [[[1, 2], forced, 2]], calls
    assert_equal [['async_partition_lifecycle_products', forced]], client.upserts
  end

  private

  def with_async_lifecycle_stubs(store:, jobs:, forced:, &block)
    SearchEngine.config.indexer.partition_run_store = store
    SearchEngine::IndexingRun.stub(:generate_id, 'run-lifecycle') do
      SearchEngine::IndexPartitionJob.stub(:set, ->(queue:) { jobs.set(queue: queue) }) do
        SearchEngine::Schema.stub(:generate_physical_name, forced) do
          block.yield
        end
      end
    end
  end
end
