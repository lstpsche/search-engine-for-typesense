# frozen_string_literal: true

require 'test_helper'

class PostgresOutboxDrainerTest < Minitest::Test
  class FakeRepository
    attr_reader :processed_ids, :superseded_ids, :retryable_calls, :claim_args

    def initialize(events)
      @events = events
      @processed_ids = []
      @superseded_ids = []
      @retryable_calls = []
    end

    def claim_pending(limit:, worker_id:)
      @claim_args = { limit: limit, worker_id: worker_id }
      @events
    end

    def mark_processed!(event_ids)
      processed_ids.concat(event_ids)
    end

    def mark_superseded!(event_ids)
      superseded_ids.concat(event_ids)
    end

    def mark_retryable!(event_ids, error:)
      retryable_calls << [event_ids, error]
    end
  end

  class RecordingProcessor
    attr_reader :calls

    def initialize(results = {})
      @results = results
      @calls = []
    end

    def call(events:, context:)
      calls << [events.map(&:id), context]
      @results.fetch(events.first.collection) do
        SearchEngine::PostgresOutbox::ProcessorResult.success(events.map(&:id))
      end
    end
  end

  def setup
    @previous_processors = SearchEngine.config.postgres_outbox.collection_processors
    @previous_batch_sizes = SearchEngine.config.postgres_outbox.batch_sizes
    SearchEngine.config.postgres_outbox.collection_processors = {}
    SearchEngine.config.postgres_outbox.batch_sizes = {}
  end

  def teardown
    SearchEngine.config.postgres_outbox.collection_processors = @previous_processors
    SearchEngine.config.postgres_outbox.batch_sizes = @previous_batch_sizes
  end

  def test_returns_empty_summary_without_events
    repository = FakeRepository.new([])
    drainer = SearchEngine::PostgresOutbox::Drainer.new(repository: repository, worker_id: 'w1')

    summary = drainer.drain_once(limit: 10)

    assert_equal({ claimed: 0, processed: 0, superseded: 0, retryable: 0, failed: 0, collections: [] }, summary)
    assert_equal({ limit: 10, worker_id: 'w1' }, repository.claim_args)
  end

  def test_coalesces_latest_event_per_collection_document_and_marks_older_superseded
    events = [
      event(id: 1, collection: 'products', document_id: 'sku-1'),
      event(id: 3, collection: 'products', document_id: 'sku-1'),
      event(id: 2, collection: 'brands', document_id: 'brand-1')
    ]
    repository = FakeRepository.new(events)
    processor = RecordingProcessor.new
    drainer = SearchEngine::PostgresOutbox::Drainer.new(
      repository: repository,
      processor: processor,
      worker_id: 'w1'
    )

    SearchEngine::DependencyPlanner.stub(:order_events, ->(input) { input }) do
      summary = drainer.drain_once(limit: 10)

      assert_equal [1], repository.superseded_ids
      assert_equal [2, 3], repository.processed_ids.sort
      assert_equal 3, summary[:claimed]
      refute summary[:continue]
      assert_equal 2, summary[:processed]
      assert_equal 1, summary[:superseded]
    end
  end

  def test_nonempty_collection_limited_legacy_batch_sets_continue
    SearchEngine.config.postgres_outbox.batch_sizes = { products: 1 }
    repository = FakeRepository.new([event(id: 1, collection: 'products')])
    processor = RecordingProcessor.new
    drainer = SearchEngine::PostgresOutbox::Drainer.new(
      repository: repository,
      processor: processor,
      worker_id: 'w1'
    )

    SearchEngine::DependencyPlanner.stub(:order_events, ->(input) { input }) do
      summary = drainer.drain_once(limit: nil)

      assert_equal true, summary[:continue]
      assert_equal [1], repository.processed_ids
    end
  end

  def test_orders_events_with_dependency_planner
    products = event(id: 1, collection: 'products')
    barcodes = event(id: 2, collection: 'product_barcodes')
    ordered_seen = nil
    repository = FakeRepository.new([barcodes, products])
    processor = RecordingProcessor.new

    planner = lambda do |input|
      ordered_seen = input.map(&:id)
      [products, barcodes]
    end

    SearchEngine::DependencyPlanner.stub(:order_events, planner) do
      SearchEngine::PostgresOutbox::Drainer
        .new(repository: repository, processor: processor, worker_id: 'w1')
        .drain_once
    end

    assert_equal [1, 2], ordered_seen.sort
    assert_equal [[1], [2]], processor.calls.map(&:first)
  end

  def test_invokes_custom_collection_processor
    custom = RecordingProcessor.new
    SearchEngine.config.postgres_outbox.collection_processors = { 'products' => custom }
    repository = FakeRepository.new([event(id: 1, collection: 'products')])
    default = RecordingProcessor.new

    SearchEngine::DependencyPlanner.stub(:order_events, ->(input) { input }) do
      SearchEngine::PostgresOutbox::Drainer
        .new(repository: repository, processor: default, worker_id: 'w1')
        .drain_once
    end

    assert_equal [[1]], custom.calls.map(&:first)
    assert_empty default.calls
  end

  def test_failure_marks_group_and_remaining_groups_retryable_without_processing_them
    error = StandardError.new('boom')
    failure = SearchEngine::PostgresOutbox::ProcessorResult.failure([1], error: error)
    processor = RecordingProcessor.new('products' => failure)
    repository = FakeRepository.new(
      [
        event(id: 1, collection: 'products'),
        event(id: 2, collection: 'product_barcodes')
      ]
    )

    SearchEngine::DependencyPlanner.stub(:order_events, ->(input) { input }) do
      summary = SearchEngine::PostgresOutbox::Drainer
                .new(repository: repository, processor: processor, worker_id: 'w1')
                .drain_once

      assert_empty repository.processed_ids
      assert_equal [[1], [2]], repository.retryable_calls.map(&:first)
      assert_same error, repository.retryable_calls.first.last
      assert_equal 2, summary[:retryable]
      assert_equal 2, summary[:failed]
    end
  end

  def test_partial_processor_result_marks_processed_ids_and_retries_failed_ids
    error = StandardError.new('partial boom')
    partial = SearchEngine::PostgresOutbox::ProcessorResult.new(
      processed_event_ids: [1],
      failed_event_ids: [2],
      error: error
    )
    processor = RecordingProcessor.new('products' => partial)
    repository = FakeRepository.new(
      [
        event(id: 1, collection: 'products'),
        event(id: 2, collection: 'products'),
        event(id: 3, collection: 'product_barcodes')
      ]
    )

    SearchEngine::DependencyPlanner.stub(:order_events, ->(input) { input }) do
      summary = SearchEngine::PostgresOutbox::Drainer
                .new(repository: repository, processor: processor, worker_id: 'w1')
                .drain_once

      assert_equal [1], repository.processed_ids
      assert_equal [[2], [3]], repository.retryable_calls.map(&:first)
      assert_same error, repository.retryable_calls.first.last
      assert_equal 1, summary[:processed]
      assert_equal 2, summary[:retryable]
    end
  end

  def test_partial_success_result_retries_uncovered_ids
    partial_success = SearchEngine::PostgresOutbox::ProcessorResult.success([1])
    processor = RecordingProcessor.new('products' => partial_success)
    repository = FakeRepository.new(
      [
        event(id: 1, collection: 'products'),
        event(id: 2, collection: 'products')
      ]
    )

    SearchEngine::DependencyPlanner.stub(:order_events, ->(input) { input }) do
      summary = SearchEngine::PostgresOutbox::Drainer
                .new(repository: repository, processor: processor, worker_id: 'w1')
                .drain_once

      assert_equal [1], repository.processed_ids
      assert_equal [[2]], repository.retryable_calls.map(&:first)
      assert_match(/reported success without covering event ids: 2/, repository.retryable_calls.first.last)
      assert_equal 1, summary[:processed]
      assert_equal 1, summary[:retryable]
    end
  end

  def test_target_key_is_added_to_summary_and_processor_context
    repository = FakeRepository.new([event(id: 1, collection: 'products', delivery_id: 100, target_key: 'target_1')])
    processor = RecordingProcessor.new
    drainer = SearchEngine::PostgresOutbox::Drainer.new(
      repository: repository,
      processor: processor,
      worker_id: 'w1',
      target_key: :target_1
    )

    SearchEngine::DependencyPlanner.stub(:order_events, ->(input) { input }) do
      summary = drainer.drain_once(limit: 10)

      assert_equal 'target_1', summary[:target_key]
      assert_equal true, summary[:continue]
      assert_equal [1], repository.processed_ids
      assert_equal [{ worker_id: 'w1', target_key: 'target_1' }], processor.calls.map(&:last)
    end
  end

  def test_target_key_constructor_builds_target_scoped_repository
    repository_seen = nil
    factory = lambda do |target_key: nil|
      repository_seen = FakeRepository.new([])
      assert_equal 'target_1', target_key
      repository_seen
    end

    SearchEngine::PostgresOutbox::Repository.stub(:new, factory) do
      summary = SearchEngine::PostgresOutbox::Drainer
                .new(worker_id: 'w1', target_key: :target_1)
                .drain_once(limit: 10)

      assert_equal 'target_1', summary[:target_key]
      assert_equal({ limit: 10, worker_id: 'w1' }, repository_seen.claim_args)
    end
  end

  private

  def event(id:, collection: 'products', document_id: nil, delivery_id: nil, target_key: nil)
    SearchEngine::PostgresOutbox::Event.new(
      id: id,
      source_table: collection,
      source_model_name: 'Product',
      collection: collection,
      record_id: id.to_s,
      document_id: document_id || id.to_s,
      operation: 'upsert',
      attempts: 0,
      payload: {},
      delivery_id: delivery_id,
      target_key: target_key
    )
  end
end
