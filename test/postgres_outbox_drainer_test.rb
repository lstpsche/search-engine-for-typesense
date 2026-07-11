# frozen_string_literal: true

require 'test_helper'

class PostgresOutboxDrainerTest < Minitest::Test
  class FakeRepository
    attr_reader :processed_ids,
                :superseded_ids,
                :retryable_calls,
                :terminal_retryable_calls,
                :claim_args,
                :claim_history,
                :renewal_calls

    attr_accessor :renewal_error, :renewal_results

    def initialize(events)
      @events = events
      @processed_ids = []
      @superseded_ids = []
      @retryable_calls = []
      @terminal_retryable_calls = []
      @claim_history = []
      @renewal_calls = []
      @renewal_results = []
    end

    def claim_pending(limit:, worker_id:)
      @claim_args = { limit: limit, worker_id: worker_id }
      claim_history << @claim_args
      @events
    end

    def mark_processed!(events)
      ids = events.map(&:id)
      processed_ids.concat(ids)
      ids
    end

    def mark_superseded!(events)
      ids = events.map(&:id)
      superseded_ids.concat(ids)
      ids
    end

    def mark_retryable!(events, error:)
      ids = events.map(&:id)
      retryable_calls << [ids, error]
      terminal_ids = events.filter_map do |event|
        event.id if event.attempts + 1 >= SearchEngine.config.postgres_outbox.max_attempts
      end
      terminal_retryable_calls << [terminal_ids, error] if terminal_ids.any?
      ids
    end

    def renew_leases!(events)
      raise renewal_error if renewal_error

      ids = events.map(&:id)
      renewal_calls << ids
      renewal_results.empty? ? ids : renewal_results.shift
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

  class CallbackProcessor
    attr_reader :calls

    def initialize(&callback)
      @callback = callback
      @calls = []
    end

    def call(events:, context:)
      calls << [events, context]
      @callback.call(events, context)
    end
  end

  # Deterministic protocol model used to exercise lease/order interleavings through the real Drainer.
  class LeaseAwareRepository
    attr_reader :deliveries

    def initialize(events)
      @deliveries = []
      events.each { |event| add(event) }
    end

    def add(event)
      deliveries << {
        event: event,
        delivery_id: event.delivery_id,
        target_key: event.target_key,
        status: :pending,
        lease_owner: nil,
        attempts: 0
      }
    end

    def claim_pending(limit:, worker_id:)
      processing_keys = deliveries.filter_map do |delivery|
        delivery_key(delivery) if delivery[:status] == :processing
      end
      latest_pending = deliveries
                       .select { |delivery| delivery[:status] == :pending }
                       .group_by { |delivery| delivery_key(delivery) }
                       .values
                       .map { |versions| versions.max_by { |delivery| delivery[:event].id.to_i } }
                       .reject { |delivery| processing_keys.include?(delivery_key(delivery)) }
                       .sort_by { |delivery| delivery[:event].id.to_i }
                       .first(limit || deliveries.size)

      latest_pending.map do |delivery|
        delivery[:status] = :processing
        delivery[:lease_owner] = worker_id
        claimed_event(delivery)
      end
    end

    def mark_processed!(events)
      mutate(events, :processed)
    end

    def mark_superseded!(events)
      mutate(events, :superseded)
    end

    def mark_retryable!(events, error:)
      mutate(events, :pending) do |delivery|
        delivery[:attempts] += 1
        delivery[:last_error] = error
      end
    end

    def renew_leases!(events)
      Array(events).filter_map do |event|
        delivery = delivery(event.delivery_id)
        next unless delivery
        next unless delivery[:status] == :processing
        next unless delivery[:lease_owner] == event.delivery_lease_owner

        event.id
      end
    end

    def expire_and_requeue!(delivery_id)
      delivery = delivery(delivery_id)
      delivery[:status] = :pending
      delivery[:lease_owner] = nil
    end

    def delivery(delivery_id)
      deliveries.find { |candidate| candidate[:delivery_id] == delivery_id }
    end

    def parent_status(event_id)
      delivery = deliveries.find { |candidate| candidate[:event].id == event_id }
      delivery && delivery[:status]
    end

    private

    def mutate(events, status)
      Array(events).filter_map do |event|
        delivery = delivery(event.delivery_id)
        next unless delivery
        next unless delivery[:status] == :processing
        next unless delivery[:lease_owner] == event.delivery_lease_owner

        yield delivery if block_given?
        delivery[:status] = status
        delivery[:lease_owner] = nil unless status == :processing
        event.id
      end
    end

    def delivery_key(delivery)
      [delivery[:target_key], *delivery[:event].coalesce_key]
    end

    def claimed_event(delivery)
      event = delivery[:event]
      SearchEngine::PostgresOutbox::Event.new(
        id: event.id,
        source_table: event.source_table,
        source_model_name: event.source_model_name,
        collection: event.collection,
        record_id: event.record_id,
        document_id: event.document_id,
        operation: event.operation,
        attempts: delivery[:attempts],
        payload: event.payload,
        delivery_id: delivery[:delivery_id],
        target_key: delivery[:target_key],
        delivery_lease_owner: delivery[:lease_owner]
      )
    end
  end

  def setup
    @previous_processors = SearchEngine.config.postgres_outbox.collection_processors
    @previous_batch_sizes = SearchEngine.config.postgres_outbox.batch_sizes
    @previous_clear_cache_after_write = SearchEngine.config.postgres_outbox.clear_cache_after_write
    SearchEngine.config.postgres_outbox.collection_processors = {}
    SearchEngine.config.postgres_outbox.batch_sizes = {}
    SearchEngine.config.postgres_outbox.clear_cache_after_write = false
  end

  def teardown
    SearchEngine.config.postgres_outbox.collection_processors = @previous_processors
    SearchEngine.config.postgres_outbox.batch_sizes = @previous_batch_sizes
    SearchEngine.config.postgres_outbox.clear_cache_after_write = @previous_clear_cache_after_write
  end

  def test_returns_empty_summary_without_events
    repository = FakeRepository.new([])
    drainer = SearchEngine::PostgresOutbox::Drainer.new(repository: repository, worker_id: 'w1')

    summary = drainer.drain_once(limit: 10)

    assert_equal(
      { claimed: 0, processed: 0, superseded: 0, retryable: 0, failed: 0, stale: 0, collections: [] },
      summary
    )
    assert_equal 10, repository.claim_args[:limit]
    assert_match(/\Aw1:[0-9a-f-]{36}\z/, repository.claim_args[:worker_id])
  end

  def test_generates_a_unique_lease_owner_for_each_claim_generation
    repository = FakeRepository.new([])
    drainer = SearchEngine::PostgresOutbox::Drainer.new(repository: repository, worker_id: 'w1')
    lease_ids = %w[lease-a lease-b]

    SecureRandom.stub(:uuid, -> { lease_ids.shift }) do
      drainer.drain_once(limit: 10)
      drainer.drain_once(limit: 10)
    end

    claim_workers = repository.claim_history.map { |args| args[:worker_id] }
    assert_equal %w[w1:lease-a w1:lease-b], claim_workers
  end

  def test_lease_owner_is_sanitized_bounded_and_keeps_the_unique_uuid_suffix
    repository = FakeRepository.new([])
    worker_id = "worker name/with unsafe chars/#{'x' * 300}"
    drainer = SearchEngine::PostgresOutbox::Drainer.new(repository: repository, worker_id: worker_id)
    lease_ids = %w[00000000-0000-4000-8000-000000000001 00000000-0000-4000-8000-000000000002]

    SecureRandom.stub(:uuid, -> { lease_ids.shift }) do
      drainer.drain_once(limit: 10)
      drainer.drain_once(limit: 10)
    end

    owners = repository.claim_history.map { |args| args[:worker_id] }
    assert_equal 2, owners.uniq.size
    all_bounded = owners.all? { |owner| owner.length <= 255 }
    assert all_bounded
    assert owners[0].end_with?(':00000000-0000-4000-8000-000000000001')
    assert owners[1].end_with?(':00000000-0000-4000-8000-000000000002')
    refute_match(%r{[ /]}, owners.join)
  end

  def test_stale_success_cannot_acknowledge_a_delivery_reclaimed_by_another_worker
    original = event(id: 1, document_id: 'sku-1', delivery_id: 101, target_key: 'target_1')
    repository = LeaseAwareRepository.new([original])
    worker_b_summary = nil
    worker_b = SearchEngine::PostgresOutbox::Drainer.new(
      repository: repository,
      processor: RecordingProcessor.new,
      worker_id: 'worker-b',
      target_key: 'target_1'
    )
    worker_a_processor = CallbackProcessor.new do |events, _context|
      repository.expire_and_requeue!(events.first.delivery_id)
      worker_b_summary = worker_b.drain_once(limit: 1)
      SearchEngine::PostgresOutbox::ProcessorResult.success(events.map(&:id))
    end
    worker_a = SearchEngine::PostgresOutbox::Drainer.new(
      repository: repository,
      processor: worker_a_processor,
      worker_id: 'worker-a',
      target_key: 'target_1'
    )
    lease_ids = %w[lease-a lease-b]

    worker_a_summary = SecureRandom.stub(:uuid, -> { lease_ids.shift }) { worker_a.drain_once(limit: 1) }

    assert_equal 1, worker_b_summary[:processed]
    assert_equal 0, worker_b_summary[:stale]
    assert_equal 0, worker_a_summary[:processed]
    assert_equal 1, worker_a_summary[:stale]
    assert_equal :processed, repository.delivery(101)[:status]
    assert_nil repository.delivery(101)[:lease_owner]
    assert_equal :processed, repository.parent_status(1)
  end

  def test_stale_failure_cannot_overwrite_a_newer_success
    original = event(id: 1, document_id: 'sku-1', delivery_id: 101, target_key: 'target_1')
    repository = LeaseAwareRepository.new([original])
    worker_b = SearchEngine::PostgresOutbox::Drainer.new(
      repository: repository,
      processor: RecordingProcessor.new,
      worker_id: 'worker-b',
      target_key: 'target_1'
    )
    stale_error = StandardError.new('late failure')
    worker_a_processor = CallbackProcessor.new do |events, _context|
      repository.expire_and_requeue!(events.first.delivery_id)
      worker_b.drain_once(limit: 1)
      SearchEngine::PostgresOutbox::ProcessorResult.failure(events.map(&:id), error: stale_error)
    end
    worker_a = SearchEngine::PostgresOutbox::Drainer.new(
      repository: repository,
      processor: worker_a_processor,
      worker_id: 'worker-a',
      target_key: 'target_1'
    )
    lease_ids = %w[lease-a lease-b]

    summary = SecureRandom.stub(:uuid, -> { lease_ids.shift }) { worker_a.drain_once(limit: 1) }

    assert_equal 0, summary[:retryable]
    assert_equal 0, summary[:failed]
    assert_equal 1, summary[:stale]
    assert_equal :processed, repository.delivery(101)[:status]
    assert_equal 0, repository.delivery(101)[:attempts]
    assert_nil repository.delivery(101)[:last_error]
    assert_equal :processed, repository.parent_status(1)
  end

  def test_newer_delete_waits_for_an_older_upsert_on_the_same_target_and_document
    original = event(id: 1, document_id: 'sku-1', operation: 'upsert', delivery_id: 101, target_key: 'target_1')
    repository = LeaseAwareRepository.new([original])
    operations = []
    blocked_summary = nil
    follower_processor = CallbackProcessor.new do |events, _context|
      operations.concat(events.map(&:operation))
      SearchEngine::PostgresOutbox::ProcessorResult.success(events.map(&:id))
    end
    follower = SearchEngine::PostgresOutbox::Drainer.new(
      repository: repository,
      processor: follower_processor,
      worker_id: 'follower',
      target_key: 'target_1'
    )
    leader_processor = CallbackProcessor.new do |events, _context|
      operations.concat(events.map(&:operation))
      repository.add(
        event(id: 2, document_id: 'sku-1', operation: 'delete', delivery_id: 102, target_key: 'target_1')
      )
      blocked_summary = follower.drain_once(limit: 1)
      SearchEngine::PostgresOutbox::ProcessorResult.success(events.map(&:id))
    end
    leader = SearchEngine::PostgresOutbox::Drainer.new(
      repository: repository,
      processor: leader_processor,
      worker_id: 'leader',
      target_key: 'target_1'
    )
    lease_ids = %w[lease-upsert blocked-delete lease-delete]

    SecureRandom.stub(:uuid, -> { lease_ids.shift }) do
      leader.drain_once(limit: 1)
      follower.drain_once(limit: 1)
    end

    assert_equal 0, blocked_summary[:claimed]
    assert_equal %i[upsert delete], operations
    assert_equal :processed, repository.delivery(101)[:status]
    assert_equal :processed, repository.delivery(102)[:status]
  end

  def test_recreation_upsert_waits_for_an_older_delete_on_the_same_target_and_document
    original = event(id: 1, document_id: 'sku-1', operation: 'delete', delivery_id: 101, target_key: 'target_1')
    repository = LeaseAwareRepository.new([original])
    operations = []
    blocked_summary = nil
    follower_processor = CallbackProcessor.new do |events, _context|
      operations.concat(events.map(&:operation))
      SearchEngine::PostgresOutbox::ProcessorResult.success(events.map(&:id))
    end
    follower = SearchEngine::PostgresOutbox::Drainer.new(
      repository: repository,
      processor: follower_processor,
      worker_id: 'follower',
      target_key: 'target_1'
    )
    leader_processor = CallbackProcessor.new do |events, _context|
      operations.concat(events.map(&:operation))
      repository.add(
        event(id: 2, document_id: 'sku-1', operation: 'upsert', delivery_id: 102, target_key: 'target_1')
      )
      blocked_summary = follower.drain_once(limit: 1)
      SearchEngine::PostgresOutbox::ProcessorResult.success(events.map(&:id))
    end
    leader = SearchEngine::PostgresOutbox::Drainer.new(
      repository: repository,
      processor: leader_processor,
      worker_id: 'leader',
      target_key: 'target_1'
    )
    lease_ids = %w[lease-delete blocked-upsert lease-upsert]

    SecureRandom.stub(:uuid, -> { lease_ids.shift }) do
      leader.drain_once(limit: 1)
      follower.drain_once(limit: 1)
    end

    assert_equal 0, blocked_summary[:claimed]
    assert_equal %i[delete upsert], operations
    assert_equal :processed, repository.delivery(101)[:status]
    assert_equal :processed, repository.delivery(102)[:status]
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

  def test_opted_in_search_cache_is_cleared_before_processed_events_are_acknowledged
    SearchEngine.config.postgres_outbox.clear_cache_after_write = true
    order = []
    repository = FakeRepository.new([event(id: 1, collection: 'products')])
    processor = CallbackProcessor.new do |events, _context|
      order << :processor
      SearchEngine::PostgresOutbox::ProcessorResult.success(events.map(&:id))
    end
    repository.define_singleton_method(:mark_processed!) do |events|
      order << :acknowledge
      super(events)
    end

    SearchEngine::Cache.stub(:clear, -> { order << :cache_clear }) do
      SearchEngine::DependencyPlanner.stub(:order_events, ->(input) { input }) do
        SearchEngine::PostgresOutbox::Drainer
          .new(repository: repository, processor: processor, worker_id: 'w1')
          .drain_once
      end
    end

    assert_equal %i[processor cache_clear acknowledge], order
    assert_equal [1], repository.processed_ids
  end

  def test_cache_clear_failure_retries_the_group_without_acknowledging_it
    SearchEngine.config.postgres_outbox.clear_cache_after_write = true
    cache_error = StandardError.new('cache clear failed')
    repository = FakeRepository.new([event(id: 1, collection: 'products')])
    processor = RecordingProcessor.new

    SearchEngine::Cache.stub(:clear, -> { raise cache_error }) do
      SearchEngine::DependencyPlanner.stub(:order_events, ->(input) { input }) do
        summary = SearchEngine::PostgresOutbox::Drainer
                  .new(repository: repository, processor: processor, worker_id: 'w1')
                  .drain_once

        assert_empty repository.processed_ids
        assert_equal [[[1], cache_error]], repository.retryable_calls
        assert_equal 1, summary[:retryable]
      end
    end
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

  def test_one_failed_event_does_not_prevent_99_good_siblings_from_being_acknowledged
    failed_error = StandardError.new('record 50 mapping failed')
    processed_ids = (1..100).to_a - [50]
    partial = SearchEngine::PostgresOutbox::ProcessorResult.new(
      processed_event_ids: processed_ids,
      failed_event_ids: [50],
      errors_by_event_id: { 50 => failed_error }
    )
    processor = RecordingProcessor.new('products' => partial)
    repository = FakeRepository.new((1..100).map { |id| event(id: id, collection: 'products') })

    SearchEngine::DependencyPlanner.stub(:order_events, ->(input) { input }) do
      summary = SearchEngine::PostgresOutbox::Drainer
                .new(repository: repository, processor: processor, worker_id: 'w1')
                .drain_once

      assert_equal processed_ids, repository.processed_ids
      assert_equal [[[50], failed_error]], repository.retryable_calls
      assert_equal 99, summary[:processed]
      assert_equal 1, summary[:retryable]
      assert_equal 1, summary[:failed]
      assert_equal ['products'], summary[:collections]
    end
  end

  def test_missing_result_classification_retries_entire_batch_without_acknowledging_siblings
    partial_success = SearchEngine::PostgresOutbox::ProcessorResult.success([1])

    assert_malformed_result_retries_entire_batch(
      partial_success,
      error_class: SearchEngine::PostgresOutbox::ProcessorResult::ValidationError,
      message: 'missing ids: 2'
    )
  end

  def test_duplicate_result_classification_retries_entire_batch_without_acknowledging_siblings
    result = SearchEngine::PostgresOutbox::ProcessorResult.new(
      processed_event_ids: [1, 1],
      failed_event_ids: [2],
      error: 'failed'
    )

    assert_malformed_result_retries_entire_batch(
      result,
      error_class: SearchEngine::PostgresOutbox::ProcessorResult::ValidationError,
      message: 'duplicate processed ids: 1'
    )
  end

  def test_overlapping_result_classification_retries_entire_batch_without_acknowledging_siblings
    result = SearchEngine::PostgresOutbox::ProcessorResult.new(
      processed_event_ids: [1],
      failed_event_ids: [1, 2],
      error: 'failed'
    )

    assert_malformed_result_retries_entire_batch(
      result,
      error_class: SearchEngine::PostgresOutbox::ProcessorResult::ValidationError,
      message: 'overlapping ids: 1'
    )
  end

  def test_unknown_result_classification_retries_entire_batch_without_acknowledging_siblings
    result = SearchEngine::PostgresOutbox::ProcessorResult.new(
      processed_event_ids: [1, 99],
      failed_event_ids: [2],
      error: 'failed'
    )

    assert_malformed_result_retries_entire_batch(
      result,
      error_class: SearchEngine::PostgresOutbox::ProcessorResult::ValidationError,
      message: 'unknown ids: 99'
    )
  end

  def test_non_processor_result_retries_entire_batch_without_acknowledging_siblings
    assert_malformed_result_retries_entire_batch(
      Object.new,
      error_class: TypeError,
      message: 'processor must return ProcessorResult'
    )
  end

  def test_nil_result_retries_entire_batch_without_acknowledging_siblings
    assert_malformed_result_retries_entire_batch(
      nil,
      error_class: TypeError,
      message: 'processor must return ProcessorResult'
    )
  end

  def test_retry_exhaustion_retains_bounded_event_context_without_payload
    final_attempt = SearchEngine.config.postgres_outbox.max_attempts - 1
    repository = FakeRepository.new(
      [
        event(
          id: 42,
          collection: 'products',
          document_id: 'sku-42',
          source_model_name: "#{self.class.name}::MissingSource",
          payload: { 'private_data' => 'must-not-appear-in-errors' },
          attempts: final_attempt
        )
      ]
    )

    SearchEngine::DependencyPlanner.stub(:order_events, ->(input) { input }) do
      summary = SearchEngine::PostgresOutbox::Drainer
                .new(repository: repository, worker_id: 'w1')
                .drain_once

      assert_empty repository.processed_ids
      assert_equal [[42]], repository.retryable_calls.map(&:first)
      error = repository.retryable_calls.first.last
      assert_equal [[[42], error]], repository.terminal_retryable_calls
      assert_kind_of SearchEngine::PostgresOutbox::EventProcessor::EventProcessingError, error
      assert_kind_of NameError, error.original_error
      assert_equal 42, error.event_id
      assert_equal 'products', error.collection
      assert_equal :upsert, error.operation
      assert_equal 'sku-42', error.document_id
      assert_includes error.message, 'collection=products event_id=42 operation=upsert document_id=sku-42'
      refute_includes error.message, 'must-not-appear-in-errors'
      assert_operator error.message.length, :<, 1_000
      assert_equal 0, summary[:processed]
      assert_equal 1, summary[:retryable]
      assert_equal 1, summary[:failed]
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
      context = processor.calls.first.last
      assert_equal 'w1', context[:worker_id]
      assert_equal 'target_1', context[:target_key]
      assert_match(/\Aw1:[0-9a-f-]{36}\z/, context[:lease_owner])
      assert_equal [[1]], repository.renewal_calls
    end
  end

  def test_target_claim_renews_unstarted_groups_before_each_processor
    repository = FakeRepository.new(
      [
        event(id: 1, collection: 'products', delivery_id: 101, target_key: 'target_1'),
        event(id: 2, collection: 'product_barcodes', delivery_id: 102, target_key: 'target_1')
      ]
    )
    processor = RecordingProcessor.new

    SearchEngine::DependencyPlanner.stub(:order_events, ->(input) { input }) do
      summary = SearchEngine::PostgresOutbox::Drainer
                .new(repository: repository, processor: processor, worker_id: 'w1', target_key: 'target_1')
                .drain_once(limit: 10)

      assert_equal 2, summary[:processed]
      assert_equal [[1, 2], [2]], repository.renewal_calls
      assert_equal [[1], [2]], processor.calls.map(&:first)
    end
  end

  def test_target_claim_never_processes_a_later_group_after_lease_renewal_is_lost
    repository = FakeRepository.new(
      [
        event(id: 1, collection: 'products', delivery_id: 101, target_key: 'target_1'),
        event(id: 2, collection: 'product_barcodes', delivery_id: 102, target_key: 'target_1')
      ]
    )
    repository.renewal_results = [[1, 2], []]
    processor = RecordingProcessor.new

    SearchEngine::DependencyPlanner.stub(:order_events, ->(input) { input }) do
      summary = SearchEngine::PostgresOutbox::Drainer
                .new(repository: repository, processor: processor, worker_id: 'w1', target_key: 'target_1')
                .drain_once(limit: 10)

      assert_equal 1, summary[:processed]
      assert_equal 1, summary[:stale]
      assert_equal [[1]], processor.calls.map(&:first)
      assert_equal [1], repository.processed_ids
    end
  end

  def test_target_claim_retries_later_groups_when_the_current_group_lease_is_lost
    repository = FakeRepository.new(
      [
        event(id: 1, collection: 'products', delivery_id: 101, target_key: 'target_1'),
        event(id: 2, collection: 'product_barcodes', delivery_id: 102, target_key: 'target_1')
      ]
    )
    repository.renewal_results = [[2]]
    processor = RecordingProcessor.new

    SearchEngine::DependencyPlanner.stub(:order_events, ->(input) { input }) do
      summary = SearchEngine::PostgresOutbox::Drainer
                .new(repository: repository, processor: processor, worker_id: 'w1', target_key: 'target_1')
                .drain_once(limit: 10)

      assert_empty processor.calls
      assert_empty repository.processed_ids
      assert_equal 1, summary[:stale]
      assert_equal 1, summary[:retryable]
      assert_equal 1, summary[:failed]
      assert_equal [[[2], SearchEngine::PostgresOutbox::Drainer::BLOCKED_ERROR]], repository.retryable_calls
    end
  end

  def test_target_claim_retries_current_and_later_groups_when_lease_renewal_raises
    repository = FakeRepository.new(
      [
        event(id: 1, collection: 'products', delivery_id: 101, target_key: 'target_1'),
        event(id: 2, collection: 'product_barcodes', delivery_id: 102, target_key: 'target_1')
      ]
    )
    renewal_error = StandardError.new('lease renewal unavailable')
    repository.renewal_error = renewal_error
    processor = RecordingProcessor.new

    SearchEngine::DependencyPlanner.stub(:order_events, ->(input) { input }) do
      summary = SearchEngine::PostgresOutbox::Drainer
                .new(repository: repository, processor: processor, worker_id: 'w1', target_key: 'target_1')
                .drain_once(limit: 10)

      assert_empty processor.calls
      assert_equal [[[1], renewal_error], [[2], SearchEngine::PostgresOutbox::Drainer::BLOCKED_ERROR]],
                   repository.retryable_calls
      assert_equal 2, summary[:retryable]
      assert_equal 2, summary[:failed]
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
      assert_equal 10, repository_seen.claim_args[:limit]
      assert_match(/\Aw1:[0-9a-f-]{36}\z/, repository_seen.claim_args[:worker_id])
    end
  end

  private

  def assert_malformed_result_retries_entire_batch(result, error_class:, message:)
    processor = RecordingProcessor.new('products' => result)
    repository = FakeRepository.new(
      [
        event(id: 1, collection: 'products'),
        event(id: 2, collection: 'products')
      ]
    )

    summary = SearchEngine::DependencyPlanner.stub(:order_events, ->(input) { input }) do
      SearchEngine::PostgresOutbox::Drainer
        .new(repository: repository, processor: processor, worker_id: 'w1')
        .drain_once
    end

    assert_empty repository.processed_ids
    assert_equal [[1, 2]], repository.retryable_calls.map(&:first)
    error = repository.retryable_calls.first.last
    assert_kind_of error_class, error
    assert_includes error.message, message
    assert_equal 0, summary[:processed]
    assert_equal 2, summary[:retryable]
    assert_equal 2, summary[:failed]
  end

  def event(
    id:,
    collection: 'products',
    document_id: nil,
    operation: 'upsert',
    delivery_id: nil,
    target_key: nil,
    source_model_name: 'Product',
    payload: {},
    attempts: 0
  )
    SearchEngine::PostgresOutbox::Event.new(
      id: id,
      source_table: collection,
      source_model_name: source_model_name,
      collection: collection,
      record_id: id.to_s,
      document_id: document_id || id.to_s,
      operation: operation,
      attempts: attempts,
      payload: payload,
      delivery_id: delivery_id,
      target_key: target_key
    )
  end
end
