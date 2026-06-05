# frozen_string_literal: true

require 'test_helper'
require 'active_job'
require_relative '../app/search_engine/search_engine/postgres_outbox/drain_job'

class PostgresOutboxDrainJobTest < Minitest::Test
  class FakeDrainer
    attr_reader :calls

    def initialize(summary: { claimed: 0, processed: 0 })
      @calls = []
      @summaries = summary.is_a?(Array) ? summary : [summary]
    end

    def drain_once(*args, **kwargs)
      calls << [args, kwargs]
      @summaries[[calls.size - 1, @summaries.size - 1].min]
    end
  end

  class FakeSlotRepository
    attr_reader :started_slots, :released_slots, :requeued_slots

    def initialize(start_result: true, drain_slots_table_exists: false)
      @start_result = start_result
      @drain_slots_table_exists = drain_slots_table_exists
      @started_slots = []
      @released_slots = []
      @requeued_slots = []
    end

    def start_drain_slot!(target_key:, slot:, worker_id:)
      started_slots << { target_key: target_key, slot: slot, worker_id: worker_id }
      @start_result
    end

    def requeue_drain_slot!(target_key:, slot:, worker_id:)
      requeued_slots << { target_key: target_key, slot: slot, worker_id: worker_id }
      true
    end

    def release_drain_slot!(target_key:, slot:, worker_id: nil, error: nil)
      released_slots << { target_key: target_key, slot: slot, worker_id: worker_id, error: error }
    end

    def drain_slots_table_exists?
      @drain_slots_table_exists
    end
  end

  def setup
    @previous_adapter = ActiveJob::Base.queue_adapter
    @previous_enabled = SearchEngine.config.postgres_outbox.enabled
    @previous_queue_name = SearchEngine.config.postgres_outbox.queue_name
    @previous_batch_size = SearchEngine.config.postgres_outbox.batch_size
    @previous_delivery_targets = SearchEngine.config.postgres_outbox.delivery_targets
    @previous_drain_job_max_batches = SearchEngine.config.postgres_outbox.drain_job_max_batches
    @previous_drain_job_max_runtime_s = SearchEngine.config.postgres_outbox.drain_job_max_runtime_s
    SearchEngine.config.postgres_outbox.delivery_targets = -> { [] }
  end

  def teardown
    ActiveJob::Base.queue_adapter = @previous_adapter
    SearchEngine.config.postgres_outbox.enabled = @previous_enabled
    SearchEngine.config.postgres_outbox.queue_name = @previous_queue_name
    SearchEngine.config.postgres_outbox.batch_size = @previous_batch_size
    SearchEngine.config.postgres_outbox.delivery_targets = @previous_delivery_targets
    SearchEngine.config.postgres_outbox.drain_job_max_batches = @previous_drain_job_max_batches
    SearchEngine.config.postgres_outbox.drain_job_max_runtime_s = @previous_drain_job_max_runtime_s
  end

  def test_disabled_outbox_returns_without_instantiating_drainer
    SearchEngine.config.postgres_outbox.enabled = false
    constructor_called = false

    SearchEngine::PostgresOutbox::Drainer.stub(:new, -> { constructor_called = true }) do
      assert_nil SearchEngine::PostgresOutbox::DrainJob.new.perform
    end

    refute constructor_called
  end

  def test_enabled_outbox_drains_once_without_limit_when_omitted
    SearchEngine.config.postgres_outbox.enabled = true
    SearchEngine.config.postgres_outbox.batch_size = 1000
    drainer = FakeDrainer.new

    SearchEngine::PostgresOutbox::Drainer.stub(:new, drainer) do
      assert_equal({ claimed: 0, processed: 0 }, SearchEngine::PostgresOutbox::DrainJob.new.perform)
    end

    assert_equal [[[], { limit: 1000 }]], drainer.calls
  end

  def test_enabled_outbox_passes_explicit_limit
    SearchEngine.config.postgres_outbox.enabled = true
    drainer = FakeDrainer.new

    SearchEngine::PostgresOutbox::Drainer.stub(:new, drainer) do
      SearchEngine::PostgresOutbox::DrainJob.new.perform(limit: 25)
    end

    assert_equal [[[], { limit: 25 }]], drainer.calls
  end

  def test_target_key_is_passed_to_drainer
    SearchEngine.config.postgres_outbox.enabled = true
    drainer = FakeDrainer.new
    constructor_args = []

    constructor = lambda do |**kwargs|
      constructor_args << kwargs
      drainer
    end

    SearchEngine::PostgresOutbox::Drainer.stub(:new, constructor) do
      SearchEngine::PostgresOutbox::DrainJob.new.perform(limit: 25, target_key: :target_1)
    end

    assert_equal [{ target_key: :target_1 }], constructor_args
    assert_equal [[[], { limit: 25 }]], drainer.calls
  end

  def test_no_arg_job_enqueues_target_drains_when_delivery_targets_are_configured
    SearchEngine.config.postgres_outbox.enabled = true
    SearchEngine.config.postgres_outbox.delivery_targets = lambda do
      [
        SearchEngine::PostgresOutbox::DeliveryTarget.new(key: 'target_1', queue_name: 'queue_1'),
        SearchEngine::PostgresOutbox::DeliveryTarget.new(key: 'target_2', queue_name: 'queue_2')
      ]
    end
    drainer_called = false
    enqueued_limit = :unset

    SearchEngine::PostgresOutbox::Drainer.stub(:new, -> { drainer_called = true }) do
      SearchEngine::PostgresOutbox::DrainEnqueuer.stub(:enqueue_all, ->(limit: nil) { enqueued_limit = limit }) do
        assert_equal(
          { claimed: 0, processed: 0, enqueued_targets: 2 },
          SearchEngine::PostgresOutbox::DrainJob.new.perform(limit: 25)
        )
      end
    end

    refute drainer_called
    assert_equal 25, enqueued_limit
  end

  def test_enqueues_continuation_when_full_default_batch_is_claimed
    ActiveJob::Base.queue_adapter = :test
    ActiveJob::Base.queue_adapter.enqueued_jobs.clear
    SearchEngine.config.postgres_outbox.enabled = true
    SearchEngine.config.postgres_outbox.batch_size = 10
    drainer = FakeDrainer.new(summary: { claimed: 10, processed: 10 })

    SearchEngine::PostgresOutbox::Drainer.stub(:new, drainer) do
      SearchEngine::PostgresOutbox::DrainJob.new.perform
    end

    assert_equal 1, ActiveJob::Base.queue_adapter.enqueued_jobs.size
    assert_empty ActiveJob::Base.queue_adapter.enqueued_jobs.first[:args]
  end

  def test_enqueues_continuation_with_explicit_limit_when_full_batch_is_claimed
    ActiveJob::Base.queue_adapter = :test
    ActiveJob::Base.queue_adapter.enqueued_jobs.clear
    SearchEngine.config.postgres_outbox.enabled = true
    drainer = FakeDrainer.new(summary: { claimed: 25, processed: 25 })

    SearchEngine::PostgresOutbox::Drainer.stub(:new, drainer) do
      SearchEngine::PostgresOutbox::DrainJob.new.perform(limit: 25)
    end

    assert_equal 1, ActiveJob::Base.queue_adapter.enqueued_jobs.size
    assert_equal({ 'limit' => 25, '_aj_ruby2_keywords' => ['limit'] }, ActiveJob::Base.queue_adapter.enqueued_jobs.first[:args].first)
  end

  def test_target_continuation_uses_target_queue_without_limit
    ActiveJob::Base.queue_adapter = :test
    ActiveJob::Base.queue_adapter.enqueued_jobs.clear
    SearchEngine.config.postgres_outbox.enabled = true
    SearchEngine.config.postgres_outbox.batch_size = 10
    SearchEngine.config.postgres_outbox.delivery_targets = lambda do
      [{ key: :target_1, queue_name: :target_queue }]
    end
    drainer = FakeDrainer.new(summary: { claimed: 10, processed: 10 })
    slot_repository = FakeSlotRepository.new

    SearchEngine::PostgresOutbox::Repository.stub(:new, slot_repository) do
      SearchEngine::PostgresOutbox::Drainer.stub(:new, drainer) do
        SearchEngine::PostgresOutbox::DrainJob.new.perform(target_key: :target_1)
      end
    end

    job = ActiveJob::Base.queue_adapter.enqueued_jobs.first
    assert_equal 1, ActiveJob::Base.queue_adapter.enqueued_jobs.size
    assert_equal 'target_queue', job[:queue]
    assert_equal({ 'target_key' => 'target_1', '_aj_ruby2_keywords' => ['target_key'] }, job[:args].first)
  end

  def test_target_continuation_preserves_explicit_limit
    ActiveJob::Base.queue_adapter = :test
    ActiveJob::Base.queue_adapter.enqueued_jobs.clear
    SearchEngine.config.postgres_outbox.enabled = true
    SearchEngine.config.postgres_outbox.delivery_targets = lambda do
      [SearchEngine::PostgresOutbox::DeliveryTarget.new(key: 'target_1', queue_name: 'target_queue')]
    end
    drainer = FakeDrainer.new(summary: { claimed: 25, processed: 25 })
    slot_repository = FakeSlotRepository.new

    SearchEngine::PostgresOutbox::Repository.stub(:new, slot_repository) do
      SearchEngine::PostgresOutbox::Drainer.stub(:new, drainer) do
        SearchEngine::PostgresOutbox::DrainJob.new.perform(limit: 25, target_key: 'target_1')
      end
    end

    job = ActiveJob::Base.queue_adapter.enqueued_jobs.first
    assert_equal 1, ActiveJob::Base.queue_adapter.enqueued_jobs.size
    assert_equal 'target_queue', job[:queue]
    assert_equal(
      { 'target_key' => 'target_1', 'limit' => 25, '_aj_ruby2_keywords' => %w[target_key limit] },
      job[:args].first
    )
  end

  def test_target_continuation_uses_explicit_continue_signal_for_partial_batches
    ActiveJob::Base.queue_adapter = :test
    ActiveJob::Base.queue_adapter.enqueued_jobs.clear
    SearchEngine.config.postgres_outbox.enabled = true
    SearchEngine.config.postgres_outbox.delivery_targets = lambda do
      [SearchEngine::PostgresOutbox::DeliveryTarget.new(key: 'target_1', queue_name: 'target_queue')]
    end
    drainer = FakeDrainer.new(summary: { claimed: 3, processed: 3, continue: true })
    slot_repository = FakeSlotRepository.new

    SearchEngine::PostgresOutbox::Repository.stub(:new, slot_repository) do
      SearchEngine::PostgresOutbox::Drainer.stub(:new, drainer) do
        SearchEngine::PostgresOutbox::DrainJob.new.perform(limit: 25, target_key: 'target_1')
      end
    end

    job = ActiveJob::Base.queue_adapter.enqueued_jobs.first
    assert_equal 1, ActiveJob::Base.queue_adapter.enqueued_jobs.size
    assert_equal 'target_queue', job[:queue]
    assert_equal(
      { 'target_key' => 'target_1', 'limit' => 25, '_aj_ruby2_keywords' => %w[target_key limit] },
      job[:args].first
    )
  end

  def test_slot_aware_job_releases_slot_when_no_work_remains
    ActiveJob::Base.queue_adapter = :test
    ActiveJob::Base.queue_adapter.enqueued_jobs.clear
    SearchEngine.config.postgres_outbox.enabled = true
    SearchEngine.config.postgres_outbox.delivery_targets = lambda do
      [{ key: :target_1, queue_name: :target_queue }]
    end
    slot_repository = FakeSlotRepository.new
    drainer = FakeDrainer.new(summary: { claimed: 0, processed: 0, collections: [] })

    SearchEngine::PostgresOutbox::Repository.stub(:new, slot_repository) do
      SearchEngine::PostgresOutbox::Drainer.stub(:new, drainer) do
        summary = SearchEngine::PostgresOutbox::DrainJob.new.perform(target_key: :target_1, drain_slot: 2)

        assert_equal 0, summary[:claimed]
        assert_equal 2, summary[:drain_slot]
      end
    end

    assert_equal 1, slot_repository.started_slots.size
    assert_equal 'target_1', slot_repository.started_slots.first.fetch(:target_key)
    assert_equal 2, slot_repository.started_slots.first.fetch(:slot)
    assert_equal 1, slot_repository.released_slots.size
    released_slot = slot_repository.released_slots.first
    assert_equal 'target_1', released_slot.fetch(:target_key)
    assert_equal 2, released_slot.fetch(:slot)
    assert_match(/:.+:/, released_slot.fetch(:worker_id))
    assert_nil released_slot.fetch(:error)
    assert_empty ActiveJob::Base.queue_adapter.enqueued_jobs
  end

  def test_slot_aware_job_skips_stale_slot_when_start_fails
    ActiveJob::Base.queue_adapter = :test
    ActiveJob::Base.queue_adapter.enqueued_jobs.clear
    SearchEngine.config.postgres_outbox.enabled = true
    SearchEngine.config.postgres_outbox.delivery_targets = lambda do
      [{ key: :target_1, queue_name: :target_queue }]
    end
    slot_repository = FakeSlotRepository.new(start_result: false)
    drainer = FakeDrainer.new(summary: { claimed: 3, processed: 3, collections: [] })

    SearchEngine::PostgresOutbox::Repository.stub(:new, slot_repository) do
      SearchEngine::PostgresOutbox::Drainer.stub(:new, drainer) do
        summary = SearchEngine::PostgresOutbox::DrainJob.new.perform(target_key: :target_1, drain_slot: 2)

        assert_equal 0, summary[:claimed]
        assert_equal true, summary[:stale_slot]
        assert_equal 0, summary[:batches]
      end
    end

    assert_equal 1, slot_repository.started_slots.size
    assert_empty slot_repository.released_slots
    assert_empty slot_repository.requeued_slots
    assert_empty drainer.calls
    assert_empty ActiveJob::Base.queue_adapter.enqueued_jobs
  end

  def test_slot_aware_job_reenqueues_same_slot_when_loop_budget_ends_with_more_work
    ActiveJob::Base.queue_adapter = :test
    ActiveJob::Base.queue_adapter.enqueued_jobs.clear
    SearchEngine.config.postgres_outbox.enabled = true
    SearchEngine.config.postgres_outbox.drain_job_max_batches = 2
    SearchEngine.config.postgres_outbox.delivery_targets = lambda do
      [{ key: :target_1, queue_name: :target_queue }]
    end
    slot_repository = FakeSlotRepository.new
    drainer = FakeDrainer.new(
      summary: [
        { claimed: 3, processed: 3, continue: true, collections: ['products'] },
        { claimed: 3, processed: 3, continue: true, collections: ['products'] }
      ]
    )

    SearchEngine::PostgresOutbox::Repository.stub(:new, slot_repository) do
      SearchEngine::PostgresOutbox::Drainer.stub(:new, drainer) do
        summary = SearchEngine::PostgresOutbox::DrainJob.new.perform(limit: 3, target_key: :target_1, drain_slot: 2)

        assert_equal 6, summary[:claimed]
        assert_equal 2, summary[:batches]
      end
    end

    assert_empty slot_repository.released_slots
    assert_requeued_slot(slot_repository, target_key: 'target_1', slot: 2)
    assert_equal 2, drainer.calls.size
    assert_enqueued_slot_job(queue: 'target_queue', target_key: 'target_1', slot: 2, limit: 3)
  end

  def test_slot_aware_job_releases_slot_on_error
    SearchEngine.config.postgres_outbox.enabled = true
    SearchEngine.config.postgres_outbox.delivery_targets = lambda do
      [{ key: :target_1, queue_name: :target_queue }]
    end
    slot_repository = FakeSlotRepository.new
    error = RuntimeError.new('boom')

    SearchEngine::PostgresOutbox::Repository.stub(:new, slot_repository) do
      SearchEngine::PostgresOutbox::Drainer.stub(:new, ->(**) { raise error }) do
        raised = assert_raises(RuntimeError) do
          SearchEngine::PostgresOutbox::DrainJob.new.perform(target_key: :target_1, drain_slot: 2)
        end
        assert_same error, raised
      end
    end

    assert_equal 1, slot_repository.released_slots.size
    assert_equal error, slot_repository.released_slots.first.fetch(:error)
    assert_match(/:.+:/, slot_repository.released_slots.first.fetch(:worker_id))
  end

  def test_legacy_target_continuation_uses_slot_aware_enqueuer_when_slots_exist
    ActiveJob::Base.queue_adapter = :test
    ActiveJob::Base.queue_adapter.enqueued_jobs.clear
    SearchEngine.config.postgres_outbox.enabled = true
    SearchEngine.config.postgres_outbox.delivery_targets = lambda do
      [{ key: :target_1, queue_name: :target_queue }]
    end
    slot_repository = FakeSlotRepository.new(drain_slots_table_exists: true)
    drainer = FakeDrainer.new(summary: { claimed: 25, processed: 25 })
    enqueued_limit = :unset

    SearchEngine::PostgresOutbox::Repository.stub(:new, slot_repository) do
      SearchEngine::PostgresOutbox::Drainer.stub(:new, drainer) do
        SearchEngine::PostgresOutbox::DrainEnqueuer.stub(:enqueue_all, ->(limit: nil) { enqueued_limit = limit }) do
          SearchEngine::PostgresOutbox::DrainJob.new.perform(limit: 25, target_key: :target_1)
        end
      end
    end

    assert_equal 25, enqueued_limit
    assert_empty ActiveJob::Base.queue_adapter.enqueued_jobs
  end

  def test_target_continuation_raises_when_target_is_not_configured
    SearchEngine.config.postgres_outbox.enabled = true
    SearchEngine.config.postgres_outbox.batch_size = 10
    SearchEngine.config.postgres_outbox.delivery_targets = -> { [] }
    drainer = FakeDrainer.new(summary: { claimed: 10, processed: 10 })
    slot_repository = FakeSlotRepository.new

    error = assert_raises(ArgumentError) do
      SearchEngine::PostgresOutbox::Repository.stub(:new, slot_repository) do
        SearchEngine::PostgresOutbox::Drainer.stub(:new, drainer) do
          SearchEngine::PostgresOutbox::DrainJob.new.perform(target_key: 'missing')
        end
      end
    end

    assert_equal 'unknown postgres outbox delivery target: missing', error.message
  end

  def test_does_not_enqueue_continuation_when_partial_batch_is_claimed
    ActiveJob::Base.queue_adapter = :test
    ActiveJob::Base.queue_adapter.enqueued_jobs.clear
    SearchEngine.config.postgres_outbox.enabled = true
    drainer = FakeDrainer.new(summary: { claimed: 24, processed: 24 })

    SearchEngine::PostgresOutbox::Drainer.stub(:new, drainer) do
      SearchEngine::PostgresOutbox::DrainJob.new.perform(limit: 25)
    end

    assert_empty ActiveJob::Base.queue_adapter.enqueued_jobs
  end

  def test_perform_later_uses_configured_queue_name
    ActiveJob::Base.queue_adapter = :test
    ActiveJob::Base.queue_adapter.enqueued_jobs.clear
    SearchEngine.config.postgres_outbox.queue_name = 'critical_search'

    job = SearchEngine::PostgresOutbox::DrainJob.perform_later

    assert_kind_of ActiveJob::Base, job
    assert_equal 1, ActiveJob::Base.queue_adapter.enqueued_jobs.size
    assert_equal 'critical_search', ActiveJob::Base.queue_adapter.enqueued_jobs.first[:queue]
  end

  private

  def assert_requeued_slot(repository, target_key:, slot:)
    assert_equal 1, repository.requeued_slots.size
    requeued_slot = repository.requeued_slots.first
    assert_equal target_key, requeued_slot.fetch(:target_key)
    assert_equal slot, requeued_slot.fetch(:slot)
    assert_match(/:.+:/, requeued_slot.fetch(:worker_id))
  end

  def assert_enqueued_slot_job(queue:, target_key:, slot:, limit:)
    job = ActiveJob::Base.queue_adapter.enqueued_jobs.first
    assert_equal 1, ActiveJob::Base.queue_adapter.enqueued_jobs.size
    assert_equal queue, job[:queue]
    assert_equal(
      { 'target_key' => target_key, 'drain_slot' => slot, 'limit' => limit,
        '_aj_ruby2_keywords' => %w[target_key drain_slot limit] },
      job[:args].first
    )
  end
end
