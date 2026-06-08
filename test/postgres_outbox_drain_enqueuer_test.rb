# frozen_string_literal: true

require 'test_helper'
require 'search_engine/postgres_outbox/drain_enqueuer'

class PostgresOutboxDrainEnqueuerTest < Minitest::Test
  class FakeRepository
    attr_reader :materialize_calls, :acquire_calls, :released_requeued_slots

    def initialize(drain_slots_table_exists: false, acquired_slots: [])
      @drain_slots_table_exists = drain_slots_table_exists
      @acquired_slots = acquired_slots
      @materialize_calls = []
      @acquire_calls = []
      @released_requeued_slots = []
    end

    def materialize_deliveries!(limit: nil)
      @materialize_calls << { limit: limit }
    end

    def drain_slots_table_exists?
      @drain_slots_table_exists
    end

    def acquire_drain_slots!(targets:)
      @acquire_calls << targets
      @acquired_slots
    end

    def release_requeued_drain_slot!(target_key:, slot:, error: nil)
      released_requeued_slots << { target_key: target_key, slot: slot, error: error }
    end
  end

  class FakeDrainJob
    attr_reader :calls

    def initialize
      @calls = []
      @error = nil
    end

    def perform_later(**kwargs)
      calls << { queue: nil, kwargs: kwargs }
    end

    def raise_on_enqueue!(error)
      @error = error
    end

    def enqueue_error
      @error
    end

    def set(queue:)
      QueuedFakeDrainJob.new(self, queue)
    end
  end

  class QueuedFakeDrainJob
    def initialize(job, queue)
      @job = job
      @queue = queue
    end

    def perform_later(**kwargs)
      raise @job.enqueue_error if @job.enqueue_error

      @job.calls << { queue: @queue, kwargs: kwargs }
    end
  end

  def setup
    @previous_delivery_targets = SearchEngine.config.postgres_outbox.delivery_targets
    SearchEngine.config.postgres_outbox.delivery_targets = -> { [] }
  end

  def teardown
    SearchEngine.config.postgres_outbox.delivery_targets = @previous_delivery_targets
  end

  def test_no_targets_enqueues_one_legacy_job_without_materializing_deliveries
    repository = FakeRepository.new
    drain_job = FakeDrainJob.new

    enqueuer = SearchEngine::PostgresOutbox::DrainEnqueuer.new(repository: repository, drain_job: drain_job)
    enqueuer.enqueue_all

    assert_empty repository.materialize_calls
    assert_equal [{ queue: nil, kwargs: {} }], drain_job.calls
  end

  def test_initialize_does_not_resolve_default_drain_job
    enqueuer = SearchEngine::PostgresOutbox::DrainEnqueuer.new(repository: FakeRepository.new)

    assert_instance_of SearchEngine::PostgresOutbox::DrainEnqueuer, enqueuer
  end

  def test_no_targets_passes_explicit_limit_to_legacy_job
    drain_job = FakeDrainJob.new

    enqueuer = SearchEngine::PostgresOutbox::DrainEnqueuer.new(drain_job: drain_job)
    enqueuer.enqueue_all(limit: 25)

    assert_equal [{ queue: nil, kwargs: { limit: 25 } }], drain_job.calls
  end

  def test_targets_materialize_once_and_enqueue_one_job_per_target_queue
    repository = FakeRepository.new(drain_slots_table_exists: false)
    drain_job = FakeDrainJob.new
    targets = [
      { key: :target_1, queue_name: :queue_1 },
      SearchEngine::PostgresOutbox::DeliveryTarget.new(key: 'target_2', queue_name: 'queue_2')
    ]

    enqueuer = SearchEngine::PostgresOutbox::DrainEnqueuer.new(
      repository: repository,
      drain_job: drain_job,
      targets_resolver: -> { targets }
    )
    enqueuer.enqueue_all

    assert_equal [{ limit: nil }], repository.materialize_calls
    assert_equal(
      [
        { queue: 'queue_1', kwargs: { target_key: 'target_1' } },
        { queue: 'queue_2', kwargs: { target_key: 'target_2' } }
      ],
      drain_job.calls
    )
  end

  def test_target_jobs_preserve_explicit_limit
    repository = FakeRepository.new(drain_slots_table_exists: false)
    drain_job = FakeDrainJob.new

    enqueuer = SearchEngine::PostgresOutbox::DrainEnqueuer.new(
      repository: repository,
      drain_job: drain_job,
      targets_resolver: -> { [{ 'key' => :target_1, 'queue_name' => :queue_1 }] }
    )
    enqueuer.enqueue_all(limit: 10)

    assert_equal [{ limit: 10 }], repository.materialize_calls
    assert_equal(
      [{ queue: 'queue_1', kwargs: { target_key: 'target_1', limit: 10 } }],
      drain_job.calls
    )
  end

  def test_targets_enqueue_acquired_drain_slots_when_slot_table_exists
    repository = FakeRepository.new(
      drain_slots_table_exists: true,
      acquired_slots: [
        { target_key: 'target_1', slot: 1, queue_name: 'queue_1' },
        { target_key: 'target_1', slot: 2, queue_name: 'queue_1' }
      ]
    )
    drain_job = FakeDrainJob.new
    targets = [{ key: :target_1, queue_name: :queue_1, parallelism: 2 }]

    enqueuer = SearchEngine::PostgresOutbox::DrainEnqueuer.new(
      repository: repository,
      drain_job: drain_job,
      targets_resolver: -> { targets }
    )
    enqueuer.enqueue_all(limit: 10)

    assert_equal [{ limit: 10 }], repository.materialize_calls
    assert_equal 1, repository.acquire_calls.size
    assert_equal ['target_1'], repository.acquire_calls.first.map(&:key)
    assert_equal(
      [
        { queue: 'queue_1', kwargs: { target_key: 'target_1', drain_slot: 1, limit: 10 } },
        { queue: 'queue_1', kwargs: { target_key: 'target_1', drain_slot: 2, limit: 10 } }
      ],
      drain_job.calls
    )
  end

  def test_targets_release_acquired_drain_slot_when_enqueue_fails
    repository = FakeRepository.new(
      drain_slots_table_exists: true,
      acquired_slots: [
        { target_key: 'target_1', slot: 1, queue_name: 'queue_1' }
      ]
    )
    error = RuntimeError.new('queue down')
    drain_job = FakeDrainJob.new
    drain_job.raise_on_enqueue!(error)
    targets = [{ key: :target_1, queue_name: :queue_1, parallelism: 1 }]

    enqueuer = SearchEngine::PostgresOutbox::DrainEnqueuer.new(
      repository: repository,
      drain_job: drain_job,
      targets_resolver: -> { targets }
    )

    raised = assert_raises(RuntimeError) { enqueuer.enqueue_all(limit: 10) }

    assert_same error, raised
    assert_equal(
      [{ target_key: 'target_1', slot: 1, error: error }],
      repository.released_requeued_slots
    )
    assert_empty drain_job.calls
  end

  def test_targets_skip_enqueue_when_all_drain_slots_are_occupied
    repository = FakeRepository.new(drain_slots_table_exists: true, acquired_slots: [])
    drain_job = FakeDrainJob.new

    enqueuer = SearchEngine::PostgresOutbox::DrainEnqueuer.new(
      repository: repository,
      drain_job: drain_job,
      targets_resolver: -> { [{ key: :target_1, queue_name: :queue_1, parallelism: 2 }] }
    )
    enqueuer.enqueue_all

    assert_equal [{ limit: nil }], repository.materialize_calls
    assert_equal 1, repository.acquire_calls.size
    assert_empty drain_job.calls
  end

  def test_targets_fall_back_to_target_jobs_when_slot_table_is_missing
    repository = FakeRepository.new(drain_slots_table_exists: false)
    drain_job = FakeDrainJob.new

    enqueuer = SearchEngine::PostgresOutbox::DrainEnqueuer.new(
      repository: repository,
      drain_job: drain_job,
      targets_resolver: -> { [{ key: :target_1, queue_name: :queue_1, parallelism: 2 }] }
    )
    enqueuer.enqueue_all

    assert_empty repository.acquire_calls
    assert_equal [{ queue: 'queue_1', kwargs: { target_key: 'target_1' } }], drain_job.calls
  end
end
