# frozen_string_literal: true

require 'test_helper'
require 'search_engine/postgres_outbox/drain_enqueuer'

class PostgresOutboxDrainEnqueuerTest < Minitest::Test
  class FakeRepository
    attr_reader :materialize_calls

    def initialize
      @materialize_calls = 0
    end

    def materialize_deliveries!
      @materialize_calls += 1
    end
  end

  class FakeDrainJob
    attr_reader :calls

    def initialize
      @calls = []
    end

    def perform_later(**kwargs)
      calls << { queue: nil, kwargs: kwargs }
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

    assert_equal 0, repository.materialize_calls
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
    repository = FakeRepository.new
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

    assert_equal 1, repository.materialize_calls
    assert_equal(
      [
        { queue: 'queue_1', kwargs: { target_key: 'target_1' } },
        { queue: 'queue_2', kwargs: { target_key: 'target_2' } }
      ],
      drain_job.calls
    )
  end

  def test_target_jobs_preserve_explicit_limit
    drain_job = FakeDrainJob.new

    enqueuer = SearchEngine::PostgresOutbox::DrainEnqueuer.new(
      drain_job: drain_job,
      targets_resolver: -> { [{ 'key' => :target_1, 'queue_name' => :queue_1 }] }
    )
    enqueuer.enqueue_all(limit: 10)

    assert_equal(
      [{ queue: 'queue_1', kwargs: { target_key: 'target_1', limit: 10 } }],
      drain_job.calls
    )
  end
end
