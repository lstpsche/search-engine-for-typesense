# frozen_string_literal: true

require 'test_helper'
require 'active_job'
require_relative '../app/search_engine/search_engine/postgres_outbox/drain_job'

class PostgresOutboxDrainJobTest < Minitest::Test
  class FakeDrainer
    attr_reader :calls

    def initialize(summary: { claimed: 0, processed: 0 })
      @calls = []
      @summary = summary
    end

    def drain_once(*args, **kwargs)
      calls << [args, kwargs]
      @summary
    end
  end

  def setup
    @previous_adapter = ActiveJob::Base.queue_adapter
    @previous_enabled = SearchEngine.config.postgres_outbox.enabled
    @previous_queue_name = SearchEngine.config.postgres_outbox.queue_name
    @previous_batch_size = SearchEngine.config.postgres_outbox.batch_size
  end

  def teardown
    ActiveJob::Base.queue_adapter = @previous_adapter
    SearchEngine.config.postgres_outbox.enabled = @previous_enabled
    SearchEngine.config.postgres_outbox.queue_name = @previous_queue_name
    SearchEngine.config.postgres_outbox.batch_size = @previous_batch_size
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
end
