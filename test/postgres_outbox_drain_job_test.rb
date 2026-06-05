# frozen_string_literal: true

require 'test_helper'
require 'active_job'
require_relative '../app/search_engine/search_engine/postgres_outbox/drain_job'

class PostgresOutboxDrainJobTest < Minitest::Test
  class FakeDrainer
    attr_reader :calls

    def initialize
      @calls = []
    end

    def drain_once(*args, **kwargs)
      calls << [args, kwargs]
      { processed: 0 }
    end
  end

  def setup
    @previous_adapter = ActiveJob::Base.queue_adapter
    @previous_enabled = SearchEngine.config.postgres_outbox.enabled
    @previous_queue_name = SearchEngine.config.postgres_outbox.queue_name
  end

  def teardown
    ActiveJob::Base.queue_adapter = @previous_adapter
    SearchEngine.config.postgres_outbox.enabled = @previous_enabled
    SearchEngine.config.postgres_outbox.queue_name = @previous_queue_name
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
    drainer = FakeDrainer.new

    SearchEngine::PostgresOutbox::Drainer.stub(:new, drainer) do
      assert_equal({ processed: 0 }, SearchEngine::PostgresOutbox::DrainJob.new.perform)
    end

    assert_equal [[[], {}]], drainer.calls
  end

  def test_enabled_outbox_passes_explicit_limit
    SearchEngine.config.postgres_outbox.enabled = true
    drainer = FakeDrainer.new

    SearchEngine::PostgresOutbox::Drainer.stub(:new, drainer) do
      SearchEngine::PostgresOutbox::DrainJob.new.perform(limit: 25)
    end

    assert_equal [[[], { limit: 25 }]], drainer.calls
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
