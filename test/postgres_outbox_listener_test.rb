# frozen_string_literal: true

require 'test_helper'
require 'search_engine/postgres_outbox/listener'

class PostgresOutboxListenerTest < Minitest::Test
  class FakeDrainJob
    attr_reader :calls

    def initialize
      @calls = 0
    end

    def perform_later
      @calls += 1
    end
  end

  class FakeConnectionPool
    attr_reader :connections

    def initialize(*connections)
      @connections = connections
      @index = 0
    end

    def with_connection
      connection = @connections.fetch(@index)
      @index += 1 if @index < (@connections.size - 1)
      yield connection
    end
  end

  class FakeConnection
    attr_reader :executed

    def initialize(raw: FakeRawConnection.new, advisory_lock: nil)
      @raw = raw
      @advisory_lock = advisory_lock
      @executed = []
    end

    def execute(sql)
      @executed << sql
      true
    end

    def select_value(sql)
      @executed << sql
      @advisory_lock
    end

    def raw_connection
      @raw
    end

    def quote_table_name(name)
      %("#{name.to_s.gsub('"', '""')}")
    end
  end

  class FakeRawConnection
    def initialize(events: [])
      @events = events
    end

    def wait_for_notify(timeout)
      sleep(timeout)
      event = @events.shift
      case event&.fetch(:type, :timeout)
      when :notify
        yield(event.fetch(:channel), event.fetch(:pid, 1), event[:payload])
        true
      when :error
        raise event.fetch(:error)
      else
        false
      end
    end
  end

  def test_start_is_idempotent_and_stop_joins_thread
    drain_job = FakeDrainJob.new
    listener = build_listener(raw: FakeRawConnection.new, drain_job: drain_job)

    listener.start
    first_thread = listener.instance_variable_get(:@thread)
    listener.start

    assert_same first_thread, listener.instance_variable_get(:@thread)

    wait_until { drain_job.calls.positive? }
    listener.stop

    refute listener.running?
    assert_operator drain_job.calls, :>=, 1
  end

  def test_notification_enqueues_drain_job
    drain_job = FakeDrainJob.new
    raw = FakeRawConnection.new(
      events: [
        { type: :notify, channel: 'search_engine_outbox', payload: 'event' }
      ]
    )
    listener = build_listener(raw: raw, drain_job: drain_job)

    listener.start
    wait_until { drain_job.calls == 1 }
    listener.stop

    assert_equal 1, drain_job.calls
  end

  def test_timeout_fallback_enqueues_drain_job
    drain_job = FakeDrainJob.new
    listener = build_listener(raw: FakeRawConnection.new, drain_job: drain_job)

    listener.start
    wait_until { drain_job.calls == 1 }
    listener.stop

    assert_equal 1, drain_job.calls
  end

  def test_notification_enqueues_are_throttled_within_poll_interval
    drain_job = FakeDrainJob.new
    listener = build_listener(drain_job: drain_job, poll_interval_s: 10)

    listener.send(:enqueue_drain)
    listener.send(:enqueue_drain)

    assert_equal 1, drain_job.calls
  end

  def test_fallback_enqueue_bypasses_notification_throttle
    drain_job = FakeDrainJob.new
    listener = build_listener(drain_job: drain_job, poll_interval_s: 10)

    listener.send(:enqueue_drain)
    listener.send(:enqueue_drain, force: true)

    assert_equal 2, drain_job.calls
  end

  def test_advisory_lock_skipped_prevents_enqueue
    drain_job = FakeDrainJob.new
    sleeps = []
    connection = FakeConnection.new(advisory_lock: false)
    listener = build_listener(
      connection: connection,
      drain_job: drain_job,
      advisory_lock: true,
      sleeper: lambda do |seconds|
        sleeps << seconds
        listener.stop
      end
    )

    listener.start
    wait_until { sleeps.any? }
    listener.stop

    assert_equal 0, drain_job.calls
    assert_includes connection.executed, 'SELECT pg_try_advisory_lock(1658021551)'
  end

  def test_advisory_lock_acquired_listens_and_unlocks
    drain_job = FakeDrainJob.new
    connection = FakeConnection.new(advisory_lock: true)
    listener = build_listener(connection: connection, drain_job: drain_job, advisory_lock: true)

    listener.start
    wait_until { drain_job.calls == 1 }
    listener.stop

    assert_includes connection.executed, 'SELECT pg_try_advisory_lock(1658021551)'
    assert_includes connection.executed, 'LISTEN "search_engine_outbox"'
    assert_includes connection.executed, 'SELECT pg_advisory_unlock(1658021551)'
  end

  def test_connection_errors_are_retried
    drain_job = FakeDrainJob.new
    failing_connection = FakeConnection.new(
      raw: FakeRawConnection.new(
        events: [
          { type: :error, error: RuntimeError.new('db down') }
        ]
      )
    )
    successful_connection = FakeConnection.new
    pool = FakeConnectionPool.new(failing_connection, successful_connection)
    sleeps = []
    listener = build_listener(connection_pool: pool, drain_job: drain_job, sleeper: ->(seconds) { sleeps << seconds })

    listener.start
    wait_until { drain_job.calls == 1 }
    listener.stop

    assert_equal [0.01], sleeps
    assert_equal 1, drain_job.calls
  end

  def test_initialize_does_not_checkout_connection
    pool = Object.new

    def pool.with_connection
      raise 'should not checkout during initialize'
    end

    listener = SearchEngine::PostgresOutbox::Listener.new(connection_pool: pool)

    refute listener.running?
  end

  private

  def build_listener(
    raw: FakeRawConnection.new,
    connection: FakeConnection.new(raw: raw),
    connection_pool: FakeConnectionPool.new(connection),
    drain_job: FakeDrainJob.new,
    advisory_lock: false,
    poll_interval_s: 0.01,
    sleeper: ->(_seconds) {}
  )
    SearchEngine::PostgresOutbox::Listener.new(
      connection_pool: connection_pool,
      drain_job: drain_job,
      channel: 'search_engine_outbox',
      wait_timeout_s: 0.01,
      poll_interval_s: poll_interval_s,
      advisory_lock: advisory_lock,
      advisory_lock_key: 1_658_021_551,
      sleeper: sleeper
    )
  end

  def wait_until
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 1
    until yield
      raise 'timed out waiting for listener' if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

      sleep 0.001
    end
  end
end
