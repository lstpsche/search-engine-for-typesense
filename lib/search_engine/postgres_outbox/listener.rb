# frozen_string_literal: true

require 'zlib'

module SearchEngine
  module PostgresOutbox
    # Generic PostgreSQL LISTEN/NOTIFY listener that enqueues outbox drain jobs.
    class Listener
      MAX_PAYLOAD_LENGTH = 256

      # @param connection_pool [#with_connection, nil] ActiveRecord connection pool
      # @param drain_job [#perform_later] job class used to nudge outbox draining
      # @param channel [String] PostgreSQL notification channel
      # @param wait_timeout_s [Numeric] wait timeout for LISTEN notifications
      # @param poll_interval_s [Numeric] fallback/retry polling interval
      # @param advisory_lock [Boolean] whether to guard this listener with a PostgreSQL advisory lock
      # @param advisory_lock_key [Integer, nil] optional explicit advisory lock key
      # @param sleeper [#call] injectable sleep hook for tests
      def initialize(
        connection_pool: nil,
        drain_job: nil,
        channel: SearchEngine.config.postgres_outbox.channel,
        wait_timeout_s: SearchEngine.config.postgres_outbox.listener_wait_timeout_s,
        poll_interval_s: SearchEngine.config.postgres_outbox.poll_interval_s,
        advisory_lock: SearchEngine.config.postgres_outbox.advisory_lock,
        advisory_lock_key: SearchEngine.config.postgres_outbox.advisory_lock_key,
        sleeper: ->(seconds) { sleep(seconds) }
      )
        @connection_pool = connection_pool
        @drain_job = drain_job
        @channel = channel.to_s
        @wait_timeout_s = wait_timeout_s.to_f
        @poll_interval_s = poll_interval_s.to_f
        @advisory_lock = advisory_lock ? true : false
        @advisory_lock_key = advisory_lock_key
        @sleeper = sleeper
        @mutex = Mutex.new
        @stop_requested = false
        @thread = nil
      end

      # Start the listener thread.
      # @return [SearchEngine::PostgresOutbox::Listener]
      def start
        @mutex.synchronize do
          return self if running?

          @stop_requested = false
          @thread = Thread.new { run }
        end

        instrument('search_engine.postgres_outbox.listener_start') {}
        self
      end

      # Stop the listener thread and wait for it to finish.
      # @param timeout [Numeric, nil] join timeout in seconds
      # @return [SearchEngine::PostgresOutbox::Listener]
      def stop(timeout: nil)
        thread = @mutex.synchronize do
          @stop_requested = true
          @thread
        end

        thread&.join(timeout) if thread && thread != Thread.current
        instrument('search_engine.postgres_outbox.listener_stop') {}
        self
      end

      # @return [Boolean] whether the listener thread is alive
      def running?
        thread = @thread
        thread&.alive? || false
      end

      private

      attr_reader :channel, :wait_timeout_s, :poll_interval_s, :sleeper

      def run
        attempt = 0

        until stop_requested?
          begin
            run_connection_loop
            attempt = 0
          rescue StandardError => error
            attempt += 1
            sleep_s = reconnect_sleep_s(attempt)
            instrument_error(error, sleep_s)
            break if stop_requested?

            instrument('search_engine.postgres_outbox.listener_reconnect', sleep_s: sleep_s) {}
            sleep_interval(sleep_s)
          end
        end
      ensure
        @mutex.synchronize { @thread = nil if @thread == Thread.current }
      end

      def run_connection_loop
        connection_pool.with_connection do |connection|
          if advisory_lock_enabled?
            run_with_advisory_lock(connection) { listen(connection) }
          else
            listen(connection)
          end
        end
      end

      def listen(connection)
        quoted_channel = quote_channel(connection)
        connection.execute("LISTEN #{quoted_channel}")

        begin
          loop do
            break if stop_requested?

            notified = wait_for_notification(connection)
            next if notified || stop_requested?

            instrument('search_engine.postgres_outbox.listener_fallback') {}
            enqueue_drain
          end
        ensure
          connection.execute("UNLISTEN #{quoted_channel}")
        end
      end

      def wait_for_notification(connection)
        raw_connection(connection).wait_for_notify(wait_timeout_s) do |notify_channel, _pid, payload|
          next if stop_requested?

          instrument(
            'search_engine.postgres_outbox.listener_notification',
            channel: notify_channel.to_s,
            payload: truncate(payload)
          ) {}
          enqueue_drain
        end
      end

      def run_with_advisory_lock(connection)
        lock_key = advisory_lock_key
        acquired = advisory_lock_acquired?(connection, lock_key)
        unless acquired
          instrument('search_engine.postgres_outbox.listener_advisory_lock', status: 'skipped', lock_key: lock_key) {}
          sleep_interval(poll_interval_s)
          return
        end

        instrument('search_engine.postgres_outbox.listener_advisory_lock', status: 'acquired', lock_key: lock_key) {}
        yield
      ensure
        if acquired
          release_advisory_lock(connection, lock_key)
          instrument('search_engine.postgres_outbox.listener_advisory_lock', status: 'released', lock_key: lock_key) {}
        end
      end

      def advisory_lock_acquired?(connection, lock_key)
        value = select_value(connection, "SELECT pg_try_advisory_lock(#{Integer(lock_key)})")
        value == true || value.to_s == 't' || value.to_s == 'true'
      end

      def release_advisory_lock(connection, lock_key)
        connection.execute("SELECT pg_advisory_unlock(#{Integer(lock_key)})")
      end

      def select_value(connection, sql)
        return connection.select_value(sql) if connection.respond_to?(:select_value)

        connection.execute(sql)
      end

      def raw_connection(connection)
        return connection.raw_connection if connection.respond_to?(:raw_connection)

        connection
      end

      def quote_channel(connection)
        if connection.respond_to?(:quote_table_name)
          connection.quote_table_name(channel)
        else
          %("#{channel.gsub('"', '""')}")
        end
      end

      def enqueue_drain
        drain_job.perform_later
      end

      def drain_job
        @drain_job ||= SearchEngine::PostgresOutbox::DrainJob
      end

      def connection_pool
        @connection_pool ||= ::ActiveRecord::Base.connection_pool
      end

      def advisory_lock_enabled?
        @advisory_lock
      end

      def advisory_lock_key
        @advisory_lock_key || Zlib.crc32(channel)
      end

      def reconnect_sleep_s(attempt)
        [[poll_interval_s * attempt, poll_interval_s].max, [wait_timeout_s, 30].max].min
      end

      def stop_requested?
        @mutex.synchronize { @stop_requested }
      end

      def sleep_interval(seconds)
        sleeper.call(seconds) unless stop_requested?
      end

      def instrument(event, payload = {})
        SearchEngine::Instrumentation.instrument(event, { channel: channel, worker_id: Thread.current.object_id }.merge(payload)) {}
      end

      def instrument_error(error, sleep_s)
        instrument(
          'search_engine.postgres_outbox.listener_error',
          error_class: error.class.name,
          message_truncated: truncate(error.message),
          sleep_s: sleep_s
        ) {}
      end

      def truncate(value)
        text = value.to_s
        return text if text.length <= MAX_PAYLOAD_LENGTH

        text[0, MAX_PAYLOAD_LENGTH]
      end
    end
  end
end
