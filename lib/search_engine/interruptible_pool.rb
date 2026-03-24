# frozen_string_literal: true

module SearchEngine
  # Wraps Concurrent::FixedThreadPool execution with proper Interrupt handling.
  # Ensures pool threads are killed promptly on Ctrl+C instead of waiting
  # up to the full graceful-shutdown timeout.
  module InterruptiblePool
    GRACEFUL_TIMEOUT = 3600
    KILL_TIMEOUT = 10
    CLEANUP_TIMEOUT = 60

    # Execute a block that posts work to a thread pool, then wait for completion.
    #
    # Normal path: graceful shutdown → long wait → :ok.
    # Timeout:     graceful wait expires → pool.kill → :timed_out.
    # Interrupt:   on_interrupt callback → pool.kill → short wait → re-raise.
    # Other error: ensure kill → short wait.
    #
    # @param pool [Concurrent::FixedThreadPool]
    # @param on_interrupt [Proc, nil] callback invoked before killing the pool
    # @param timeout [Integer, nil] override for graceful-shutdown timeout (seconds);
    #   defaults to {GRACEFUL_TIMEOUT} when nil
    # @yield block that posts work to the pool
    # @return [Symbol] :ok on clean completion, :timed_out when the graceful timeout was exceeded
    def self.run(pool, on_interrupt: nil, timeout: nil)
      yield
      pool.shutdown
      effective_timeout = timeout || GRACEFUL_TIMEOUT
      completed = pool.wait_for_termination(effective_timeout)
      unless completed
        pool.kill
        pool.wait_for_termination(CLEANUP_TIMEOUT)
        return :timed_out
      end
      :ok
    rescue Interrupt
      on_interrupt&.call
      pool.kill
      pool.wait_for_termination(KILL_TIMEOUT)
      raise
    ensure
      unless pool.shutdown?
        pool.kill
        pool.wait_for_termination(KILL_TIMEOUT)
      end
    end
  end
end
