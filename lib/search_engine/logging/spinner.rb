# frozen_string_literal: true

require 'search_engine/logging/cursor_guard'

module SearchEngine
  module Logging
    # Animated braille spinner for long-running terminal operations.
    #
    # Runs a background thread that cycles through braille spinner frames,
    # overwriting the current line at a fixed interval. Thread-safe text
    # updates via {#update}. Hides the cursor while spinning and guarantees
    # restore via {CursorGuard}.
    #
    # Only useful on TTY -- callers should gate on {Color.enabled?}.
    #
    # @example
    #   spinner = Spinner.new
    #   spinner.start('Schema — creating')
    #   # ... long work ...
    #   spinner.stop
    #
    # @since M9
    class Spinner
      FRAMES   = %w[⠋ ⠙ ⠹ ⠸ ⠼ ⠴ ⠦ ⠧ ⠇ ⠏].freeze
      INTERVAL = 0.08 # seconds per frame

      # @param io [IO] output stream (defaults to $stdout)
      def initialize(io: $stdout)
        @io = io
        @mutex = Mutex.new
        @stop_signal = ConditionVariable.new
        @thread = nil
        @running = false
        @text = nil
      end

      # Begin spinning with the given text. Hides the cursor.
      #
      # @param text [String] text displayed after the spinner frame
      # @return [void]
      def start(text)
        @mutex.synchronize do
          return if @running

          @text = text
          @running = true
          CursorGuard.hide(@io)
          @thread = Thread.new { spin_loop }
        end
      end

      # Stop spinning, join the thread, and restore the cursor.
      #
      # @return [void]
      def stop
        @mutex.synchronize do
          return unless @running

          @running = false
          @stop_signal.signal
        end
        @thread&.join
        @thread = nil
        CursorGuard.show(@io)
      end

      # Change the text while spinning (thread-safe).
      #
      # @param text [String]
      # @return [void]
      def update(text)
        @mutex.synchronize { @text = text }
      end

      # @return [Boolean]
      def running?
        @mutex.synchronize { @running }
      end

      private

      def spin_loop
        frame_idx = 0
        loop do
          text = @mutex.synchronize do
            break unless @running

            @stop_signal.wait(@mutex, INTERVAL)
            break unless @running

            @text
          end
          break if text.nil?

          render(FRAMES[frame_idx % FRAMES.size], text)
          frame_idx += 1
        end
      end

      def render(frame, text)
        @io.write("\r\e[K#{frame} #{text}")
        @io.flush
      end
    end
  end
end
