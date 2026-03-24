# frozen_string_literal: true

module SearchEngine
  module Logging
    # Animated braille spinner for long-running terminal operations.
    #
    # Runs a background thread that cycles through braille spinner frames,
    # overwriting the current line at a fixed interval. Thread-safe text
    # updates via {#update}. Hides the cursor while spinning and guarantees
    # restore via +at_exit+.
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
        @restore_registered = false
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
          hide_cursor
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
        show_cursor
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
        @mutex.synchronize do
          while @running
            render(FRAMES[frame_idx % FRAMES.size])
            frame_idx += 1
            @stop_signal.wait(@mutex, INTERVAL)
          end
        end
      end

      def render(frame)
        @io.write("\r\e[K#{frame} #{@text}")
        @io.flush
      end

      def hide_cursor
        @io.write("\e[?25l")
        @io.flush
        register_cursor_restore
      end

      def show_cursor
        @io.write("\e[?25h")
        @io.flush
      end

      def register_cursor_restore
        return if @restore_registered

        io = @io
        at_exit { io.write("\e[?25h") if io.respond_to?(:write) }
        @restore_registered = true
      end
    end
  end
end
