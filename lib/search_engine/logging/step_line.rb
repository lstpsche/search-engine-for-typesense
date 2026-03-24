# frozen_string_literal: true

require 'search_engine/logging/color'

module SearchEngine
  module Logging
    # Manages a single terminal line that can be overwritten in-place.
    #
    # On TTY: writes without trailing newline, overwrites via +\r\e[K+ on
    # subsequent calls, finalizes with a newline.
    # On non-TTY: buffers the pending text and only emits it when
    # {#yield_line!} is called; atomic operations (no sub-output) produce
    # exactly one output line.
    #
    # @example Atomic operation (overwrite on completion)
    #   step = StepLine.new('Schema Status')
    #   step.update('checking')
    #   drift = check_drift
    #   step.finish(drift ? 'drift' : 'in_sync')
    #
    # @example Operation with sub-output
    #   step = StepLine.new('Schema')
    #   step.update('creating')
    #   step.yield_line!
    #   index_partitions!          # prints partition progress lines
    #   step.finish('created')
    #
    # @since M9
    class StepLine
      PENDING = "\u2026" # …
      SUCCESS = "\u2713" # ✓
      WARNING = '!'
      SKIP    = '-'

      # @param label [String] short step name (e.g. "Schema", "Cleanup")
      # @param io [IO] output stream (defaults to $stdout)
      def initialize(label, io: $stdout)
        @label = label
        @io = io
        @tty = Color.enabled?
        @active = false
        @yielded = false
        @buffered = nil
        @started_at = now
      end

      # Show or overwrite the current line with a pending state.
      #
      # @param detail [String, nil] optional status text
      # @return [void]
      def update(detail = nil)
        if @tty && !@yielded
          @io.write("\r\e[K#{format_line(PENDING, @label, detail)}")
          @io.flush
          @active = true
        else
          @buffered = format_line(PENDING, @label, detail) unless @yielded
        end
      end

      # Finalize successfully (green checkmark).
      #
      # @param detail [String, nil] result text
      # @return [void]
      def finish(detail = nil)
        text = format_line(Color.apply(SUCCESS, :green), @label, detail, elapsed: elapsed_s)
        write_final(text)
      end

      # Finalize with a warning (yellow bang).
      #
      # @param detail [String, nil] result text
      # @return [void]
      def finish_warn(detail = nil)
        text = format_line(Color.apply(WARNING, :yellow), @label, detail, elapsed: elapsed_s)
        write_final(text)
      end

      # Finalize as skipped (dim).
      #
      # @param reason [String, nil] why the step was skipped
      # @return [void]
      def skip(reason = nil)
        detail = reason ? "skip (#{reason})" : 'skip'
        text = Color.dim(format_line(SKIP, @label, detail))
        write_final(text)
      end

      # Terminate the current line before sub-output starts.
      #
      # On TTY, appends a newline so the pending text stays visible above
      # the sub-output.  On non-TTY, flushes the buffered pending text.
      # After this call, {#finish} writes a new line instead of overwriting.
      #
      # @return [void]
      def yield_line!
        return if @yielded

        if @tty && @active
          @io.write("\n")
          @active = false
        elsif @buffered
          @io.puts(@buffered)
          @buffered = nil
        end
        @yielded = true
      end

      private

      def write_final(text)
        @buffered = nil
        if @tty && @active && !@yielded
          @io.write("\r\e[K#{text}\n")
        else
          @io.puts(text)
        end
        @active = false
        @yielded = true
      end

      def format_line(symbol, label, detail, elapsed: nil)
        parts = +"#{symbol} #{label}"
        parts << " \u2014 #{detail}" if detail
        parts << " (#{elapsed}s)" if elapsed && elapsed >= 0.1
        parts
      end

      def elapsed_s
        (now - @started_at).round(1)
      end

      def now
        Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end
    end
  end
end
