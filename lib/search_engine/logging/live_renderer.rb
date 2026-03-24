# frozen_string_literal: true

require 'search_engine/logging/color'
require 'search_engine/logging/cursor_guard'

module SearchEngine
  module Logging
    # Multi-line live progress renderer for parallel partition imports.
    #
    # Owns a vertical region of N terminal lines (one per partition). A single
    # render thread redraws changed lines on a 100ms tick with animated braille
    # spinners and optional progress bars. Worker threads update slot state via
    # thread-safe callbacks.
    #
    # On non-TTY, falls back to printing one static line per partition completion
    # using {PartitionProgress.line}, preserving CI/pipe compatibility.
    #
    # @example
    #   renderer = LiveRenderer.new(labels: parts.map(&:inspect), per_partition_estimate: 50)
    #   renderer.start
    #   parts.each_with_index do |part, i|
    #     renderer[i].start
    #     summary = Indexer.rebuild_partition!(klass, partition: part, on_batch: renderer[i].method(:progress))
    #     renderer[i].finish(summary)
    #   end
    #   renderer.stop
    #
    # @since M9
    class LiveRenderer
      FRAMES    = %w[⠋ ⠙ ⠹ ⠸ ⠼ ⠴ ⠦ ⠧ ⠇ ⠏].freeze
      INTERVAL  = 0.1 # seconds per render tick
      BAR_WIDTH = 20

      # @param labels [Array<String>] display label for each slot (partition key)
      # @param partitions [Array, nil] raw partition values for non-TTY output (defaults to labels)
      # @param per_partition_estimate [Integer, nil] estimated batches per partition (for progress bars)
      # @param io [IO] output stream (defaults to $stdout)
      def initialize(labels:, partitions: nil, per_partition_estimate: nil, io: $stdout)
        @io = io
        @tty = Color.enabled?
        @mutex = Mutex.new
        @stop_signal = ConditionVariable.new
        @thread = nil
        @running = false
        @rendered_once = false
        nontty_cb = @tty ? nil : method(:flush_nontty_slot)
        raw = partitions || labels
        @slots = labels.each_with_index.map do |label, idx|
          Slot.new(label: label, partition: raw[idx], estimate: per_partition_estimate, on_done: nontty_cb)
        end
      end

      # Start the background render thread. Hides the cursor on TTY.
      #
      # @return [void]
      def start
        return unless @tty

        @mutex.synchronize do
          return if @running

          @running = true
          CursorGuard.hide(@io)
          @thread = Thread.new { render_loop }
        end
      end

      # Stop rendering, write final static frame, restore cursor.
      #
      # @return [void]
      def stop
        if @tty
          @mutex.synchronize do
            return unless @running

            @running = false
            @stop_signal.signal
          end
          @thread&.join
          @thread = nil
          render_final_frame
          CursorGuard.show(@io)
        else
          flush_pending_nontty
        end
      end

      # @param index [Integer] zero-based slot index
      # @return [Slot]
      def [](index)
        @slots[index]
      end

      # @return [Integer]
      def size
        @slots.size
      end

      private

      def render_loop
        frame_idx = 0
        loop do
          lines = @mutex.synchronize do
            break unless @running

            @stop_signal.wait(@mutex, INTERVAL)
            break unless @running

            @slots.map { |slot| slot.render(frame_idx) }
          end
          break if lines.nil?

          write_lines(lines)
          frame_idx += 1
        end
      end

      def write_lines(lines)
        return if lines.empty?

        @io.write("\e[#{lines.size}A") if @rendered_once
        lines.each { |line| @io.write("\r\e[K#{line}\n") }
        @io.flush
        @rendered_once = true
      end

      def render_final_frame
        return if @slots.empty?

        @io.write("\e[#{@slots.size}A") if @rendered_once
        @slots.each { |slot| @io.write("\r\e[K#{slot.render_final}\n") }
        @io.flush
      end

      def flush_nontty_slot(slot)
        @mutex.synchronize do
          return if slot.flushed?

          @io.puts(slot.nontty_line)
          slot.mark_flushed!
        end
      end

      def flush_pending_nontty
        @slots.each do |slot|
          next if slot.flushed?

          @io.puts(slot.nontty_line)
          slot.mark_flushed!
        end
      end

      # Represents one partition line managed by {LiveRenderer}.
      #
      # Thread-safe: workers call {#start}, {#progress}, {#finish}, {#finish_error}
      # from pool threads; the render thread calls {#render} / {#render_final}.
      #
      # @since M9
      class Slot
        PENDING     = '-'
        SUCCESS     = "\u2713" # ✓
        ERROR_SYM   = '!'

        attr_reader :label, :state

        # @param label [String] partition display label (e.g. partition key inspect)
        # @param partition [Object, nil] raw partition value for non-TTY output (defaults to label)
        # @param estimate [Integer, nil] estimated total batches for progress bar
        # @param on_done [Proc, nil] callback invoked after finish/finish_error (non-TTY flush)
        def initialize(label:, partition: nil, estimate: nil, on_done: nil)
          @label = label
          @partition = partition.nil? ? label : partition
          @estimate = estimate
          @on_done = on_done
          @state = :pending
          @batches_done = 0
          @docs_total = 0
          @success_total = 0
          @failed_total = 0
          @started_at = nil
          @finished_at = nil
          @error_message = nil
          @summary = nil
          @flushed = false
          @mutex = Mutex.new
        end

        # Transition to in-progress.
        #
        # @return [void]
        def start
          @mutex.synchronize do
            @state = :in_progress
            @started_at = now
          end
        end

        # Update batch/doc counters (called from on_batch callback).
        #
        # @param batches_done [Integer]
        # @param docs_total [Integer]
        # @param success_total [Integer]
        # @param failed_total [Integer]
        # @return [void]
        def progress(batches_done: 0, docs_total: 0, success_total: 0, failed_total: 0)
          @mutex.synchronize do
            @batches_done = batches_done
            @docs_total = docs_total
            @success_total = success_total
            @failed_total = failed_total
          end
        end

        # Mark partition as successfully completed.
        #
        # @param summary [SearchEngine::Indexer::Summary]
        # @return [void]
        def finish(summary)
          @mutex.synchronize do
            @state = :done
            @summary = summary
            @finished_at = now
          end
          @on_done&.call(self)
        end

        # Mark partition as failed.
        #
        # @param error [StandardError]
        # @return [void]
        def finish_error(error)
          @mutex.synchronize do
            @state = :error
            @error_message = "#{error.class}: #{error.message.to_s[0, 200]}"
            @finished_at = now
          end
          @on_done&.call(self)
        end

        # Build the animated line for the current render tick (TTY).
        #
        # @param frame_idx [Integer] global frame counter for spinner
        # @return [String]
        def render(frame_idx)
          @mutex.synchronize { render_locked(frame_idx) }
        end

        # Build the static final line (TTY teardown).
        #
        # @return [String]
        def render_final
          @mutex.synchronize { render_final_locked }
        end

        # Build a static line for non-TTY output.
        #
        # @return [String]
        def nontty_line
          @mutex.synchronize do
            return partition_progress_line if @summary
            return "  partition=#{@label} \u2014 error: #{@error_message}" if @state == :error && @error_message

            "  partition=#{@label} \u2014 #{@state}"
          end
        end

        # @return [Boolean]
        def flushed?
          @flushed
        end

        # @return [void]
        def mark_flushed!
          @flushed = true
        end

        private

        def render_locked(frame_idx)
          case @state
          when :pending
            "    #{Color.dim("#{PENDING} partition=#{@label}")}"
          when :in_progress
            frame = FRAMES[frame_idx % FRAMES.size]
            elapsed = elapsed_s
            progress_part = build_progress_part
            time_part = elapsed >= 0.1 ? " (#{elapsed}s)" : ''
            "    #{frame} partition=#{@label}  #{progress_part}#{time_part}"
          when :done, :error
            render_final_locked
          end
        end

        def render_final_locked
          case @state
          when :done
            s = @summary
            elapsed = finished_elapsed_s
            status_color = Color.for_partition_status(s.failed_total.to_i, s.success_total.to_i)
            sym = Color.apply(SUCCESS, :green)
            detail = +"docs=#{s.docs_total} "
            detail << success_str(s.success_total.to_i) << ' '
            detail << failed_str(s.failed_total.to_i)
            detail << " (#{elapsed}s)" if elapsed >= 0.1
            "    #{sym} #{Color.apply("partition=#{@label}", status_color)}  #{detail}"
          when :error
            elapsed = finished_elapsed_s
            sym = Color.apply(ERROR_SYM, :red)
            time_part = elapsed >= 0.1 ? " (#{elapsed}s)" : ''
            "    #{sym} #{Color.apply("partition=#{@label}", :red)}  #{@error_message}#{time_part}"
          else
            "    #{Color.dim("#{PENDING} partition=#{@label}  incomplete")}"
          end
        end

        def build_progress_part
          if @estimate&.positive? && @batches_done.positive?
            ratio = @batches_done.to_f / @estimate
            pct = [100, (ratio * 100).round].min
            filled = [(ratio * BAR_WIDTH).round, BAR_WIDTH].min
            empty = BAR_WIDTH - filled
            bar = "\u2588" * filled + "\u2591" * empty
            "#{bar}  #{pct}%  #{@batches_done}/#{@estimate} batches  (#{@docs_total} docs)"
          elsif @batches_done.positive?
            "#{@batches_done} batches, #{@docs_total} docs"
          else
            ''
          end
        end

        def success_str(count)
          s = "success=#{count}"
          count.positive? ? Color.bold(s) : s
        end

        def failed_str(count)
          s = "failed=#{count}"
          count.positive? ? Color.apply(s, :red) : s
        end

        def elapsed_s
          return 0.0 unless @started_at

          (now - @started_at).round(1)
        end

        def finished_elapsed_s
          return 0.0 unless @started_at

          t = @finished_at || now
          (t - @started_at).round(1)
        end

        def partition_progress_line
          require 'search_engine/logging/partition_progress'
          PartitionProgress.line(@partition, @summary)
        end

        def now
          Process.clock_gettime(Process::CLOCK_MONOTONIC)
        end
      end
    end
  end
end
