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
    #   renderer = LiveRenderer.new(labels: parts.map(&:inspect), per_partition_docs_estimate: 5000)
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
      VIEWPORT_MARGIN = 3

      # @param labels [Array<String>] display label for each slot (partition key)
      # @param partitions [Array, nil] raw partition values for non-TTY output (defaults to labels)
      # @param per_partition_docs_estimates [Array<Integer, nil>, nil] per-slot doc estimates (takes priority)
      # @param per_partition_docs_estimate [Integer, nil] uniform doc estimate for all slots (fallback)
      # @param per_partition_estimate [Integer, nil] deprecated batch-based estimate (last resort fallback)
      # @param io [IO] output stream (defaults to $stdout)
      def initialize(labels:, partitions: nil, per_partition_docs_estimates: nil,
                     per_partition_docs_estimate: nil, per_partition_estimate: nil, io: $stdout)
        @io = io
        @tty = Color.enabled?
        @mutex = Mutex.new
        @stop_signal = ConditionVariable.new
        @thread = nil
        @running = false
        @rendered_once = false
        nontty_cb = @tty ? nil : method(:flush_nontty_slot)
        raw = partitions || labels
        per_slot = per_partition_docs_estimates || []
        global_est = per_partition_docs_estimate || per_partition_estimate
        @slots = labels.each_with_index.map do |label, idx|
          Slot.new(
            label: label, partition: raw[idx], docs_estimate: per_slot[idx] || global_est,
            on_done: nontty_cb
          )
        end
        @viewport = resolve_viewport
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

            compact? ? build_viewport_lines(frame_idx) : @slots.map { |slot| slot.render(frame_idx) }
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

        @io.write("\e[#{compact? ? @viewport : @slots.size}A") if @rendered_once
        @slots.each { |slot| @io.write("\r\e[K#{slot.render_final}\n") }
        @io.flush
      end

      def resolve_viewport
        return @slots.size unless @tty

        max = terminal_height - VIEWPORT_MARGIN
        return @slots.size if max < 1 || @slots.size <= max

        max
      end

      def terminal_height
        require 'io/console'
        IO.console&.winsize&.first || 24
      rescue StandardError
        24
      end

      def compact?
        @viewport < @slots.size
      end

      # Build a fixed-size frame showing a summary header, active slots, and
      # recent completions — capped to @viewport lines so cursor-up stays
      # within terminal bounds.
      def build_viewport_lines(frame_idx)
        available = @viewport - 1
        lines = [viewport_header]

        active = []
        done = []
        @slots.each do |slot|
          case slot.state
          when :in_progress then active << slot
          when :done, :error then done << slot
          end
        end

        shown_active = active.first(available)
        shown_active.each { |s| lines << s.render(frame_idx) }
        available -= shown_active.size

        if available.positive?
          shown_done = done.last(available)
          shown_done.each { |s| lines << s.render_final }
          available -= shown_done.size
        end

        available.times { lines << '' }
        lines
      end

      def viewport_header
        done_count = 0
        active_count = 0
        error_count = 0
        @slots.each do |s|
          case s.state
          when :done then done_count += 1
          when :error then error_count += 1
          when :in_progress then active_count += 1
          end
        end
        pending_count = @slots.size - done_count - error_count - active_count

        parts = +"    #{done_count}/#{@slots.size} done"
        parts << " | #{active_count} active" if active_count.positive?
        parts << " | #{pending_count} pending" if pending_count.positive?
        parts << " | #{Color.apply("#{error_count} failed", :red)}" if error_count.positive?
        parts
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
        # @param docs_estimate [Integer, nil] estimated total docs for doc-based progress bar
        # @param on_done [Proc, nil] callback invoked after finish/finish_error (non-TTY flush)
        def initialize(label:, partition: nil, docs_estimate: nil, on_done: nil)
          @label = label
          @partition = partition.nil? ? label : partition
          @docs_estimate = docs_estimate
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
          if @docs_estimate&.positive?
            ratio = @docs_total.positive? ? @docs_total.to_f / @docs_estimate : 0.0
            pct = [100, (ratio * 100).round].min
            filled = [(ratio * BAR_WIDTH).round, BAR_WIDTH].min
            empty = BAR_WIDTH - filled
            bar = "\u2588" * filled + "\u2591" * empty
            "#{bar}  #{pct}%  (#{@docs_total}/#{@docs_estimate} docs)"
          elsif @batches_done.positive?
            "(#{@docs_total} docs)"
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
