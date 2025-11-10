# frozen_string_literal: true

module SearchEngine
  module Logging
    # ANSI color helpers for CLI output.
    #
    # Applies colors only when $stdout is a TTY and NO_COLOR is not set.
    # Intended for short substring coloring inside existing log lines.
    #
    # @since M8
    module Color
      module_function

      # @param str [String]
      # @param color [Symbol] one of :green, :yellow, :red
      # @return [String]
      def apply(str, color)
        return str unless enabled?

        code = case color.to_sym
               when :green then 32
               when :yellow then 33
               when :red then 31
               else 0
               end
        return str if code.zero?

        "\e[#{code}m#{str}\e[0m"
      end

      # Map indexation status to a color.
      # @param status [#to_s]
      # @return [Symbol] color name
      def for_status(status)
        case status.to_s
        when 'ok' then :green
        when 'failed' then :red
        when 'partial' then :yellow
        else :yellow
        end
      end

      # Determine color for partition/status based on success/failure counts.
      # @param failed_total [Integer] number of failed documents
      # @param success_total [Integer] number of successful documents
      # @return [Symbol] color name (:green, :yellow, or :red)
      def for_partition_status(failed_total, success_total)
        if failed_total.to_i.zero?
          :green
        elsif success_total.to_i.positive?
          :yellow # partial success
        else
          :red # all failed
        end
      end

      # @return [Boolean] whether coloring is active
      def enabled?
        return false if ENV['NO_COLOR']

        begin
          $stdout.isatty
        rescue StandardError
          false
        end
      end
    end
  end
end
