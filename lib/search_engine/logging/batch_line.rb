# frozen_string_literal: true

module SearchEngine
  module Logging
    # Shared formatter for per-batch log lines during indexation.
    #
    # Produces the aligned `single →` / continuation lines used by both
    # {SearchEngine::Base::IndexMaintenance} and {SearchEngine::Indexer::BulkImport}.
    #
    # @since M8
    module BatchLine
      FIRST_PREFIX = '  single → '
      CONTINUATION_PREFIX = '           '

      module_function

      # Format a single batch log line.
      #
      # @param stats [Hash] batch statistics with :docs_count, :success_count, :failure_count, :duration_ms, :errors_sample
      # @param batch_number [Integer] 1-based batch index
      # @param indifferent [Boolean] when true, also check string keys (for summary hashes)
      # @return [String]
      def format(stats, batch_number, indifferent: false)
        require 'search_engine/logging/color'

        batch_status = status_from_counts(stats, indifferent: indifferent)
        status_color = SearchEngine::Logging::Color.for_status(batch_status)

        prefix = batch_number == 1 ? FIRST_PREFIX : CONTINUATION_PREFIX
        line = +prefix
        line << SearchEngine::Logging::Color.apply("status=#{batch_status}", status_color) << ' '
        line << "docs=#{fetch_stat(stats, :docs_count, indifferent: indifferent, default: 0)}" << ' '

        success_count = fetch_stat(stats, :success_count, indifferent: indifferent, default: 0).to_i
        success_str = "success=#{success_count}"
        line << (success_count.positive? ? SearchEngine::Logging::Color.bold(success_str) : success_str) << ' '

        failed_count = fetch_stat(stats, :failure_count, indifferent: indifferent, default: 0).to_i
        failed_str = "failed=#{failed_count}"
        line << (failed_count.positive? ? SearchEngine::Logging::Color.apply(failed_str, :red) : failed_str) << ' '

        line << "batch=#{batch_number} "
        line << "duration_ms=#{fetch_stat(stats, :duration_ms, indifferent: indifferent, default: 0.0)}"

        sample_err = extract_sample_error(stats, indifferent: indifferent)
        line << " sample_error=#{sample_err.inspect}" if sample_err

        line
      end

      # Derive batch status from success/failure counts.
      # @return [Symbol] :ok, :partial, or :failed
      def status_from_counts(stats, indifferent: false)
        success = fetch_stat(stats, :success_count, indifferent: indifferent, default: 0).to_i
        failure = fetch_stat(stats, :failure_count, indifferent: indifferent, default: 0).to_i

        if failure.positive? && success.positive?
          :partial
        elsif failure.positive?
          :failed
        else
          :ok
        end
      end

      # Extract first non-blank sample error message.
      # @return [String, nil]
      def extract_sample_error(stats, indifferent: false)
        samples = fetch_stat(stats, :errors_sample, indifferent: indifferent, default: nil)
        return nil unless samples.is_a?(Array) && samples.any?

        samples.each do |msg|
          s = msg.to_s
          return s unless s.strip.empty?
        end
        nil
      end

      # @api private
      def fetch_stat(stats, key, indifferent: false, default: nil)
        val = stats[key]
        val = stats[key.to_s] if val.nil? && indifferent
        val.nil? ? default : val
      end

      private_class_method :fetch_stat
    end
  end
end
