# frozen_string_literal: true

module SearchEngine
  module PostgresOutbox
    # Normalized result returned by collection event processors.
    class ProcessorResult
      MAX_REPORTED_IDS = 20
      MAX_REPORTED_ID_LENGTH = 120

      class ValidationError < ArgumentError; end

      attr_reader :processed_event_ids, :failed_event_ids, :error, :errors_by_event_id

      # @param processed_event_ids [Array<Integer, String>]
      # @param failed_event_ids [Array<Integer, String>]
      # @param error [Exception, String, nil]
      # @param errors_by_event_id [Hash]
      def initialize(processed_event_ids:, failed_event_ids: [], error: nil, errors_by_event_id: {})
        @processed_event_ids = Array(processed_event_ids)
        @failed_event_ids = Array(failed_event_ids)
        @error = error
        @errors_by_event_id = normalize_errors_by_event_id(errors_by_event_id)
      end

      # @param event_ids [Array<Integer, String>]
      # @return [ProcessorResult]
      def self.success(event_ids)
        new(processed_event_ids: event_ids)
      end

      # @param event_ids [Array<Integer, String>]
      # @param error [Exception, String]
      # @return [ProcessorResult]
      def self.failure(event_ids, error:)
        new(processed_event_ids: [], failed_event_ids: event_ids, error: error)
      end

      # Validate that each claimed event is classified exactly once.
      # @param event_ids [Array<Integer, String>]
      # @return [self]
      # @raise [ValidationError] when classifications are duplicated, overlapping, unknown, or incomplete
      def validate_for!(event_ids)
        expected = canonical_ids(event_ids)
        processed = canonical_ids(processed_event_ids)
        failed = canonical_ids(failed_event_ids)
        problems = []

        append_duplicate_problem(problems, 'input', expected)
        append_duplicate_problem(problems, 'processed', processed)
        append_duplicate_problem(problems, 'failed', failed)
        append_set_problem(problems, 'overlapping ids', processed & failed)
        append_set_problem(problems, 'unknown ids', (processed | failed) - expected)
        append_set_problem(problems, 'missing ids', expected - (processed | failed))
        append_set_problem(problems, 'errors for non-failed ids', errors_by_event_id.keys - failed)

        unexplained = failed.reject { |event_id| error_for(event_id) }
        append_set_problem(problems, 'failed ids without errors', unexplained)
        problems << 'error present without failed ids' if failed.empty? && !error.nil?

        raise ValidationError, "invalid postgres outbox processor result: #{problems.join('; ')}" if problems.any?

        self
      end

      # @param event_id [Integer, String]
      # @return [Exception, String, nil]
      def error_for(event_id)
        errors_by_event_id[canonical_id(event_id)] || error
      end

      # @return [Boolean]
      def success?
        failed_event_ids.empty? && error.nil? && errors_by_event_id.empty?
      end

      private

      def normalize_errors_by_event_id(errors)
        raise ValidationError, 'errors_by_event_id must be a hash-like object' unless errors.respond_to?(:each_pair)

        errors.each_pair.with_object({}) do |(event_id, event_error), normalized|
          key = canonical_id(event_id)
          raise ValidationError, "duplicate error for event id #{format_ids([key])}" if normalized.key?(key)

          normalized[key] = event_error
        end.freeze
      end

      def canonical_ids(ids)
        Array(ids).map { |event_id| canonical_id(event_id) }
      end

      def canonical_id(event_id)
        event_id.to_s
      end

      def append_duplicate_problem(problems, label, ids)
        duplicates = ids.tally.select { |_event_id, count| count > 1 }.keys
        append_set_problem(problems, "duplicate #{label} ids", duplicates)
      end

      def append_set_problem(problems, label, ids)
        values = ids.to_a
        return if values.empty?

        problems << "#{label}: #{format_ids(values)}"
      end

      def format_ids(ids)
        shown = ids.first(MAX_REPORTED_IDS)
        suffix = ids.size > shown.size ? " ... (#{ids.size} total)" : ''
        "#{shown.map { |event_id| format_id(event_id) }.join(', ')}#{suffix}"
      end

      def format_id(event_id)
        value = event_id.to_s.encode(Encoding::UTF_8, invalid: :replace, undef: :replace, replace: '?')
        value = value.gsub(/\s+/, ' ')
        return value if value.length <= MAX_REPORTED_ID_LENGTH

        "#{value[0, MAX_REPORTED_ID_LENGTH - 3]}..."
      rescue StandardError
        '<unprintable>'
      end
    end
  end
end
