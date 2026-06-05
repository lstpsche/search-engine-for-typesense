# frozen_string_literal: true

module SearchEngine
  module PostgresOutbox
    # Normalized result returned by collection event processors.
    class ProcessorResult
      attr_reader :processed_event_ids, :failed_event_ids, :error

      # @param processed_event_ids [Array<Integer, String>]
      # @param failed_event_ids [Array<Integer, String>]
      # @param error [Exception, String, nil]
      def initialize(processed_event_ids:, failed_event_ids: [], error: nil)
        @processed_event_ids = Array(processed_event_ids)
        @failed_event_ids = Array(failed_event_ids)
        @error = error
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

      # @return [Boolean]
      def success?
        failed_event_ids.empty? && error.nil?
      end
    end
  end
end
