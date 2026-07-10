# frozen_string_literal: true

module SearchEngine
  module PostgresOutbox
    # Immutable value object for one PostgreSQL outbox row.
    class Event
      VALID_OPERATIONS = %i[upsert delete].freeze

      attr_reader :id,
                  :source_table,
                  :source_model_name,
                  :collection,
                  :record_id,
                  :document_id,
                  :operation,
                  :attempts,
                  :payload,
                  :created_at,
                  :delivery_id,
                  :target_key,
                  :delivery_lease_owner

      # @param row [Hash] outbox row with string or symbol keys
      # @raise [ArgumentError] when operation is not upsert/delete
      def initialize(row)
        @id = value(row, :id)
        @source_table = string_value(row, :source_table)
        @source_model_name = string_value(row, :source_model_name)
        @collection = string_value(row, :collection)
        @record_id = string_value(row, :record_id)
        @document_id = string_value(row, :document_id)
        @operation = normalize_operation(value(row, :operation))
        @attempts = (value(row, :delivery_attempts) || value(row, :attempts)).to_i
        @payload = value(row, :payload) || {}
        @created_at = value(row, :created_at)
        @delivery_id = value(row, :delivery_id)
        @target_key = string_value(row, :target_key)
        @delivery_lease_owner = string_value(row, :delivery_lease_owner)
      end

      # @return [Array<String>] key used by drainer coalescing
      def coalesce_key
        [collection, document_id]
      end

      private

      def value(row, key)
        row[key] || row[key.to_s]
      end

      def string_value(row, key)
        raw = value(row, key)
        raw&.to_s
      end

      def normalize_operation(raw)
        operation = raw.to_s.downcase.to_sym
        return operation if VALID_OPERATIONS.include?(operation)

        raise ArgumentError, "unsupported postgres outbox operation: #{raw.inspect}"
      end
    end
  end
end
