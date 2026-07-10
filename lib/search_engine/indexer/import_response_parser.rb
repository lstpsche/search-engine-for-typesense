# frozen_string_literal: true

require 'json'

module SearchEngine
  class Indexer
    # Strict, payload-safe parser for Typesense import API responses.
    #
    # Typesense clients return either JSONL or an already-decoded Array<Hash>.
    # Parsing preserves response order and deliberately ignores document/payload
    # keys that some client options may include in each response row.
    module ImportResponseParser
      MAX_ERROR_LENGTH = 200
      MAX_ERROR_SAMPLES = 5
      SUCCESS_KEYS = ['success', :success].freeze
      STATUS_KEYS = ['status', :status, 'code', :code].freeze
      ERROR_KEYS = ['error', :error, 'message', :message].freeze

      module_function

      # Parse every response row into a stable, ordered representation.
      #
      # @param raw [String, Array<Hash>]
      # @return [Array<Hash>] frozen rows with :index, :success, :status, and :error
      # @raise [SearchEngine::Errors::InvalidParams] for malformed or ambiguous responses
      def parse_rows(raw)
        entries = response_entries(raw)
        entries.each_with_index.map { |entry, index| parse_row(entry, index) }.freeze
      end

      # Return the legacy aggregate tuple, derived from strict row parsing.
      #
      # @param raw [String, Array<Hash>]
      # @return [Array(Integer, Integer, Array<String>)]
      def parse(raw)
        rows = parse_rows(raw)
        successes = rows.count { |row| row[:success] }
        failures = rows.length - successes
        errors = rows.filter_map { |row| row[:error] unless row[:success] }
        [successes, failures, errors.first(MAX_ERROR_SAMPLES)]
      end

      def response_entries(raw)
        case raw
        when String
          parse_jsonl(raw)
        when Array
          raw
        when nil
          raise_invalid!('Typesense import response is nil')
        else
          raise_invalid!("Unsupported Typesense import response shape: #{raw.class.name}")
        end
      end
      module_function :response_entries

      def parse_jsonl(raw)
        entries = []
        raw.each_line.with_index do |line, line_index|
          text = line.strip
          next if text.empty?

          begin
            entries << JSON.parse(text)
          rescue JSON::ParserError
            raise_invalid!("Invalid JSON in Typesense import response row #{line_index}")
          end
        end
        entries
      end
      module_function :parse_jsonl

      def parse_row(entry, index)
        unless entry.is_a?(Hash)
          raise_invalid!("Typesense import response row #{index} must be a Hash (got #{entry.class.name})")
        end

        row = {
          index: index,
          success: parse_success(entry, index),
          status: parse_status(entry, index),
          error: parse_error(entry)
        }
        row[:error]&.freeze
        row.freeze
      end
      module_function :parse_row

      def parse_success(entry, index)
        values = values_for(entry, SUCCESS_KEYS)
        raise_invalid!("Typesense import response row #{index} is missing success") if values.empty?

        unless values.all? { |value| [true, false].include?(value) }
          raise_invalid!("Typesense import response row #{index} has a non-boolean success value")
        end
        raise_invalid!("Typesense import response row #{index} has conflicting success values") if values.uniq.size > 1

        values.first
      end
      module_function :parse_success

      def parse_status(entry, index)
        values = values_for(entry, STATUS_KEYS).compact.map { |value| integer_status(value, index) }
        return nil if values.empty?

        raise_invalid!("Typesense import response row #{index} has conflicting status values") if values.uniq.size > 1

        values.first
      end
      module_function :parse_status

      def integer_status(value, index)
        return value if value.is_a?(Integer)
        return Integer(value, 10) if value.is_a?(String) && value.match?(/\A\d+\z/)

        raise_invalid!("Typesense import response row #{index} has a non-integer status value")
      end
      module_function :integer_status

      def parse_error(entry)
        value = ERROR_KEYS.filter_map { |key| entry[key] if entry.key?(key) }.first
        return nil if value.nil?
        return nil unless value.is_a?(String) || value.is_a?(Symbol) || value.is_a?(Numeric)

        value.to_s.gsub(/\s+/, ' ').strip[0, MAX_ERROR_LENGTH]
      end
      module_function :parse_error

      def values_for(entry, keys)
        keys.each_with_object([]) do |key, values|
          values << entry[key] if entry.key?(key)
        end
      end
      module_function :values_for

      def raise_invalid!(message)
        raise SearchEngine::Errors::InvalidParams, message
      end
      module_function :raise_invalid!
    end
  end
end
