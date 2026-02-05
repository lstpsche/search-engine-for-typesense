# frozen_string_literal: true

require 'json'

module SearchEngine
  class Indexer
    # Shared parser for Typesense import API responses.
    #
    # Accepts the two shapes returned by different versions of the Typesense Ruby
    # client:
    # - String (JSONL status lines)
    # - Array<Hash> (already parsed per-document statuses)
    #
    # Returns a stable tuple:
    #   [success_count, failure_count, errors_sample]
    module ImportResponseParser
      MAX_ERROR_SAMPLES = 5
      INVALID_JSON_LINE = 'invalid-json-line'

      module_function

      # @param raw [String, Array, Object]
      # @return [Array(Integer, Integer, Array<String>)]
      def parse(raw)
        return parse_from_string(raw) if raw.is_a?(String)
        return parse_from_array(raw) if raw.is_a?(Array)

        [0, 0, []]
      end

      def parse_from_string(str)
        success = 0
        failure = 0
        samples = []

        str.each_line do |line|
          row = line.strip
          next if row.empty?

          parsed = safe_parse_json(row)
          unless parsed
            failure += 1
            samples << INVALID_JSON_LINE
            next
          end

          if truthy?(parsed['success'] || parsed[:success])
            success += 1
          else
            failure += 1
            msg = parsed['error'] || parsed[:error] || parsed['message'] || parsed[:message]
            samples << msg.to_s[0, 200] if msg
          end
        end

        [success, failure, samples[0, MAX_ERROR_SAMPLES]]
      end
      module_function :parse_from_string

      def parse_from_array(arr)
        success = 0
        failure = 0
        samples = []

        arr.each do |entry|
          if entry.is_a?(Hash) && truthy?(entry['success'] || entry[:success])
            success += 1
          else
            failure += 1
            msg = entry.is_a?(Hash) ? (entry['error'] || entry[:error] || entry['message'] || entry[:message]) : nil
            samples << msg.to_s[0, 200] if msg
          end
        end

        [success, failure, samples[0, MAX_ERROR_SAMPLES]]
      end
      module_function :parse_from_array

      def safe_parse_json(line)
        JSON.parse(line)
      rescue StandardError
        nil
      end
      module_function :safe_parse_json

      def truthy?(value)
        value == true || value.to_s.downcase == 'true'
      end
      module_function :truthy?
    end
  end
end
