# frozen_string_literal: true

require 'digest'
require 'date'
require 'json'
require 'securerandom'
require 'time'

module SearchEngine
  # Helpers for async partition indexing run metadata.
  module IndexingRun
    module_function

    # Generate a unique run id suitable for operational metadata keys.
    # @return [String]
    def generate_id
      "se-run-#{Time.now.utc.strftime('%Y%m%d%H%M%S')}-#{SecureRandom.hex(8)}"
    end

    # Return a stable primitive key for a partition value.
    # @param partition [Object]
    # @return [String]
    def partition_key(partition)
      "p-#{Digest::SHA256.hexdigest(normalized_partition_json(partition))}"
    end

    # Convert a partition value into ActiveJob-friendly primitives when possible.
    # @param value [Object]
    # @return [Object]
    def serialize_partition(value)
      normalize(value)
    end

    # Return a compact partition string for logs and snapshots.
    # @param partition [Object]
    # @param max [Integer]
    # @return [String]
    def partition_display(partition, max: 160)
      text = begin
        JSON.generate(serialize_partition(partition))
      rescue StandardError
        partition.to_s
      end
      compact(text, max)
    end

    # Build lifecycle-compatible aggregate counters from a run snapshot.
    # @param snapshot [Hash, nil]
    # @return [Hash] result with :status, :docs_total, :success_total, :failed_total, and :sample_error
    def aggregate_result(snapshot)
      unless valid_snapshot?(snapshot)
        return {
          status: :failed,
          docs_total: 0,
          success_total: 0,
          failed_total: 1,
          sample_error: invalid_snapshot_error(snapshot)
        }
      end

      partitions = snapshot && snapshot[:partitions]
      values = partitions.is_a?(Hash) ? partitions.values : []

      docs_total = 0
      success_total = 0
      failed_total = 0
      sample_error = nil
      statuses = []

      values.each do |entry|
        data = symbolize_keys(entry)
        statuses << data[:status].to_s
        docs_total += data[:docs_total].to_i
        success_total += data[:success_total].to_i
        failed_total += data[:failed_total].to_i
        sample_error ||= present_string(data[:sample_error] || data[:error])
      end

      {
        status: aggregate_status(statuses, success_total, failed_total),
        docs_total: docs_total,
        success_total: success_total,
        failed_total: failed_total,
        sample_error: sample_error
      }
    end

    # Build the stored metadata for one partition.
    # @param partition [Object]
    # @return [Hash]
    def partition_entry(partition)
      serialized = serialize_partition(partition)
      {
        partition: serialized,
        partition_display: partition_display(serialized),
        status: 'pending',
        docs_total: 0,
        success_total: 0,
        failed_total: 0,
        sample_error: nil,
        attempts: 0,
        job_id: nil,
        updated_at: iso8601_now
      }
    end

    # @return [String]
    def iso8601_now
      Time.now.utc.iso8601(6)
    end

    def normalized_partition_json(value)
      JSON.generate(serialize_partition(value))
    rescue StandardError
      JSON.generate('__search_engine_fallback__' => value.to_s)
    end

    def normalize(value)
      case value
      when NilClass, TrueClass, FalseClass, Numeric, String
        value
      when Symbol
        value.to_s
      when Time
        value.utc.iso8601(6)
      when Date
        value.iso8601
      when Array
        value.map { |item| normalize(item) }
      when Hash
        value.keys.map(&:to_s).sort.each_with_object({}) do |key, hash|
          original_key = value.key?(key) ? key : value.keys.find { |candidate| candidate.to_s == key }
          hash[key] = normalize(value[original_key])
        end
      else
        value.to_s
      end
    end
    private_class_method :normalized_partition_json, :normalize

    def aggregate_status(statuses, success_total, failed_total)
      return :failed if statuses.empty?
      return :failed if failed_total.positive? && success_total.zero?
      return :partial if failed_total.positive?
      return :running if statuses.any? { |status| %w[pending running].include?(status) }

      :ok
    end
    private_class_method :aggregate_status

    def valid_snapshot?(snapshot)
      return false unless snapshot.is_a?(Hash)

      partitions = snapshot[:partitions]
      return false unless partitions.is_a?(Hash)

      expected_total = snapshot[:total_partitions].to_i
      expected_total.positive? && partitions.size == expected_total
    end
    private_class_method :valid_snapshot?

    def invalid_snapshot_error(snapshot)
      return 'async partition indexing run snapshot is missing' unless snapshot.is_a?(Hash)
      return 'async partition indexing run snapshot has no partition metadata' unless snapshot[:partitions].is_a?(Hash)

      expected_total = snapshot[:total_partitions].to_i
      actual_total = snapshot[:partitions].size
      "async partition indexing run snapshot has #{actual_total}/#{expected_total} partitions"
    end
    private_class_method :invalid_snapshot_error

    def symbolize_keys(hash)
      return {} unless hash.is_a?(Hash)

      hash.transform_keys(&:to_sym)
    end
    private_class_method :symbolize_keys

    def present_string(value)
      text = value.to_s
      text.strip.empty? ? nil : text
    end
    private_class_method :present_string

    def compact(text, max)
      return text if text.length <= max

      "#{text[0, max - 3]}..."
    end
    private_class_method :compact
  end
end
