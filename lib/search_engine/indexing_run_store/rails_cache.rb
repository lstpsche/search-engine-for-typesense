# frozen_string_literal: true

require 'search_engine/indexing_run'

module SearchEngine
  module IndexingRunStore
    # Rails.cache-backed implementation for async partition indexing run metadata.
    class RailsCache
      # @param cache [Object, nil] cache object responding to read/write/delete
      def initialize(cache: nil)
        @cache = cache || resolve_rails_cache!
        validate_cache!
      end

      # Create a run snapshot with pending partition entries.
      # @return [Hash]
      def create_run(run_id:, collection:, collection_class_name:, into:, partitions:, ttl_s:)
        entries = build_partitions(partitions)
        run = {
          run_id: run_id.to_s,
          collection: collection.to_s,
          collection_class_name: collection_class_name.to_s,
          into: into.to_s,
          status: 'running',
          total_partitions: Array(partitions).size,
          partition_keys: entries.keys,
          ttl_s: ttl_s,
          created_at: SearchEngine::IndexingRun.iso8601_now,
          updated_at: SearchEngine::IndexingRun.iso8601_now
        }
        write_meta(run, ttl_s: ttl_s)
        entries.each do |partition_key, entry|
          @cache.write(partition_cache_key(run_id, partition_key), entry, expires_in: ttl_s)
        end
        snapshot(run_id: run_id)
      end

      # Mark a partition as started.
      # @return [Hash]
      def mark_started(run_id:, partition_key:, job_id: nil)
        update_partition(run_id, partition_key) do |entry|
          entry[:status] = 'running'
          entry[:job_id] = job_id.to_s unless job_id.nil?
        end
      end

      # Mark a partition as succeeded with import counters.
      # @return [Hash]
      def mark_succeeded(run_id:, partition_key:, summary:)
        update_partition(run_id, partition_key) do |entry|
          assign_summary!(entry, summary)
          entry[:status] = 'succeeded'
          entry[:sample_error] = nil
        end
      end

      # Mark a partition as failed.
      # @return [Hash]
      def mark_failed(run_id:, partition_key:, error:)
        update_partition(run_id, partition_key) do |entry|
          entry[:status] = 'failed'
          entry[:sample_error] = error_message(error)
          entry[:failed_total] = [entry[:failed_total].to_i, 1].max
        end
      end

      # Read the current run snapshot.
      # @return [Hash, nil]
      def snapshot(run_id:)
        meta = symbolize_meta(@cache.read(meta_cache_key(run_id)))
        return nil unless meta

        partition_keys = Array(meta[:partition_keys]).map(&:to_s)
        partitions = partition_keys.each_with_object({}) do |partition_key, hash|
          entry = @cache.read(partition_cache_key(run_id, partition_key))
          hash[partition_key] = if entry.is_a?(Hash)
                                  deep_symbolize(entry)
                                else
                                  missing_partition_entry(partition_key)
                                end
        end
        meta[:partitions] = partitions
        meta[:status] = run_status(partitions)
        meta
      end

      # Expire a run immediately.
      # @return [Object]
      def expire(run_id:)
        meta = symbolize_meta(@cache.read(meta_cache_key(run_id)))
        Array(meta && meta[:partition_keys]).each do |partition_key|
          @cache.delete(partition_cache_key(run_id, partition_key))
        end
        @cache.delete(meta_cache_key(run_id))
      end

      private

      def resolve_rails_cache!
        rails = Object.const_get(:Rails) if Object.const_defined?(:Rails)
        cache = rails.cache if rails.respond_to?(:cache)
        return cache if cache

        raise ArgumentError,
              'SearchEngine async partition indexing requires Rails.cache or a custom partition_run_store'
      rescue NameError
        raise ArgumentError,
              'SearchEngine async partition indexing requires Rails.cache or a custom partition_run_store'
      end

      def validate_cache!
        missing = %i[read write delete].reject { |method_name| @cache.respond_to?(method_name) }
        unless missing.empty?
          raise ArgumentError, "Rails.cache is unusable for indexing runs; missing: #{missing.join(', ')}"
        end

        probe_key = meta_cache_key("__probe__#{object_id}")
        @cache.write(probe_key, { ok: true }, expires_in: 1)
        value = @cache.read(probe_key)
        @cache.delete(probe_key)
        return if value

        raise ArgumentError, 'Rails.cache is unusable for indexing runs; probe read returned nil'
      rescue ArgumentError
        raise
      rescue StandardError => error
        raise ArgumentError, "Rails.cache is unusable for indexing runs: #{error.class}: #{error.message}"
      end

      def build_partitions(partitions)
        Array(partitions).each_with_object({}) do |partition, hash|
          hash[SearchEngine::IndexingRun.partition_key(partition)] = SearchEngine::IndexingRun.partition_entry(partition)
        end
      end

      def write_meta(run, ttl_s:)
        @cache.write(meta_cache_key(run[:run_id]), run, expires_in: ttl_s)
      end

      def update_partition(run_id, partition_key)
        meta = symbolize_meta(@cache.read(meta_cache_key(run_id)))
        raise KeyError, "indexing run not found: #{run_id}" unless meta

        partition_key = partition_key.to_s
        unless Array(meta[:partition_keys]).map(&:to_s).include?(partition_key)
          raise KeyError, "indexing run partition not found: #{partition_key}"
        end

        entry = @cache.read(partition_cache_key(run_id, partition_key))
        raise KeyError, "indexing run partition not found: #{partition_key}" unless entry

        entry = deep_symbolize(entry)
        yield entry
        now = SearchEngine::IndexingRun.iso8601_now
        entry[:updated_at] = now
        @cache.write(partition_cache_key(run_id, partition_key), entry, expires_in: meta[:ttl_s])
        meta[:updated_at] = now
        write_meta(meta, ttl_s: meta[:ttl_s])
        snapshot(run_id: run_id)
      end

      def run_status(partitions)
        statuses = partitions.values.map { |entry| entry[:status].to_s }
        return 'failed' if statuses.include?('failed')
        return 'succeeded' if statuses.all? { |status| status == 'succeeded' }

        'running'
      end

      def assign_summary!(entry, summary)
        entry[:docs_total] = summary_value(summary, :docs_total).to_i
        entry[:success_total] = summary_value(summary, :success_total).to_i
        entry[:failed_total] = summary_value(summary, :failed_total).to_i
        entry[:sample_error] = summary_value(summary, :sample_error)
        entry[:duration_ms_total] = summary_value(summary, :duration_ms_total)
        entry[:source_duration_ms_total] = summary_value(summary, :source_duration_ms_total)
        entry[:map_duration_ms_total] = summary_value(summary, :map_duration_ms_total)
        entry[:jsonl_duration_ms_total] = summary_value(summary, :jsonl_duration_ms_total)
        entry[:import_duration_ms_total] = summary_value(summary, :import_duration_ms_total)
      end

      def summary_value(summary, key)
        return summary.public_send(key) if summary.respond_to?(key)
        return summary[key] if summary.is_a?(Hash) && summary.key?(key)
        return summary[key.to_s] if summary.is_a?(Hash) && summary.key?(key.to_s)

        nil
      end

      def error_message(error)
        return error if error.is_a?(String)

        "#{error.class}: #{error.message.to_s[0, 200]}"
      end

      def meta_cache_key(run_id)
        "search_engine:indexing_run:#{run_id}:meta"
      end

      def partition_cache_key(run_id, partition_key)
        "search_engine:indexing_run:#{run_id}:partition:#{partition_key}"
      end

      def missing_partition_entry(partition_key)
        {
          partition: nil,
          partition_display: partition_key,
          status: 'failed',
          docs_total: 0,
          success_total: 0,
          failed_total: 1,
          sample_error: "partition metadata missing for #{partition_key}",
          job_id: nil,
          updated_at: SearchEngine::IndexingRun.iso8601_now
        }
      end

      def symbolize_meta(meta)
        return nil unless meta.is_a?(Hash)

        deep_symbolize(meta)
      end

      def deep_symbolize(value)
        case value
        when Hash
          value.each_with_object({}) { |(key, item), hash| hash[key.to_sym] = deep_symbolize(item) }
        when Array
          value.map { |item| deep_symbolize(item) }
        else
          value
        end
      end
    end
  end
end
