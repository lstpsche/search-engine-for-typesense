# frozen_string_literal: true

require 'socket'
require 'securerandom'
require 'set'
require 'search_engine/cache'

module SearchEngine
  module PostgresOutbox
    # Orchestrates one bounded PostgreSQL outbox drain pass.
    class Drainer
      BLOCKED_ERROR = 'Skipped because an earlier outbox collection group failed'
      LEASE_OWNER_MAX_LENGTH = 255

      # @param repository [SearchEngine::PostgresOutbox::Repository]
      # @param processor [#call]
      # @param worker_id [String, nil]
      # @param target_key [String, Symbol, nil] optional delivery target scope
      def initialize(repository: nil, processor: EventProcessor, worker_id: nil, target_key: nil)
        @target_key = normalize_optional_target_key(target_key)
        @repository = repository || Repository.new(target_key: @target_key)
        @processor = processor
        @worker_id = worker_id || default_worker_id
      end

      # Claim, coalesce, order, process, and mark one batch.
      # @param limit [Integer]
      # @return [Hash]
      def drain_once(limit: nil)
        SearchEngine::Instrumentation.instrument(
          'search_engine.postgres_outbox.drain',
          drain_payload(limit)
        ) do |payload|
          lease_owner = next_lease_owner
          events = repository.claim_pending(limit: limit, worker_id: lease_owner)
          summary = empty_summary(events)
          next summary if events.empty?

          summary[:continue] = true if continue_after_nonempty_batch?

          kept, superseded_events = coalesce(events)
          superseded_ids = repository.mark_superseded!(superseded_events)
          record_acknowledgements(summary, :superseded, superseded_events, superseded_ids)

          ordered = SearchEngine::DependencyPlanner.order_events(kept)
          process_ordered_events(ordered, summary, lease_owner: lease_owner)
          payload.merge!(summary)
          summary
        end
      end

      private

      attr_reader :repository, :processor, :worker_id, :target_key

      def empty_summary(events)
        summary = {
          claimed: events.size,
          processed: 0,
          superseded: 0,
          retryable: 0,
          failed: 0,
          stale: 0,
          collections: []
        }
        summary[:target_key] = target_key if target_key
        summary
      end

      def continue_after_nonempty_batch?
        !target_key.nil? || SearchEngine.config.postgres_outbox.collection_batch_sizes?
      end

      def coalesce(events)
        latest_by_key = {}
        superseded = []

        events.each do |event|
          current = latest_by_key[event.coalesce_key]
          if current.nil? || event.id.to_i > current.id.to_i
            superseded << current if current
            latest_by_key[event.coalesce_key] = event
          else
            superseded << event
          end
        end

        [latest_by_key.values.sort_by { |event| event.id.to_i }, superseded]
      end

      def process_ordered_events(events, summary, lease_owner:)
        grouped = group_by_collection(events)

        grouped.each_with_index do |(collection, claimed_collection_events), index|
          collection_events = claimed_collection_events
          claimed_collection_size = claimed_collection_events.size
          collection_events = renew_remaining_group_leases!(grouped, index, summary)
          current_group_lease_lost = collection_events.size < claimed_collection_size
          if collection_events.empty?
            if current_group_lease_lost
              mark_remaining_retryable(grouped[(index + 1)..], summary)
              break
            end

            next
          end

          result = call_processor(collection, collection_events, lease_owner: lease_owner)
          processed_events = events_for_ids(collection_events, result.processed_event_ids)
          failed_events = events_for_ids(collection_events, result.failed_event_ids)

          mark_processed(processed_events, summary)
          if result.success?
            if current_group_lease_lost
              mark_remaining_retryable(grouped[(index + 1)..], summary)
              break
            end

            next
          end

          mark_result_failures_retryable(failed_events, result, summary)
          mark_remaining_retryable(grouped[(index + 1)..], summary)
          break
        rescue StandardError => error
          mark_retryable(collection_events, error, summary)
          mark_remaining_retryable(grouped[(index + 1)..], summary)
          break
        end
      end

      # A target claim can contain several dependency-ordered collection groups.
      # Refresh all work that has not started before each group, then refuse to
      # call a processor for any delivery whose fenced lease was not renewed.
      # Host processors must still bound one collection group below the configured
      # processing timeout; this prevents earlier groups from consuming the lease
      # budget of later groups.
      def renew_remaining_group_leases!(grouped, index, summary)
        current_events = grouped.fetch(index).last
        return current_events unless target_key

        remaining_groups = grouped[index..]
        remaining_events = remaining_groups.flat_map(&:last)
        renewed_ids = repository.renew_leases!(remaining_events)
        renewed_keys = Set.new(Array(renewed_ids).map(&:to_s))
        stale_count = 0

        remaining_groups.each do |group|
          group_events = group.last
          before = group_events.size
          group_events.select! { |event| renewed_keys.include?(event.id.to_s) }
          stale_count += before - group_events.size
        end

        summary[:stale] += stale_count
        grouped.fetch(index).last
      end

      def group_by_collection(events)
        grouped = []
        index = {}

        events.each do |event|
          collection = event.collection.to_s
          unless index.key?(collection)
            index[collection] = grouped.size
            grouped << [collection, []]
          end
          grouped[index.fetch(collection)].last << event
        end

        grouped
      end

      def call_processor(collection, events, lease_owner:)
        result = processor_for(collection).call(events: events, context: processor_context(lease_owner))
        result = normalize_result(result, events)
        clear_search_cache_after_write!
        result
      end

      # Typesense search-cache entries are not part of the durable outbox
      # acknowledgement. When the host opts in, invalidate them after the
      # processor returns but before any event is acknowledged. A cache-clear
      # failure therefore retries the idempotent document writes instead of
      # reporting convergence that clients cannot observe yet.
      def clear_search_cache_after_write!
        return unless SearchEngine.config.postgres_outbox.clear_cache_after_write

        SearchEngine::Cache.clear
      end

      def processor_for(collection)
        processors = SearchEngine.config.postgres_outbox.collection_processors || {}
        processors[collection] || processors[collection.to_sym] || processor
      end

      def normalize_result(result, events)
        raise TypeError, 'postgres outbox processor must return ProcessorResult' unless result.is_a?(ProcessorResult)

        result.validate_for!(events.map(&:id))
      end

      def mark_result_failures_retryable(events, result, summary)
        Array(events).group_by { |event| result.error_for(event.id) }.each do |event_error, failed_events|
          mark_retryable(failed_events, event_error, summary)
        end
      end

      def mark_processed(events, summary)
        claimed_events = Array(events).compact
        return if claimed_events.empty?

        acknowledged_ids = repository.mark_processed!(claimed_events)
        acknowledged_keys = record_acknowledgements(summary, :processed, claimed_events, acknowledged_ids)
        acknowledged_events = events_for_ids(claimed_events, acknowledged_keys)
        summary[:collections] |= acknowledged_events.map(&:collection)
      end

      def mark_retryable(events, error, summary)
        claimed_events = Array(events).compact
        return if claimed_events.empty?

        acknowledged_ids = repository.mark_retryable!(claimed_events, error: error)
        acknowledged_keys = record_acknowledgements(summary, :retryable, claimed_events, acknowledged_ids)
        summary[:failed] += acknowledged_keys.size
      end

      def mark_remaining_retryable(remaining_groups, summary)
        Array(remaining_groups).each do |group|
          mark_retryable(group.last, BLOCKED_ERROR, summary)
        end
      end

      def events_for_ids(events, ids)
        id_keys = Set.new(Array(ids).compact.map(&:to_s))
        Array(events).select { |event| id_keys.include?(event.id.to_s) }
      end

      def record_acknowledgements(summary, counter, events, acknowledged_ids)
        acknowledged_keys = Array(acknowledged_ids).compact.map(&:to_s).uniq
        requested_keys = Array(events).map { |event| event.id.to_s }.uniq
        acknowledged_keys &= requested_keys
        summary[counter] += acknowledged_keys.size
        summary[:stale] += requested_keys.size - acknowledged_keys.size
        acknowledged_keys
      end

      def default_worker_id
        "#{Socket.gethostname}:#{$PROCESS_ID}:#{Thread.current.object_id}"
      end

      def next_lease_owner
        suffix = SecureRandom.uuid
        prefix_limit = LEASE_OWNER_MAX_LENGTH - suffix.length - 1
        prefix = worker_id.to_s.gsub(/[^a-zA-Z0-9_.:-]/, '_')[0, prefix_limit]
        "#{prefix}:#{suffix}"
      end

      def processor_context(lease_owner)
        context = { worker_id: worker_id, lease_owner: lease_owner }
        context[:target_key] = target_key if target_key
        context
      end

      def drain_payload(limit)
        payload = { limit: limit }
        payload[:target_key] = target_key if target_key
        payload
      end

      def normalize_optional_target_key(value)
        normalized = value&.to_s
        return nil if normalized.nil? || normalized.strip.empty?

        normalized
      end
    end
  end
end
