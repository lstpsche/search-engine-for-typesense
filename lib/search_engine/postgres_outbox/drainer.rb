# frozen_string_literal: true

require 'socket'

module SearchEngine
  module PostgresOutbox
    # Orchestrates one bounded PostgreSQL outbox drain pass.
    class Drainer
      BLOCKED_ERROR = 'Skipped because an earlier outbox collection group failed'

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
      def drain_once(limit: SearchEngine.config.postgres_outbox.batch_size)
        SearchEngine::Instrumentation.instrument(
          'search_engine.postgres_outbox.drain',
          drain_payload(limit)
        ) do |payload|
          events = repository.claim_pending(limit: limit, worker_id: worker_id)
          summary = empty_summary(events)
          next summary if events.empty?

          kept, superseded_ids = coalesce(events)
          repository.mark_superseded!(superseded_ids)
          summary[:superseded] = superseded_ids.size

          ordered = SearchEngine::DependencyPlanner.order_events(kept)
          process_ordered_events(ordered, summary)
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
          collections: []
        }
        summary[:target_key] = target_key if target_key
        summary
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

        [latest_by_key.values.sort_by { |event| event.id.to_i }, superseded.map(&:id)]
      end

      def process_ordered_events(events, summary)
        grouped = group_by_collection(events)

        grouped.each_with_index do |(collection, collection_events), index|
          result = call_processor(collection, collection_events)
          processed_ids = processed_event_ids(result, collection_events)
          failed_ids = failed_event_ids(result, collection_events, processed_ids)

          mark_processed(processed_ids, collection_events, summary)
          if result.success?
            mark_retryable(failed_ids, incomplete_success_error(collection, processed_ids, failed_ids), summary)
          else
            mark_retryable(failed_ids, result.error, summary)
            mark_remaining_retryable(grouped[(index + 1)..], summary)
            break
          end
        rescue StandardError => error
          mark_retryable(collection_events.map(&:id), error, summary)
          mark_remaining_retryable(grouped[(index + 1)..], summary)
          break
        end
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

      def call_processor(collection, events)
        result = processor_for(collection).call(events: events, context: processor_context)
        normalize_result(result, events)
      end

      def processor_for(collection)
        processors = SearchEngine.config.postgres_outbox.collection_processors || {}
        processors[collection] || processors[collection.to_sym] || processor
      end

      def normalize_result(result, events)
        return ProcessorResult.success(events.map(&:id)) if result.nil?
        return result if result.respond_to?(:success?)

        raise TypeError, 'postgres outbox processor must return ProcessorResult or nil'
      end

      def processed_event_ids(result, events)
        return events.map(&:id) if result.success? && result.processed_event_ids.empty?

        result.processed_event_ids
      end

      def failed_event_ids(result, events, processed_ids)
        return result.failed_event_ids unless result.failed_event_ids.empty?

        events.map(&:id) - processed_ids
      end

      def incomplete_success_error(collection, processed_ids, failed_ids)
        return nil if failed_ids.empty?

        "Postgres outbox processor for #{collection} reported success without covering event ids: " \
          "#{failed_ids.join(', ')}; processed ids: #{processed_ids.join(', ')}"
      end

      def mark_processed(event_ids, events, summary)
        ids = Array(event_ids).compact
        return if ids.empty?

        repository.mark_processed!(ids)
        summary[:processed] += ids.size
        summary[:collections] |= events.map(&:collection)
      end

      def mark_retryable(event_ids, error, summary)
        ids = Array(event_ids).compact
        return if ids.empty?

        repository.mark_retryable!(ids, error: error)
        summary[:retryable] += ids.size
        summary[:failed] += ids.size
      end

      def mark_remaining_retryable(remaining_groups, summary)
        Array(remaining_groups).each do |group|
          mark_retryable(group.last.map(&:id), BLOCKED_ERROR, summary)
        end
      end

      def default_worker_id
        "#{Socket.gethostname}:#{$PROCESS_ID}:#{Thread.current.object_id}"
      end

      def processor_context
        context = { worker_id: worker_id }
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
