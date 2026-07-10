# frozen_string_literal: true

module SearchEngine
  module PostgresOutbox
    # Default full-document outbox processor for one collection batch.
    class EventProcessor
      # Bounded, payload-free context for one failed generic event.
      class EventProcessingError < StandardError
        CONTEXT_VALUE_LIMIT = 120
        DETAIL_LIMIT = 500

        attr_reader :event_id, :collection, :operation, :document_id, :original_error

        def initialize(event, error)
          @event_id = event.id
          @collection = event.collection
          @operation = event.operation
          @document_id = event.document_id
          @original_error = error
          super(build_message(error))
          set_backtrace(error.backtrace) if error.respond_to?(:backtrace)
        end

        private

        def build_message(error)
          'postgres outbox event failed ' \
            "collection=#{bounded(collection)} event_id=#{bounded(event_id)} " \
            "operation=#{bounded(operation)} document_id=#{bounded(document_id)}: " \
            "#{bounded(error.class)}: #{bounded(error_message(error), limit: DETAIL_LIMIT)}"
        end

        def error_message(error)
          error.respond_to?(:message) ? error.message : error.to_s
        rescue StandardError
          '<message unavailable>'
        end

        def bounded(value, limit: CONTEXT_VALUE_LIMIT)
          string = value.to_s.encode(Encoding::UTF_8, invalid: :replace, undef: :replace, replace: '?')
          string.gsub(/\s+/, ' ')[0, limit]
        rescue StandardError
          '<unprintable>'
        end
      end

      # @param events [Array<SearchEngine::PostgresOutbox::Event>]
      # @param context [Hash]
      # @return [SearchEngine::PostgresOutbox::ProcessorResult]
      def self.call(events:, context: {})
        new.call(events: events, context: context)
      end

      # @param events [Array<SearchEngine::PostgresOutbox::Event>]
      # @param context [Hash]
      # @return [SearchEngine::PostgresOutbox::ProcessorResult]
      def call(events:, context: {})
        processed_ids = []
        failed_ids = []
        errors = {}

        Array(events).each do |event|
          process_event(event, context: context)
          processed_ids << event.id
        rescue StandardError => error
          failed_ids << event.id
          errors[event.id] = EventProcessingError.new(event, error)
        end

        ProcessorResult.new(
          processed_event_ids: processed_ids,
          failed_event_ids: failed_ids,
          error: errors.values.first,
          errors_by_event_id: errors
        )
      end

      private

      def process_event(event, context:)
        case event.operation
        when :delete
          delete_document(event, context: context)
        when :upsert
          upsert_or_delete_missing_source(event, context: context)
        else
          raise ArgumentError, "unsupported postgres outbox operation: #{event.operation.inspect}"
        end
      end

      def upsert_or_delete_missing_source(event, context:)
        source_model = constantize(event.source_model_name)
        record = source_model.find_by(id: event.record_id)

        unless record
          delete_document(event, context: context)
          return
        end

        search_model = SearchEngine::CollectionResolver.model_for_logical(event.collection)
        raise NameError, "SearchEngine model not found for collection #{event.collection.inspect}" unless search_model

        search_model.upsert(record: record)
      end

      def delete_document(event, context:)
        client = context[:client] || SearchEngine.client
        target = client.resolve_alias(event.collection) || event.collection
        client.delete_document(collection: target, id: event.document_id)
      end

      def constantize(name)
        return name.constantize if name.respond_to?(:constantize)

        Object.const_get(name.to_s)
      end
    end
  end
end
