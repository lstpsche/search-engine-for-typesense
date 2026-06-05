# frozen_string_literal: true

module SearchEngine
  module PostgresOutbox
    # Default full-document outbox processor for one collection batch.
    class EventProcessor
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
        Array(events).each { |event| process_event(event, context: context) }
        ProcessorResult.success(Array(events).map(&:id))
      rescue StandardError => error
        ProcessorResult.failure(Array(events).map(&:id), error: error)
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
