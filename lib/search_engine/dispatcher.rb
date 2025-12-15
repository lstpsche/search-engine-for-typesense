# frozen_string_literal: true

module SearchEngine
  # Dispatcher routes per-partition rebuilds either synchronously (inline) or via ActiveJob.
  #
  # Public API:
  # - {.dispatch!(klass, partition:, into: nil, mode: nil, queue: nil, metadata: {})}
  #   Returns a descriptor of what happened (enqueued job id / inline summary, mode used).
  module Dispatcher
    # Dispatch a single partition rebuild.
    #
    # @param klass [Class] model class inheriting from {SearchEngine::Base}
    # @param partition [Object] opaque partition key
    # @param into [String, nil] optional target collection (physical or alias)
    # @param mode [Symbol, String, nil] :active_job or :inline; falls back to config
    # @param queue [String, nil] ActiveJob queue override
    # @param metadata [Hash] small, JSON-safe tracing values
    # @return [Hash] descriptor of the action performed
    def self.dispatch!(klass, partition:, into: nil, mode: nil, queue: nil, metadata: {})
      unless klass.is_a?(Class)
        raise SearchEngine::Errors::InvalidParams.new(
          'klass must be a Class',
          doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/indexer#troubleshooting',
          details: { arg: :klass }
        )
      end
      unless klass.ancestors.include?(SearchEngine::Base)
        raise SearchEngine::Errors::InvalidParams.new(
          'klass must inherit from SearchEngine::Base',
          doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/indexer#troubleshooting',
          details: { klass: klass.to_s }
        )
      end

      effective_mode = resolve_mode(mode)
      case effective_mode
      when :active_job
        dispatch_active_job!(klass, partition: partition, into: into, queue: queue, metadata: metadata)
      else
        dispatch_inline!(klass, partition: partition, into: into, metadata: metadata)
      end
    end

    class << self
      private

      def resolve_mode(override)
        m = (override || SearchEngine.config.indexer.dispatch || :inline).to_sym
        return :active_job if m == :active_job && defined?(::ActiveJob::Base)

        :inline
      end

      def dispatch_active_job!(klass, partition:, into:, queue:, metadata:)
        q = (queue || SearchEngine.config.indexer.queue_name || 'search_index').to_s
        class_name = klass.name.to_s
        job = SearchEngine::IndexPartitionJob
              .set(queue: q)
              .perform_later(class_name, partition, into: into, metadata: metadata || {})
        payload = {
          collection: safe_collection_name(klass),
          partition: partition,
          into: into,
          queue: q,
          job_id: job.job_id
        }
        instrument('search_engine.dispatcher.enqueued', payload)
        {
          mode: :active_job,
          collection: payload[:collection],
          partition: partition,
          into: into,
          queue: q,
          job_id: job.job_id
        }
      end

      def dispatch_inline!(klass, partition:, into:, metadata:)
        started = monotonic_ms
        payload = {
          collection: safe_collection_name(klass),
          partition: partition,
          into: into,
          metadata: metadata
        }
        instrument('search_engine.dispatcher.inline_started', payload)
        summary = nil
        SearchEngine::Instrumentation.with_context(dispatch_mode: :inline) do
          summary = SearchEngine::Indexer.rebuild_partition!(klass, partition: partition, into: into)
        end
        duration = (monotonic_ms - started).round(1)
        instrument(
          'search_engine.dispatcher.inline_finished',
          payload.merge(duration_ms: duration, status: summary.status)
        )
        {
          mode: :inline,
          collection: payload[:collection],
          partition: partition,
          into: into,
          indexer_summary: summary,
          duration_ms: duration
        }
      rescue StandardError => error
        instrument(
          'search_engine.dispatcher.inline_error',
          payload.merge(error_class: error.class.name, message_truncated: error.message.to_s[0, 200])
        )
        raise
      end

      def instrument(event, payload)
        SearchEngine::Instrumentation.instrument(event, payload) {}
      end

      def safe_collection_name(klass)
        klass.respond_to?(:collection) ? klass.collection.to_s : klass.name.to_s
      end

      def monotonic_ms
        SearchEngine::Instrumentation.monotonic_ms
      end
    end
  end
end
