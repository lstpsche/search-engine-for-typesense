# frozen_string_literal: true

module SearchEngine
  module PostgresOutbox
    # ActiveJob entrypoint for one bounded PostgreSQL outbox drain pass.
    class DrainJob < ::ActiveJob::Base
      queue_as do
        SearchEngine.config.postgres_outbox.queue_name.to_s
      end

      # Drain pending outbox events once when PostgreSQL outbox processing is enabled.
      # @param limit [Integer, nil] optional maximum number of events to claim
      # @param target_key [String, Symbol, nil] optional delivery target scope
      # @return [Hash, nil]
      def perform(limit: nil, target_key: nil)
        return nil unless SearchEngine.config.postgres_outbox.enabled
        return enqueue_target_drains(limit: limit) if target_key.nil? && delivery_targets.any?

        effective_limit = limit || SearchEngine.config.postgres_outbox.batch_size
        drainer = drainer_for(target_key)
        summary = drainer.drain_once(limit: effective_limit)
        enqueue_continuation(limit: limit, target_key: target_key) if summary[:claimed].to_i >= effective_limit.to_i

        summary
      end

      private

      def enqueue_target_drains(limit:)
        SearchEngine::PostgresOutbox::DrainEnqueuer.enqueue_all(limit: limit)
        { claimed: 0, processed: 0, enqueued_targets: delivery_targets.size }
      end

      def drainer_for(target_key)
        return SearchEngine::PostgresOutbox::Drainer.new if target_key.nil?

        SearchEngine::PostgresOutbox::Drainer.new(target_key: target_key)
      end

      def enqueue_continuation(limit:, target_key:)
        return enqueue_target_continuation(limit: limit, target_key: target_key) unless target_key.nil?
        return self.class.perform_later if limit.nil?

        self.class.perform_later(limit: limit)
      end

      def enqueue_target_continuation(limit:, target_key:)
        target = delivery_target_for!(target_key)
        job = self.class.set(queue: target.queue_name)
        return job.perform_later(target_key: target.key) if limit.nil?

        job.perform_later(target_key: target.key, limit: limit)
      end

      def delivery_target_for!(target_key)
        normalized_key = target_key.to_s
        target = delivery_targets.find { |candidate| candidate.key == normalized_key }
        return target if target

        raise ArgumentError, "unknown postgres outbox delivery target: #{normalized_key}"
      end

      def delivery_targets
        configured = SearchEngine.config.postgres_outbox.delivery_targets
        raw_targets = configured.respond_to?(:call) ? configured.call : configured
        Array(raw_targets).map { |target| DeliveryTarget.normalize(target) }
      end
    end
  end
end
