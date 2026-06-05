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
      # @return [Hash, nil]
      def perform(limit: nil)
        return nil unless SearchEngine.config.postgres_outbox.enabled

        effective_limit = limit || SearchEngine.config.postgres_outbox.batch_size
        drainer = SearchEngine::PostgresOutbox::Drainer.new
        summary = drainer.drain_once(limit: effective_limit)
        enqueue_continuation(limit: limit) if summary[:claimed].to_i >= effective_limit.to_i

        summary
      end

      private

      def enqueue_continuation(limit:)
        return self.class.perform_later if limit.nil?

        self.class.perform_later(limit: limit)
      end
    end
  end
end
