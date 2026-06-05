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

        drainer = SearchEngine::PostgresOutbox::Drainer.new
        return drainer.drain_once if limit.nil?

        drainer.drain_once(limit: limit)
      end
    end
  end
end
