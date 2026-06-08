# frozen_string_literal: true

module SearchEngine
  module PostgresOutbox
    # Enqueues PostgreSQL outbox drain jobs for legacy or target-aware delivery mode.
    class DrainEnqueuer
      # Enqueue drain jobs for all configured targets or the legacy queue.
      # @param limit [Integer, nil] optional maximum number of events to claim
      # @return [void]
      def self.enqueue_all(limit: nil)
        new.enqueue_all(limit: limit)
      end

      # @param repository [SearchEngine::PostgresOutbox::Repository]
      # @param drain_job [#perform_later, #set, nil] ActiveJob-compatible drain job class
      # @param targets_resolver [#call, nil] optional delivery targets resolver
      def initialize(repository: Repository.new, drain_job: nil, targets_resolver: nil)
        @repository = repository
        @drain_job = drain_job
        @targets_resolver = targets_resolver
      end

      # Enqueue drain jobs for all configured targets or the legacy queue.
      # @param limit [Integer, nil] optional maximum number of events to claim
      # @return [void]
      def enqueue_all(limit: nil)
        targets = delivery_targets
        return enqueue_legacy(limit: limit) if targets.empty?

        materialize_deliveries(limit: limit)
        return enqueue_acquired_slots(targets, limit: limit) if repository.drain_slots_table_exists?

        targets.each { |target| enqueue_target(target, limit: limit) }
        nil
      end

      private

      attr_reader :repository, :targets_resolver

      def materialize_deliveries(limit:)
        return repository.materialize_deliveries! if limit.nil?

        repository.materialize_deliveries!(limit: limit)
      end

      def enqueue_legacy(limit:)
        return drain_job.perform_later if limit.nil?

        drain_job.perform_later(limit: limit)
      end

      def enqueue_target(target, limit:)
        job = drain_job.set(queue: target.queue_name)
        return job.perform_later(target_key: target.key) if limit.nil?

        job.perform_later(target_key: target.key, limit: limit)
      end

      def enqueue_acquired_slots(targets, limit:)
        repository.acquire_drain_slots!(targets: targets).each do |slot|
          enqueue_drain_slot(slot, limit: limit)
        end
        nil
      end

      def enqueue_drain_slot(slot, limit:)
        job = drain_job.set(queue: slot.fetch(:queue_name))
        kwargs = { target_key: slot.fetch(:target_key), drain_slot: slot.fetch(:slot) }
        kwargs[:limit] = limit unless limit.nil?
        job.perform_later(**kwargs)
      rescue StandardError => error
        repository.release_requeued_drain_slot!(
          target_key: slot.fetch(:target_key),
          slot: slot.fetch(:slot),
          error: error
        )
        raise
      end

      def delivery_targets
        raw_targets = targets_resolver ? targets_resolver.call : configured_delivery_targets
        Array(raw_targets).map { |target| DeliveryTarget.normalize(target) }
      end

      def configured_delivery_targets
        configured = SearchEngine.config.postgres_outbox.delivery_targets
        configured.respond_to?(:call) ? configured.call : configured
      end

      def drain_job
        @drain_job ||= SearchEngine::PostgresOutbox::DrainJob
      end
    end
  end
end
