# frozen_string_literal: true

require 'socket'

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
      # @param drain_slot [Integer, nil] optional acquired drain slot
      # @return [Hash, nil]
      def perform(limit: nil, target_key: nil, drain_slot: nil)
        return nil unless SearchEngine.config.postgres_outbox.enabled
        return enqueue_target_drains(limit: limit) if target_key.nil? && delivery_targets.any?
        unless drain_slot.nil?
          return perform_with_drain_slot(limit: limit, target_key: target_key, drain_slot: drain_slot)
        end

        effective_limit = drain_limit(limit)
        drainer = drainer_for(target_key)
        summary = drainer.drain_once(limit: effective_limit)
        enqueue_continuation(limit: limit, target_key: target_key) if continue_draining?(summary, effective_limit)

        summary
      end

      private

      def enqueue_target_drains(limit:)
        SearchEngine::PostgresOutbox::DrainEnqueuer.enqueue_all(limit: limit)
        { claimed: 0, processed: 0, enqueued_targets: delivery_targets.size }
      end

      def perform_with_drain_slot(limit:, target_key:, drain_slot:)
        target = delivery_target_for!(target_key)
        slot = drain_slot.to_i
        effective_limit = drain_limit(limit)
        repository = repository_for_slot
        slot_requeued = false
        return stale_slot_summary(target.key, slot) unless repository.start_drain_slot!(
          target_key: target.key,
          slot: slot,
          worker_id: worker_id
        )

        summary = drain_slot_batches(limit: effective_limit, target_key: target.key, drain_slot: slot)
        if summary.delete(:more_work)
          requeued = repository.requeue_drain_slot!(
            target_key: target.key,
            slot: slot,
            worker_id: worker_id
          )
          if requeued
            slot_requeued = true
            enqueue_requeued_slot_continuation(repository, target.key, slot, limit: limit)
          end
        else
          repository.release_drain_slot!(target_key: target.key, slot: slot, worker_id: worker_id)
        end
        summary
      rescue StandardError => error
        if repository && target && slot && !slot_requeued
          repository.release_drain_slot!(target_key: target.key, slot: slot, worker_id: worker_id, error: error)
        end
        raise
      end

      def drain_slot_batches(limit:, target_key:, drain_slot:)
        drainer = drainer_for(target_key)
        summary = slot_summary(target_key, drain_slot)
        max_batches = drain_job_max_batches
        batches = 0
        more_work = false

        while batches < max_batches
          batch_summary = drainer.drain_once(limit: limit)
          batches += 1
          merge_batch_summary!(summary, batch_summary)
          more_work = continue_draining?(batch_summary, limit)
          break unless more_work
          break if runtime_budget_exhausted?
        end

        summary[:batches] = batches
        summary[:more_work] = more_work
        summary
      end

      def enqueue_requeued_slot_continuation(repository, target_key, slot, limit:)
        enqueue_target_continuation(limit: limit, target_key: target_key, drain_slot: slot)
      rescue StandardError => error
        repository.release_requeued_drain_slot!(target_key: target_key, slot: slot, error: error)
        raise
      end

      def continue_draining?(summary, effective_limit)
        summary[:continue] || (!effective_limit.nil? && summary[:claimed].to_i >= effective_limit.to_i)
      end

      def drainer_for(target_key)
        return SearchEngine::PostgresOutbox::Drainer.new(worker_id: worker_id) if target_key.nil?

        SearchEngine::PostgresOutbox::Drainer.new(target_key: target_key, worker_id: worker_id)
      end

      def enqueue_continuation(limit:, target_key:)
        if target_key
          return enqueue_target_drains(limit: limit) if drain_slots_table_exists?

          return enqueue_target_continuation(limit: limit, target_key: target_key)
        end
        return self.class.perform_later if limit.nil?

        self.class.perform_later(limit: limit)
      end

      def enqueue_target_continuation(limit:, target_key:, drain_slot: nil)
        target = delivery_target_for!(target_key)
        job = self.class.set(queue: target.queue_name)
        kwargs = { target_key: target.key }
        kwargs[:drain_slot] = drain_slot unless drain_slot.nil?
        kwargs[:limit] = limit unless limit.nil?

        job.perform_later(**kwargs)
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

      def repository_for_slot
        SearchEngine::PostgresOutbox::Repository.new
      end

      def drain_slots_table_exists?
        repository_for_slot.drain_slots_table_exists?
      end

      def worker_id
        @worker_id ||= "#{Socket.gethostname}:#{Process.pid}:#{job_id}"
      end

      def slot_summary(target_key, drain_slot)
        {
          claimed: 0,
          processed: 0,
          superseded: 0,
          retryable: 0,
          failed: 0,
          stale: 0,
          collections: [],
          target_key: target_key,
          drain_slot: drain_slot
        }
      end

      def stale_slot_summary(target_key, drain_slot)
        slot_summary(target_key, drain_slot).merge(stale_slot: true, batches: 0)
      end

      def merge_batch_summary!(summary, batch_summary)
        %i[claimed processed superseded retryable failed stale].each do |key|
          summary[key] += batch_summary[key].to_i
        end
        summary[:collections] |= Array(batch_summary[:collections])
      end

      def drain_job_max_batches
        [SearchEngine.config.postgres_outbox.drain_job_max_batches.to_i, 1].max
      end

      def drain_limit(limit)
        return limit unless limit.nil?
        return nil if SearchEngine.config.postgres_outbox.collection_batch_sizes?

        SearchEngine.config.postgres_outbox.batch_size
      end

      def runtime_budget_exhausted?
        max_runtime_s = SearchEngine.config.postgres_outbox.drain_job_max_runtime_s.to_i
        return false unless max_runtime_s.positive?

        Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at >= max_runtime_s
      end

      def started_at
        @started_at ||= Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end
    end
  end
end
