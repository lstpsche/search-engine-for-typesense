# frozen_string_literal: true

module SearchEngine
  module PostgresOutbox
    # Raw SQL repository for host-managed PostgreSQL outbox rows.
    class Repository
      # Keep expensive duplicate-key ranking bounded while leaving headroom for hot keys.
      CLAIM_CANDIDATE_OVERSAMPLE = 4
      ERROR_LIMIT = 1000
      PROCESSED_AT_ASSIGNMENT = 'processed_at = CURRENT_TIMESTAMP'

      # @param connection [Object, nil] ActiveRecord-compatible connection
      # @param target_key [String, Symbol, nil] optional delivery target scope
      def initialize(connection: nil, target_key: nil)
        @connection = connection
        @target_key = normalize_optional_target_key(target_key)
      end

      # Claim pending rows for one worker and return event objects.
      # @param limit [Integer]
      # @param worker_id [String]
      # @return [Array<SearchEngine::PostgresOutbox::Event>]
      def claim_pending(limit:, worker_id:)
        return claim_pending_deliveries(limit: limit, worker_id: worker_id) if delivery_mode?

        reset_stale_processing!
        rows = []

        connection.transaction do
          rows = select_rows(claim_select_sql(limit))
          ids = rows.map { |row| row_value(row, :id) }
          execute(supersede_older_pending_sql(rows)) unless rows.empty?
          execute(claim_update_sql(ids, worker_id)) unless ids.empty?
        end

        rows.map { |row| Event.new(row) }
      end

      # Reset timed-out processing rows to pending.
      # @return [void]
      def reset_stale_processing!
        return reset_stale_delivery_processing! if delivery_mode?

        execute(<<~SQL)
          UPDATE #{quoted_table}
          SET status = 'pending',
              locked_at = NULL,
              locked_by = NULL,
              updated_at = CURRENT_TIMESTAMP
          WHERE status = 'processing'
            AND locked_at < (CURRENT_TIMESTAMP - interval '#{processing_timeout_s} seconds')
        SQL
      end

      # @param events [Array<SearchEngine::PostgresOutbox::Event, Integer, String>]
      # @return [Array<Integer, String>] event ids whose current lease was acknowledged
      def mark_processed!(events)
        return update_delivery_status!(events, 'processed', extra: PROCESSED_AT_ASSIGNMENT) if delivery_mode?

        update_status!(events, 'processed', extra: PROCESSED_AT_ASSIGNMENT)
      end

      # @param events [Array<SearchEngine::PostgresOutbox::Event, Integer, String>]
      # @return [Array<Integer, String>] event ids whose current lease was acknowledged
      def mark_superseded!(events)
        return update_delivery_status!(events, 'superseded', extra: PROCESSED_AT_ASSIGNMENT) if delivery_mode?

        update_status!(events, 'superseded', extra: PROCESSED_AT_ASSIGNMENT)
      end

      # @param events [Array<SearchEngine::PostgresOutbox::Event, Integer, String>]
      # @param error [Exception, String]
      # @return [Array<Integer, String>] event ids whose current lease was acknowledged
      def mark_retryable!(events, error:)
        ids = event_ids(events)
        return [] if ids.empty?
        return mark_delivery_retryable!(events, error: error) if delivery_mode?

        execute(<<~SQL)
          UPDATE #{quoted_table}
          SET attempts = attempts + 1,
              status = CASE WHEN attempts + 1 >= #{max_attempts} THEN 'failed' ELSE 'pending' END,
              next_attempt_at = CURRENT_TIMESTAMP + #{retry_interval_case_sql},
              locked_at = NULL,
              locked_by = NULL,
              last_error = #{quote(truncate_error(error))},
              updated_at = CURRENT_TIMESTAMP
          WHERE id IN (#{ids_sql(ids)})
        SQL
        ids
      end

      # @param events [Array<SearchEngine::PostgresOutbox::Event, Integer, String>]
      # @param error [Exception, String]
      # @return [Array<Integer, String>] event ids whose current lease was acknowledged
      def mark_failed!(events, error:)
        ids = event_ids(events)
        return [] if ids.empty?
        return mark_delivery_failed!(events, error: error) if delivery_mode?

        execute(<<~SQL)
          UPDATE #{quoted_table}
          SET status = 'failed',
              locked_at = NULL,
              locked_by = NULL,
              last_error = #{quote(truncate_error(error))},
              updated_at = CURRENT_TIMESTAMP
          WHERE id IN (#{ids_sql(ids)})
        SQL
        ids
      end

      # Refresh the lock timestamp only for deliveries still owned by these claims.
      # @param events [Array<SearchEngine::PostgresOutbox::Event>]
      # @return [Array<Integer, String>] event ids whose current lease was renewed
      def renew_leases!(events)
        raise ArgumentError, 'delivery lease renewal requires a target-scoped repository' unless delivery_mode?

        mutate_claimed_deliveries!(
          events,
          assignments: 'locked_at = CURRENT_TIMESTAMP',
          refresh_parent: false
        )
      end

      # Create missing delivery rows for all configured delivery targets.
      # @return [void]
      def materialize_deliveries!(limit: nil)
        targets = materialization_delivery_targets
        return if targets.empty?

        rows = []
        connection.transaction do
          rows = select_rows(delivery_materialization_select_sql(limit, targets))
          next if rows.empty?

          execute(materialization_supersede_older_deliveries_sql(rows, targets))
          execute(supersede_older_pending_sql(rows))
          execute(delivery_materialization_insert_sql(rows, targets))
        end

        rows
      end

      # Refresh cleanup-eligible parent event statuses from terminal delivery rows.
      #
      # This repairs historical residue where every delivery row for an event is
      # terminal but the parent event still has a non-terminal status. The
      # candidate set is bounded before aggregate work to keep cleanup safe on
      # large outbox tables.
      #
      # @param retention_s [Integer] retention window in seconds
      # @param limit [Integer, nil] maximum parent events to refresh
      # @return [Integer] refreshed parent event count
      def refresh_terminal_delivery_event_statuses!(retention_s:, limit: nil)
        return 0 unless delivery_table_exists?

        rows = select_rows(terminal_delivery_event_status_refresh_sql(retention_s: retention_s, limit: limit))

        rows.size
      end

      # Check whether the optional drain slot table exists.
      #
      # @return [Boolean]
      def drain_slots_table_exists?
        if connection.respond_to?(:data_source_exists?)
          connection.data_source_exists?(drain_slot_table_name)
        else
          connection.table_exists?(drain_slot_table_name)
        end
      end

      # Acquire idle drain slots for configured delivery targets.
      #
      # @param targets [Array<SearchEngine::PostgresOutbox::DeliveryTarget, Hash>]
      # @return [Array<Hash>] acquired slot descriptors
      def acquire_drain_slots!(targets:)
        normalized_targets = Array(targets).map { |target| DeliveryTarget.normalize(target) }
        return [] if normalized_targets.empty?

        connection.transaction do
          ensure_drain_slots!(normalized_targets)
          reset_stale_drain_slots!(normalized_targets)
          rows = select_rows(acquire_drain_slots_sql(normalized_targets))
          return rows.map { |row| drain_slot_descriptor(row) }
        end
      end

      # Mark an acquired drain slot as processing for the current worker.
      #
      # @param target_key [String, Symbol]
      # @param slot [Integer]
      # @param worker_id [String]
      # @return [Boolean] whether the queued slot was claimed by this worker
      def start_drain_slot!(target_key:, slot:, worker_id:)
        rows = select_rows(<<~SQL)
          UPDATE #{quoted_drain_slot_table}
          SET status = 'processing',
              locked_at = CURRENT_TIMESTAMP,
              locked_by = #{quote(worker_id)},
              started_at = CURRENT_TIMESTAMP,
              updated_at = CURRENT_TIMESTAMP
          WHERE target_key = #{quote(target_key)}
            AND slot = #{slot.to_i}
            AND status = 'queued'
          RETURNING target_key, slot
        SQL
        rows.any?
      end

      # Requeue a processing drain slot for a follow-up job.
      #
      # @param target_key [String, Symbol]
      # @param slot [Integer]
      # @param worker_id [String]
      # @return [Boolean] whether the current worker still owned and requeued the slot
      def requeue_drain_slot!(target_key:, slot:, worker_id:)
        rows = select_rows(<<~SQL)
          UPDATE #{quoted_drain_slot_table}
          SET status = 'queued',
              locked_at = CURRENT_TIMESTAMP,
              locked_by = NULL,
              enqueued_at = CURRENT_TIMESTAMP,
              updated_at = CURRENT_TIMESTAMP
          WHERE target_key = #{quote(target_key)}
            AND slot = #{slot.to_i}
            AND status = 'processing'
            AND locked_by = #{quote(worker_id)}
          RETURNING target_key, slot
        SQL
        rows.any?
      end

      # Release a drain slot back to idle.
      #
      # @param target_key [String, Symbol]
      # @param slot [Integer]
      # @param worker_id [String, nil]
      # @param error [Exception, String, nil]
      # @return [void]
      def release_drain_slot!(target_key:, slot:, worker_id: nil, error: nil)
        execute(<<~SQL)
          UPDATE #{quoted_drain_slot_table}
          SET status = 'idle',
              locked_at = NULL,
              locked_by = NULL,
              finished_at = CURRENT_TIMESTAMP,
              last_error = #{quote(error.nil? ? nil : truncate_error(error))},
              updated_at = CURRENT_TIMESTAMP
          WHERE target_key = #{quote(target_key)}
            AND slot = #{slot.to_i}
            #{drain_slot_owner_guard_sql(worker_id)}
        SQL
      end

      # Release a queued slot when a follow-up job could not be enqueued.
      #
      # @param target_key [String, Symbol]
      # @param slot [Integer]
      # @param error [Exception, String, nil]
      # @return [void]
      def release_requeued_drain_slot!(target_key:, slot:, error: nil)
        execute(<<~SQL)
          UPDATE #{quoted_drain_slot_table}
          SET status = 'idle',
              locked_at = NULL,
              locked_by = NULL,
              finished_at = CURRENT_TIMESTAMP,
              last_error = #{quote(error.nil? ? nil : truncate_error(error))},
              updated_at = CURRENT_TIMESTAMP
          WHERE target_key = #{quote(target_key)}
            AND slot = #{slot.to_i}
            AND status = 'queued'
        SQL
      end

      private

      attr_reader :target_key

      def connection
        @connection ||= begin
          require 'active_record'
          ActiveRecord::Base.connection
        end
      end

      def claim_pending_deliveries(limit:, worker_id:)
        reset_stale_delivery_processing!
        rows = claim_pending_delivery_rows(limit: limit, worker_id: worker_id)

        if rows.empty?
          materialize_deliveries!(limit: limit)
          rows = claim_pending_delivery_rows(limit: limit, worker_id: worker_id)
        end

        rows.map { |row| Event.new(row) }
      end

      def delivery_materialization_select_sql(limit, targets)
        return collection_limited_materialization_select_sql(targets) if collection_limited_batch?(limit)

        limit = global_limit_for(limit)
        <<~SQL
          WITH target(target_key, queue_name) AS (
            VALUES #{delivery_target_values_sql(targets)}
          ),
          candidate_events AS MATERIALIZED (
            SELECT outbox.id,
                   outbox.collection,
                   outbox.document_id
            FROM #{quoted_table} outbox
            WHERE outbox.status IN ('pending', 'processing', 'failed')
              AND (outbox.next_attempt_at IS NULL OR outbox.next_attempt_at <= CURRENT_TIMESTAMP)
              AND EXISTS (
                SELECT 1
                FROM target
                WHERE NOT EXISTS (
                  SELECT 1
                  FROM #{quoted_delivery_table} deliveries
                  WHERE deliveries.event_id = outbox.id
                    AND deliveries.target_key = target.target_key
                )
              )
            ORDER BY outbox.id ASC
            LIMIT #{limit}
            FOR UPDATE SKIP LOCKED
          ),
          latest_candidate_ids AS (
            SELECT id
            FROM (
              SELECT id,
                     ROW_NUMBER() OVER (
                       PARTITION BY collection, document_id
                       ORDER BY id DESC
                     ) AS row_number
              FROM candidate_events
            ) ranked_candidate_events
            WHERE row_number = 1
          )
          SELECT outbox.*
          FROM #{quoted_table} outbox
          INNER JOIN latest_candidate_ids
            ON latest_candidate_ids.id = outbox.id
          ORDER BY outbox.id ASC
        SQL
      end

      def delivery_materialization_insert_sql(rows, targets)
        <<~SQL
          INSERT INTO #{quoted_delivery_table} (
            event_id,
            target_key,
            queue_name,
            status,
            attempts,
            created_at,
            updated_at
          )
          SELECT outbox.id,
                 target.target_key,
                 target.queue_name,
                 'pending',
                 0,
                 CURRENT_TIMESTAMP,
                 CURRENT_TIMESTAMP
          FROM (
            VALUES #{materialization_event_values_sql(rows)}
          ) AS selected_events(event_id)
          INNER JOIN #{quoted_table} outbox
            ON outbox.id = selected_events.event_id
          CROSS JOIN (
            VALUES #{delivery_target_values_sql(targets)}
          ) AS target(target_key, queue_name)
          ON CONFLICT (event_id, target_key) DO NOTHING
        SQL
      end

      def claim_pending_delivery_rows(limit:, worker_id:)
        rows = []

        connection.transaction do
          rows = select_rows(delivery_claim_select_sql(limit))
          delivery_ids = rows.map { |row| row_value(row, :delivery_id) }
          execute(delivery_supersede_older_pending_sql(rows)) unless rows.empty?
          execute(delivery_claim_update_sql(delivery_ids, worker_id)) unless delivery_ids.empty?
        end

        rows.map { |row| row.merge('delivery_lease_owner' => worker_id) }
      end

      def reset_stale_delivery_processing!
        execute(<<~SQL)
          UPDATE #{quoted_delivery_table}
          SET status = 'pending',
              locked_at = NULL,
              locked_by = NULL,
              updated_at = CURRENT_TIMESTAMP
          WHERE target_key = #{quote(target_key)}
            AND status = 'processing'
            AND locked_at < (CURRENT_TIMESTAMP - interval '#{processing_timeout_s} seconds')
        SQL
      end

      def claim_select_sql(limit)
        return collection_limited_claim_select_sql if collection_limited_batch?(limit)

        limit = global_limit_for(limit)
        <<~SQL
          WITH candidate_events AS MATERIALIZED (
            SELECT outbox.id,
                   outbox.collection,
                   outbox.document_id
            FROM #{quoted_table} outbox
            WHERE outbox.status = 'pending'
              AND (outbox.next_attempt_at IS NULL OR outbox.next_attempt_at <= CURRENT_TIMESTAMP)
              #{processing_event_exclusion_sql('outbox')}
            ORDER BY outbox.id ASC
            LIMIT #{claim_candidate_limit(limit)}
            FOR UPDATE SKIP LOCKED
          ),
          ranked_candidate_events AS (
            SELECT id,
                   collection,
                   document_id,
                   ROW_NUMBER() OVER (
                     PARTITION BY collection, document_id
                     ORDER BY id DESC
                   ) AS row_number
            FROM candidate_events
          ),
          latest_due AS (
            SELECT latest_event.id
            FROM ranked_candidate_events
            #{latest_pending_event_lateral_sql('ranked_candidate_events')}
            WHERE ranked_candidate_events.row_number = 1
              AND (latest_event.next_attempt_at IS NULL OR latest_event.next_attempt_at <= CURRENT_TIMESTAMP)
            ORDER BY ranked_candidate_events.id ASC
            LIMIT #{limit}
          )
          SELECT outbox.*
          FROM #{quoted_table} outbox
          INNER JOIN latest_due
            ON latest_due.id = outbox.id
          ORDER BY outbox.id ASC
          FOR UPDATE OF outbox SKIP LOCKED
        SQL
      end

      def claim_update_sql(ids, worker_id)
        <<~SQL
          UPDATE #{quoted_table}
          SET status = 'processing',
              locked_at = CURRENT_TIMESTAMP,
              locked_by = #{quote(worker_id)},
              updated_at = CURRENT_TIMESTAMP
          WHERE id IN (#{ids_sql(ids)})
            AND status = 'pending'
        SQL
      end

      def delivery_claim_select_sql(limit)
        return collection_limited_delivery_claim_select_sql if collection_limited_batch?(limit)

        limit = global_limit_for(limit)
        <<~SQL
          WITH candidate_deliveries AS MATERIALIZED (
            SELECT deliveries.id AS delivery_id,
                   events.id AS event_id,
                   deliveries.target_key,
                   events.collection,
                   events.document_id
            FROM #{quoted_delivery_table} deliveries
            INNER JOIN #{quoted_table} events
              ON events.id = deliveries.event_id
            WHERE deliveries.target_key = #{quote(target_key)}
              AND deliveries.status = 'pending'
              AND (deliveries.next_attempt_at IS NULL OR deliveries.next_attempt_at <= CURRENT_TIMESTAMP)
              #{processing_delivery_exclusion_sql(delivery_relation: 'deliveries', event_relation: 'events')}
            ORDER BY deliveries.id ASC
            LIMIT #{claim_candidate_limit(limit)}
            FOR UPDATE OF deliveries SKIP LOCKED
          ),
          ranked_candidate_deliveries AS (
            SELECT delivery_id,
                   event_id,
                   target_key,
                   collection,
                   document_id,
                   ROW_NUMBER() OVER (
                     PARTITION BY target_key, collection, document_id
                     ORDER BY event_id DESC, delivery_id DESC
                   ) AS row_number
            FROM candidate_deliveries
          ),
          latest_due AS (
            SELECT latest_delivery.id
            FROM ranked_candidate_deliveries
            #{latest_pending_delivery_lateral_sql('ranked_candidate_deliveries')}
            WHERE ranked_candidate_deliveries.row_number = 1
              AND (latest_delivery.next_attempt_at IS NULL OR latest_delivery.next_attempt_at <= CURRENT_TIMESTAMP)
            ORDER BY ranked_candidate_deliveries.delivery_id ASC
            LIMIT #{limit}
          )
          SELECT events.*,
                 deliveries.id AS delivery_id,
                 deliveries.target_key,
                 deliveries.attempts AS delivery_attempts
          FROM #{quoted_delivery_table} deliveries
          INNER JOIN #{quoted_table} events
            ON events.id = deliveries.event_id
          INNER JOIN latest_due
            ON latest_due.id = deliveries.id
          ORDER BY deliveries.id ASC
          FOR UPDATE OF deliveries SKIP LOCKED
        SQL
      end

      def delivery_claim_update_sql(delivery_ids, worker_id)
        <<~SQL
          UPDATE #{quoted_delivery_table}
          SET status = 'processing',
              locked_at = CURRENT_TIMESTAMP,
              locked_by = #{quote(worker_id)},
              updated_at = CURRENT_TIMESTAMP
          WHERE id IN (#{ids_sql(delivery_ids)})
            AND target_key = #{quote(target_key)}
            AND status = 'pending'
        SQL
      end

      def materialization_supersede_older_deliveries_sql(rows, targets)
        <<~SQL
          WITH latest(collection, document_id, id) AS (
            VALUES #{coalesce_values_sql(rows)}
          ),
          target(target_key, queue_name) AS (
            VALUES #{delivery_target_values_sql(targets)}
          ),
          older_event_targets AS MATERIALIZED (
            SELECT older_events.id AS event_id,
                   target.target_key
            FROM latest
            CROSS JOIN target
            INNER JOIN #{quoted_table} older_events
              ON older_events.collection = latest.collection
             AND older_events.document_id = latest.document_id
             AND older_events.id < latest.id
          ),
          updated_deliveries AS (
            UPDATE #{quoted_delivery_table} older_deliveries
            SET status = 'superseded',
                processed_at = CURRENT_TIMESTAMP,
                locked_at = NULL,
                locked_by = NULL,
                updated_at = CURRENT_TIMESTAMP
            FROM older_event_targets
            WHERE older_deliveries.event_id = older_event_targets.event_id
              AND older_deliveries.status = 'pending'
              AND older_deliveries.target_key = older_event_targets.target_key
            RETURNING older_deliveries.event_id
          ),
          aggregate AS (
            #{event_status_aggregate_sql('SELECT event_id FROM updated_deliveries')}
          )
          UPDATE #{quoted_table} events
          SET status = aggregate.status,
              processed_at = #{aggregate_processed_at_sql},
              last_error = aggregate.last_error,
              updated_at = CURRENT_TIMESTAMP
          FROM aggregate
          WHERE events.id = aggregate.event_id
        SQL
      end

      def supersede_older_pending_sql(rows)
        <<~SQL
          UPDATE #{quoted_table} older
          SET status = 'superseded',
              processed_at = CURRENT_TIMESTAMP,
              locked_at = NULL,
              locked_by = NULL,
              updated_at = CURRENT_TIMESTAMP
          FROM (
            VALUES #{coalesce_values_sql(rows)}
          ) AS latest(collection, document_id, id)
          WHERE older.status = 'pending'
            AND older.collection = latest.collection
            AND older.document_id = latest.document_id
            AND older.id < latest.id
        SQL
      end

      def delivery_supersede_older_pending_sql(rows)
        <<~SQL
          WITH latest(target_key, collection, document_id, event_id, delivery_id) AS (
            VALUES #{delivery_coalesce_values_sql(rows)}
          ),
          older_event_targets AS MATERIALIZED (
            SELECT older_events.id AS event_id,
                   latest.target_key
            FROM latest
            INNER JOIN #{quoted_table} older_events
              ON older_events.collection = latest.collection
             AND older_events.document_id = latest.document_id
             AND older_events.id < latest.event_id
          ),
          updated_deliveries AS (
            UPDATE #{quoted_delivery_table} older_deliveries
            SET status = 'superseded',
                processed_at = CURRENT_TIMESTAMP,
                locked_at = NULL,
                locked_by = NULL,
                updated_at = CURRENT_TIMESTAMP
            FROM older_event_targets
            WHERE older_deliveries.event_id = older_event_targets.event_id
              AND older_deliveries.status = 'pending'
              AND older_deliveries.target_key = older_event_targets.target_key
            RETURNING older_deliveries.event_id
          ),
          aggregate AS (
            #{event_status_aggregate_sql('SELECT event_id FROM updated_deliveries')}
          )
          UPDATE #{quoted_table} events
          SET status = aggregate.status,
              processed_at = #{aggregate_processed_at_sql},
              last_error = aggregate.last_error,
              updated_at = CURRENT_TIMESTAMP
          FROM aggregate
          WHERE events.id = aggregate.event_id
        SQL
      end

      def update_status!(events, status, extra:)
        ids = event_ids(events)
        return [] if ids.empty?

        execute(<<~SQL)
          UPDATE #{quoted_table}
          SET status = #{quote(status)},
              #{extra},
              locked_at = NULL,
              locked_by = NULL,
              updated_at = CURRENT_TIMESTAMP
          WHERE id IN (#{ids_sql(ids)})
        SQL
        ids
      end

      def update_delivery_status!(events, status, extra:)
        mutate_claimed_deliveries!(
          events,
          assignments: <<~SQL.chomp
            status = #{quote(status)},
            #{extra},
            locked_at = NULL,
            locked_by = NULL
          SQL
        )
      end

      def mark_delivery_retryable!(events, error:)
        mutate_claimed_deliveries!(
          events,
          assignments: <<~SQL.chomp
            attempts = attempts + 1,
            status = CASE WHEN attempts + 1 >= #{max_attempts} THEN 'failed' ELSE 'pending' END,
            next_attempt_at = CURRENT_TIMESTAMP + #{retry_interval_case_sql},
            locked_at = NULL,
            locked_by = NULL,
            last_error = #{quote(truncate_error(error))}
          SQL
        )
      end

      def mark_delivery_failed!(events, error:)
        mutate_claimed_deliveries!(
          events,
          assignments: <<~SQL.chomp
            status = 'failed',
            locked_at = NULL,
            locked_by = NULL,
            last_error = #{quote(truncate_error(error))}
          SQL
        )
      end

      def mutate_claimed_deliveries!(events, assignments:, refresh_parent: true)
        claims = delivery_claims(events)
        return [] if claims.empty?

        connection.transaction do
          # Cross-path lock order is parent events first, then deliveries. Producers that mutate both
          # tables must use the same order to avoid deadlocks with acknowledgements.
          lock_event_rows!(claims.map { |claim| claim[:event_id] }) if refresh_parent
          rows = select_rows(<<~SQL)
            WITH claimed_deliveries(delivery_id, event_id, lease_owner) AS (
              VALUES #{delivery_claim_values_sql(claims)}
            )
            UPDATE #{quoted_delivery_table} deliveries
            SET #{assignments},
                updated_at = CURRENT_TIMESTAMP
            FROM claimed_deliveries
            WHERE deliveries.id = claimed_deliveries.delivery_id
              AND deliveries.event_id = claimed_deliveries.event_id
              AND deliveries.target_key = #{quote(target_key)}
              AND deliveries.status = 'processing'
              AND deliveries.locked_by = claimed_deliveries.lease_owner
            RETURNING deliveries.event_id AS id
          SQL

          ids = rows.map { |row| row_value(row, :id) }
          refresh_event_statuses!(ids) if refresh_parent && ids.any?
          ids
        end
      end

      def refresh_event_statuses!(event_ids)
        ids = Array(event_ids).compact
        return if ids.empty?

        execute(<<~SQL)
          UPDATE #{quoted_table} events
          SET status = aggregate.status,
              processed_at = #{aggregate_processed_at_sql},
              last_error = aggregate.last_error,
              updated_at = CURRENT_TIMESTAMP
          FROM (
            #{event_status_aggregate_sql(ids_sql(ids))}
          ) aggregate
          WHERE events.id = aggregate.event_id
        SQL
      end

      def lock_event_rows!(event_ids)
        execute(<<~SQL)
          SELECT id
          FROM #{quoted_table}
          WHERE id IN (#{ids_sql(event_ids)})
          ORDER BY id ASC
          FOR UPDATE
        SQL
      end

      def event_status_aggregate_sql(event_ids_sql)
        <<~SQL.chomp
          SELECT event_id,
                 CASE
                   WHEN COUNT(*) FILTER (WHERE status = 'failed') > 0 THEN 'failed'
                   WHEN COUNT(*) FILTER (WHERE status IN ('pending', 'processing')) > 0 THEN 'pending'
                   WHEN COUNT(*) FILTER (WHERE status = 'superseded') = COUNT(*) THEN 'superseded'
                   WHEN COUNT(*) FILTER (WHERE status = 'processed') > 0 THEN 'processed'
                   ELSE 'pending'
                 END AS status,
                 MAX(processed_at) FILTER (WHERE status IN ('processed', 'superseded')) AS terminal_processed_at,
                 (ARRAY_AGG(last_error ORDER BY updated_at DESC) FILTER (WHERE last_error IS NOT NULL))[1] AS last_error
          FROM #{quoted_delivery_table}
          WHERE event_id IN (#{event_ids_sql})
          GROUP BY event_id
        SQL
      end

      def terminal_delivery_event_status_refresh_sql(retention_s:, limit:)
        retention_seconds = [retention_s.to_i, 0].max
        batch_limit = global_limit_for(limit)

        <<~SQL
          WITH candidate_events AS MATERIALIZED (
            SELECT events.id
            FROM #{quoted_table} events
            WHERE events.status NOT IN ('processed', 'superseded')
              AND EXISTS (
                SELECT 1
                FROM #{quoted_delivery_table} deliveries
                WHERE deliveries.event_id = events.id
              )
              AND NOT EXISTS (
                SELECT 1
                FROM #{quoted_delivery_table} deliveries
                WHERE deliveries.event_id = events.id
                  AND deliveries.status IN ('pending', 'processing', 'failed')
              )
            ORDER BY events.id ASC
            LIMIT #{batch_limit}
            FOR UPDATE SKIP LOCKED
          ),
          eligible_events AS (
            SELECT candidate_events.id
            FROM candidate_events
            WHERE (
              SELECT MAX(deliveries.processed_at)
              FROM #{quoted_delivery_table} deliveries
              WHERE deliveries.event_id = candidate_events.id
                AND deliveries.status IN ('processed', 'superseded')
            ) < (CURRENT_TIMESTAMP - interval '#{retention_seconds} seconds')
          ),
          aggregate AS (
            #{event_status_aggregate_sql('SELECT id FROM eligible_events')}
          ),
          updated_events AS (
            UPDATE #{quoted_table} events
            SET status = aggregate.status,
                processed_at = #{aggregate_processed_at_sql},
                last_error = aggregate.last_error,
                updated_at = CURRENT_TIMESTAMP
            FROM aggregate
            WHERE events.id = aggregate.event_id
            RETURNING events.id
          )
          SELECT id
          FROM updated_events
        SQL
      end

      def aggregate_processed_at_sql
        "CASE WHEN aggregate.status IN ('processed', 'superseded') " \
          'THEN COALESCE(aggregate.terminal_processed_at, CURRENT_TIMESTAMP) ELSE NULL END'
      end

      def select_rows(sql)
        result = connection.select_all(sql)
        return result.to_a if result.respond_to?(:to_a)

        Array(result)
      end

      def execute(sql)
        connection.execute(sql)
      end

      def ids_sql(ids)
        ids.map { |id| quote(id) }.join(', ')
      end

      def event_ids(events)
        Array(events).compact.map do |event|
          event.respond_to?(:id) ? event.id : event
        end
      end

      def delivery_claims(events)
        Array(events).compact.map do |event|
          unless event.respond_to?(:delivery_id) && event.respond_to?(:delivery_lease_owner)
            raise ArgumentError, 'target-scoped delivery mutations require claimed Event objects'
          end
          if event.delivery_id.nil? || event.delivery_lease_owner.to_s.empty?
            raise ArgumentError, 'claimed delivery is missing its delivery id or lease owner'
          end

          if event.target_key.to_s != target_key
            raise ArgumentError,
                  "claimed delivery target #{event.target_key.inspect} does not match #{target_key.inspect}"
          end

          {
            delivery_id: event.delivery_id,
            event_id: event.id,
            lease_owner: event.delivery_lease_owner
          }
        end
      end

      def delivery_claim_values_sql(claims)
        claims.map do |claim|
          "(#{quote(claim[:delivery_id])}::bigint, " \
            "#{quote(claim[:event_id])}::bigint, #{quote(claim[:lease_owner])})"
        end.join(', ')
      end

      def coalesce_values_sql(rows)
        rows.map do |row|
          collection = row_value(row, :collection)
          document_id = row_value(row, :document_id)
          id = row_value(row, :id)

          "(#{quote(collection)}, #{quote(document_id)}, #{quote(id)})"
        end.join(', ')
      end

      def delivery_coalesce_values_sql(rows)
        rows.map do |row|
          target = row_value(row, :target_key)
          collection = row_value(row, :collection)
          document_id = row_value(row, :document_id)
          event_id = row_value(row, :id)
          delivery_id = row_value(row, :delivery_id)

          "(#{quote(target)}, #{quote(collection)}, #{quote(document_id)}, #{quote(event_id)}, #{quote(delivery_id)})"
        end.join(', ')
      end

      def delivery_target_values_sql(targets)
        targets.map do |target|
          "(#{quote(target.key)}, #{quote(target.queue_name)})"
        end.join(', ')
      end

      def materialization_event_values_sql(rows)
        rows.map do |row|
          "(#{quote(row_value(row, :id))})"
        end.join(', ')
      end

      def ensure_drain_slots!(targets)
        targets.each do |target|
          execute(<<~SQL)
            INSERT INTO #{quoted_drain_slot_table} (
              target_key,
              slot,
              queue_name,
              status,
              created_at,
              updated_at
            )
            SELECT #{quote(target.key)},
                   slot,
                   #{quote(target.queue_name)},
                   'idle',
                   CURRENT_TIMESTAMP,
                   CURRENT_TIMESTAMP
            FROM generate_series(1, #{target.parallelism}) AS slot
            ON CONFLICT (target_key, slot) DO UPDATE
            SET queue_name = EXCLUDED.queue_name,
                updated_at = #{quoted_drain_slot_table}.updated_at
          SQL
        end
      end

      def reset_stale_drain_slots!(targets)
        execute(<<~SQL)
          UPDATE #{quoted_drain_slot_table}
          SET status = 'idle',
              locked_at = NULL,
              locked_by = NULL,
              last_error = NULL,
              updated_at = CURRENT_TIMESTAMP
          WHERE target_key IN (#{ids_sql(targets.map(&:key))})
            AND status IN ('queued', 'processing')
            AND locked_at < (CURRENT_TIMESTAMP - interval '#{processing_timeout_s} seconds')
        SQL
      end

      def acquire_drain_slots_sql(targets)
        <<~SQL
          WITH target(target_key, queue_name, parallelism) AS (
            VALUES #{drain_slot_target_values_sql(targets)}
          ),
          available AS (
            SELECT slots.id
            FROM #{quoted_drain_slot_table} slots
            INNER JOIN target
              ON target.target_key = slots.target_key
            WHERE slots.status = 'idle'
              AND slots.slot <= target.parallelism
            ORDER BY slots.target_key ASC, slots.slot ASC
            FOR UPDATE SKIP LOCKED
          ),
          updated AS (
            UPDATE #{quoted_drain_slot_table} slots
            SET status = 'queued',
                locked_at = CURRENT_TIMESTAMP,
                locked_by = NULL,
                enqueued_at = CURRENT_TIMESTAMP,
                last_error = NULL,
                updated_at = CURRENT_TIMESTAMP
            FROM available
            WHERE slots.id = available.id
            RETURNING slots.target_key, slots.slot, slots.queue_name
          )
          SELECT target_key, slot, queue_name
          FROM updated
          ORDER BY target_key ASC, slot ASC
        SQL
      end

      def drain_slot_target_values_sql(targets)
        targets.map do |target|
          "(#{quote(target.key)}, #{quote(target.queue_name)}, #{target.parallelism})"
        end.join(', ')
      end

      def drain_slot_descriptor(row)
        {
          target_key: row_value(row, :target_key).to_s,
          slot: row_value(row, :slot).to_i,
          queue_name: row_value(row, :queue_name).to_s
        }
      end

      def drain_slot_owner_guard_sql(worker_id)
        return '' if worker_id.nil?

        "AND locked_by = #{quote(worker_id)}"
      end

      def quoted_table
        connection.quote_table_name(SearchEngine.config.postgres_outbox.table_name)
      end

      def quoted_delivery_table
        connection.quote_table_name(SearchEngine.config.postgres_outbox.delivery_table_name)
      end

      def quoted_drain_slot_table
        connection.quote_table_name(drain_slot_table_name)
      end

      def drain_slot_table_name
        SearchEngine.config.postgres_outbox.drain_slot_table_name
      end

      def delivery_table_exists?
        table_name = SearchEngine.config.postgres_outbox.delivery_table_name
        if connection.respond_to?(:data_source_exists?)
          connection.data_source_exists?(table_name)
        else
          connection.table_exists?(table_name)
        end
      end

      def quote(value)
        connection.quote(value)
      end

      def row_value(row, key)
        row[key] || row[key.to_s]
      end

      def delivery_mode?
        !target_key.nil?
      end

      def normalize_optional_target_key(value)
        normalized = value&.to_s
        return nil if normalized.nil? || normalized.strip.empty?

        normalized
      end

      def delivery_targets
        configured = SearchEngine.config.postgres_outbox.delivery_targets
        raw_targets = configured.respond_to?(:call) ? configured.call : configured
        Array(raw_targets).map { |target| DeliveryTarget.normalize(target) }
      end

      def materialization_delivery_targets
        targets = delivery_targets
        return targets unless delivery_mode?

        targets.select { |target| target.key == target_key }
      end

      def max_attempts
        SearchEngine.config.postgres_outbox.max_attempts.to_i
      end

      def processing_timeout_s
        SearchEngine.config.postgres_outbox.processing_timeout_s.to_i
      end

      def retry_interval_case_sql
        clauses = (1..max_attempts).map do |attempt|
          "WHEN #{attempt} THEN interval '#{retry_delay_s(attempt)} seconds'"
        end

        "CASE attempts + 1 #{clauses.join(' ')} ELSE interval '#{retry_delay_s(max_attempts)} seconds' END"
      end

      def retry_delay_s(attempt)
        backoff = SearchEngine.config.postgres_outbox.retry_backoff
        delay = backoff.respond_to?(:call) ? backoff.call(attempt) : backoff
        [delay.to_i, 0].max
      end

      def collection_limited_claim_select_sql
        <<~SQL
          WITH #{collection_limits_cte_sql},
          candidate_events AS MATERIALIZED (
            SELECT outbox.id,
                   outbox.collection,
                   outbox.document_id
            FROM #{quoted_table} outbox
            WHERE outbox.status = 'pending'
              AND (outbox.next_attempt_at IS NULL OR outbox.next_attempt_at <= CURRENT_TIMESTAMP)
              #{processing_event_exclusion_sql('outbox')}
            ORDER BY outbox.id ASC
            LIMIT #{claim_candidate_limit(collection_limited_candidate_limit)}
            FOR UPDATE SKIP LOCKED
          ),
          ranked_candidate_events AS (
            SELECT id,
                   collection,
                   document_id,
                   ROW_NUMBER() OVER (
                     PARTITION BY collection, document_id
                     ORDER BY id DESC
                   ) AS row_number
            FROM candidate_events
          ),
          latest_due AS (
            SELECT latest_event.id,
                   ranked_candidate_events.collection
            FROM ranked_candidate_events
            #{latest_pending_event_lateral_sql('ranked_candidate_events')}
            WHERE ranked_candidate_events.row_number = 1
              AND (latest_event.next_attempt_at IS NULL OR latest_event.next_attempt_at <= CURRENT_TIMESTAMP)
          ),
          ranked_by_collection AS (
            SELECT latest_due.id,
                   ROW_NUMBER() OVER (
                     PARTITION BY latest_due.collection
                     ORDER BY latest_due.id ASC
                   ) AS collection_row_number,
                   COALESCE(collection_limits.batch_size, #{global_limit_for(nil)}) AS collection_batch_size
            FROM latest_due
            LEFT JOIN collection_limits
              ON collection_limits.collection = latest_due.collection
          ),
          selected_due AS (
            SELECT id
            FROM ranked_by_collection
            WHERE collection_row_number <= collection_batch_size
            ORDER BY id ASC
            LIMIT #{collection_limited_candidate_limit}
          )
          SELECT outbox.*
          FROM #{quoted_table} outbox
          INNER JOIN selected_due
            ON selected_due.id = outbox.id
          ORDER BY outbox.id ASC
          FOR UPDATE OF outbox SKIP LOCKED
        SQL
      end

      def collection_limited_materialization_select_sql(targets)
        <<~SQL
          WITH target(target_key, queue_name) AS (
            VALUES #{delivery_target_values_sql(targets)}
          ),
          #{collection_limits_cte_sql},
          candidate_events AS MATERIALIZED (
            SELECT outbox.id,
                   outbox.collection,
                   outbox.document_id
            FROM #{quoted_table} outbox
            WHERE outbox.status IN ('pending', 'processing', 'failed')
              AND (outbox.next_attempt_at IS NULL OR outbox.next_attempt_at <= CURRENT_TIMESTAMP)
              AND EXISTS (
                SELECT 1
                FROM target
                WHERE NOT EXISTS (
                  SELECT 1
                  FROM #{quoted_delivery_table} deliveries
                  WHERE deliveries.event_id = outbox.id
                    AND deliveries.target_key = target.target_key
                )
            )
            ORDER BY outbox.id ASC
            LIMIT #{collection_limited_candidate_limit}
            FOR UPDATE SKIP LOCKED
          ),
          latest_candidate_ids AS (
            SELECT id,
                   collection
            FROM (
              SELECT id,
                     collection,
                     ROW_NUMBER() OVER (
                       PARTITION BY collection, document_id
                       ORDER BY id DESC
                     ) AS row_number
              FROM candidate_events
            ) ranked_candidate_events
            WHERE row_number = 1
          ),
          ranked_by_collection AS (
            SELECT latest_candidate_ids.id,
                   ROW_NUMBER() OVER (
                     PARTITION BY latest_candidate_ids.collection
                     ORDER BY latest_candidate_ids.id ASC
                   ) AS collection_row_number,
                   COALESCE(collection_limits.batch_size, #{global_limit_for(nil)}) AS collection_batch_size
            FROM latest_candidate_ids
            LEFT JOIN collection_limits
              ON collection_limits.collection = latest_candidate_ids.collection
          ),
          selected_candidate_ids AS (
            SELECT id
            FROM ranked_by_collection
            WHERE collection_row_number <= collection_batch_size
          )
          SELECT outbox.*
          FROM #{quoted_table} outbox
          INNER JOIN selected_candidate_ids
            ON selected_candidate_ids.id = outbox.id
          ORDER BY outbox.id ASC
        SQL
      end

      def collection_limited_delivery_claim_select_sql
        <<~SQL
          WITH #{collection_limits_cte_sql},
          candidate_deliveries AS MATERIALIZED (
            SELECT deliveries.id AS delivery_id,
                   events.id AS event_id,
                   deliveries.target_key,
                   events.collection,
                   events.document_id
            FROM #{quoted_delivery_table} deliveries
            INNER JOIN #{quoted_table} events
              ON events.id = deliveries.event_id
            WHERE deliveries.target_key = #{quote(target_key)}
              AND deliveries.status = 'pending'
              AND (deliveries.next_attempt_at IS NULL OR deliveries.next_attempt_at <= CURRENT_TIMESTAMP)
              #{processing_delivery_exclusion_sql(delivery_relation: 'deliveries', event_relation: 'events')}
            ORDER BY deliveries.id ASC
            LIMIT #{claim_candidate_limit(collection_limited_candidate_limit)}
            FOR UPDATE OF deliveries SKIP LOCKED
          ),
          ranked_candidate_deliveries AS (
            SELECT delivery_id,
                   event_id,
                   target_key,
                   collection,
                   document_id,
                   ROW_NUMBER() OVER (
                     PARTITION BY target_key, collection, document_id
                     ORDER BY event_id DESC, delivery_id DESC
                   ) AS row_number
            FROM candidate_deliveries
          ),
          latest_due AS (
            SELECT latest_delivery.id,
                   ranked_candidate_deliveries.collection
            FROM ranked_candidate_deliveries
            #{latest_pending_delivery_lateral_sql('ranked_candidate_deliveries')}
            WHERE ranked_candidate_deliveries.row_number = 1
              AND (latest_delivery.next_attempt_at IS NULL OR latest_delivery.next_attempt_at <= CURRENT_TIMESTAMP)
          ),
          ranked_by_collection AS (
            SELECT latest_due.id,
                   ROW_NUMBER() OVER (
                     PARTITION BY latest_due.collection
                     ORDER BY latest_due.id ASC
                   ) AS collection_row_number,
                   COALESCE(collection_limits.batch_size, #{global_limit_for(nil)}) AS collection_batch_size
            FROM latest_due
            LEFT JOIN collection_limits
              ON collection_limits.collection = latest_due.collection
          ),
          selected_due AS (
            SELECT id
            FROM ranked_by_collection
            WHERE collection_row_number <= collection_batch_size
            ORDER BY id ASC
            LIMIT #{collection_limited_candidate_limit}
          )
          SELECT events.*,
                 deliveries.id AS delivery_id,
                 deliveries.target_key,
                 deliveries.attempts AS delivery_attempts
          FROM #{quoted_delivery_table} deliveries
          INNER JOIN #{quoted_table} events
            ON events.id = deliveries.event_id
          INNER JOIN selected_due
            ON selected_due.id = deliveries.id
          ORDER BY deliveries.id ASC
          FOR UPDATE OF deliveries SKIP LOCKED
        SQL
      end

      def collection_limited_batch?(limit)
        limit.nil? && SearchEngine.config.postgres_outbox.collection_batch_sizes?
      end

      def global_limit_for(limit)
        (limit || SearchEngine.config.postgres_outbox.batch_size).to_i
      end

      def collection_limited_candidate_limit
        batch_size_sum = SearchEngine.config.postgres_outbox.normalized_batch_sizes.values.sum
        [global_limit_for(nil), 1].max + batch_size_sum
      end

      def claim_candidate_limit(limit)
        [limit.to_i, 0].max * CLAIM_CANDIDATE_OVERSAMPLE
      end

      def processing_event_exclusion_sql(event_relation)
        <<~SQL.chomp
          AND NOT EXISTS (
            SELECT 1
            FROM #{quoted_table} processing_events
            WHERE processing_events.status = 'processing'
              AND processing_events.collection = #{event_relation}.collection
              AND processing_events.document_id = #{event_relation}.document_id
          )
        SQL
      end

      def processing_delivery_exclusion_sql(delivery_relation:, event_relation:)
        <<~SQL.chomp
          AND NOT EXISTS (
            SELECT 1
            FROM #{quoted_delivery_table} processing_deliveries
            INNER JOIN #{quoted_table} processing_events
              ON processing_events.id = processing_deliveries.event_id
            WHERE processing_deliveries.target_key = #{delivery_relation}.target_key
              AND processing_deliveries.status = 'processing'
              AND processing_events.collection = #{event_relation}.collection
              AND processing_events.document_id = #{event_relation}.document_id
          )
        SQL
      end

      # Resolve a bounded candidate key to its globally latest pending event. Without this lookup,
      # a hot key whose newest row falls outside the candidate pool can be claimed out of order or stall forever.
      def latest_pending_event_lateral_sql(candidate_relation)
        <<~SQL.chomp
          CROSS JOIN LATERAL (
            SELECT latest_pending.id,
                   latest_pending.next_attempt_at
            FROM #{quoted_table} latest_pending
            WHERE latest_pending.collection = #{candidate_relation}.collection
              AND latest_pending.document_id = #{candidate_relation}.document_id
              AND latest_pending.status = 'pending'
            ORDER BY latest_pending.id DESC
            LIMIT 1
          ) latest_event
        SQL
      end

      # Delivery rows are ordered by their parent event id so every target resolves the same latest document version.
      def latest_pending_delivery_lateral_sql(candidate_relation)
        <<~SQL.chomp
          CROSS JOIN LATERAL (
            SELECT latest_deliveries.id,
                   latest_deliveries.next_attempt_at
            FROM #{quoted_table} latest_events
            INNER JOIN #{quoted_delivery_table} latest_deliveries
              ON latest_deliveries.event_id = latest_events.id
            WHERE latest_deliveries.target_key = #{candidate_relation}.target_key
              AND latest_deliveries.status = 'pending'
              AND latest_events.collection = #{candidate_relation}.collection
              AND latest_events.document_id = #{candidate_relation}.document_id
            ORDER BY latest_events.id DESC, latest_deliveries.id DESC
            LIMIT 1
          ) latest_delivery
        SQL
      end

      def collection_limits_cte_sql
        "collection_limits(collection, batch_size) AS (VALUES #{collection_batch_size_values_sql})"
      end

      def collection_batch_size_values_sql
        SearchEngine.config.postgres_outbox.normalized_batch_sizes.sort.map do |collection, size|
          "(#{quote(collection)}, #{size})"
        end.join(', ')
      end

      def truncate_error(error)
        message = error.respond_to?(:message) ? error.message : error.to_s
        message.to_s[0, ERROR_LIMIT]
      end
    end
  end
end
