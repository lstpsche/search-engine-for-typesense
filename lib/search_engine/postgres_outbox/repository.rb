# frozen_string_literal: true

module SearchEngine
  module PostgresOutbox
    # Raw SQL repository for host-managed PostgreSQL outbox rows.
    class Repository
      ERROR_LIMIT = 1000

      # @param connection [Object, nil] ActiveRecord-compatible connection
      def initialize(connection: nil)
        @connection = connection
      end

      # Claim pending rows for one worker and return event objects.
      # @param limit [Integer]
      # @param worker_id [String]
      # @return [Array<SearchEngine::PostgresOutbox::Event>]
      def claim_pending(limit:, worker_id:)
        reset_stale_processing!
        rows = []

        connection.transaction do
          rows = select_rows(claim_select_sql(limit.to_i))
          ids = rows.map { |row| row_value(row, :id) }
          execute(claim_update_sql(ids, worker_id)) unless ids.empty?
        end

        rows.map { |row| Event.new(row) }
      end

      # Reset timed-out processing rows to pending.
      # @return [void]
      def reset_stale_processing!
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

      # @param event_ids [Array<Integer, String>]
      # @return [void]
      def mark_processed!(event_ids)
        update_status!(event_ids, 'processed', extra: 'processed_at = CURRENT_TIMESTAMP')
      end

      # @param event_ids [Array<Integer, String>]
      # @return [void]
      def mark_superseded!(event_ids)
        update_status!(event_ids, 'superseded', extra: 'processed_at = CURRENT_TIMESTAMP')
      end

      # @param event_ids [Array<Integer, String>]
      # @param error [Exception, String]
      # @return [void]
      def mark_retryable!(event_ids, error:)
        ids = Array(event_ids).compact
        return if ids.empty?

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
      end

      # @param event_ids [Array<Integer, String>]
      # @param error [Exception, String]
      # @return [void]
      def mark_failed!(event_ids, error:)
        ids = Array(event_ids).compact
        return if ids.empty?

        execute(<<~SQL)
          UPDATE #{quoted_table}
          SET status = 'failed',
              locked_at = NULL,
              locked_by = NULL,
              last_error = #{quote(truncate_error(error))},
              updated_at = CURRENT_TIMESTAMP
          WHERE id IN (#{ids_sql(ids)})
        SQL
      end

      private

      def connection
        @connection ||= begin
          require 'active_record'
          ActiveRecord::Base.connection
        end
      end

      def claim_select_sql(limit)
        <<~SQL
          SELECT *
          FROM #{quoted_table}
          WHERE status = 'pending'
            AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP)
          ORDER BY id ASC
          LIMIT #{limit}
          FOR UPDATE SKIP LOCKED
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
        SQL
      end

      def update_status!(event_ids, status, extra:)
        ids = Array(event_ids).compact
        return if ids.empty?

        execute(<<~SQL)
          UPDATE #{quoted_table}
          SET status = #{quote(status)},
              #{extra},
              locked_at = NULL,
              locked_by = NULL,
              updated_at = CURRENT_TIMESTAMP
          WHERE id IN (#{ids_sql(ids)})
        SQL
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

      def quoted_table
        connection.quote_table_name(SearchEngine.config.postgres_outbox.table_name)
      end

      def quote(value)
        connection.quote(value)
      end

      def row_value(row, key)
        row[key] || row[key.to_s]
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

      def truncate_error(error)
        message = error.respond_to?(:message) ? error.message : error.to_s
        message.to_s[0, ERROR_LIMIT]
      end
    end
  end
end
