# frozen_string_literal: true

module SearchEngine
  module PostgresOutbox
    # Rails migration helpers for installing PostgreSQL outbox tables and triggers.
    #
    # Custom SQL expressions passed to trigger helpers are trusted migration code
    # and may refer to the local PL/pgSQL variable `record_data`.
    module MigrationHelpers
      ALLOWED_TRIGGER_OPERATIONS = {
        'insert' => 'INSERT',
        'update' => 'UPDATE',
        'delete' => 'DELETE'
      }.freeze

      # Create the durable SearchEngine outbox events table.
      #
      # @param table_name [String, Symbol] destination table name
      # @return [void]
      def create_search_engine_outbox_events(table_name: SearchEngine.config.postgres_outbox.table_name)
        create_table table_name do |t|
          t.string :source_table, null: false
          t.string :source_model_name, null: false
          t.string :collection, null: false
          t.string :record_id, null: false
          t.string :document_id, null: false
          t.string :operation, null: false
          t.string :status, null: false, default: 'pending'
          t.integer :attempts, null: false, default: 0
          t.datetime :locked_at
          t.datetime :next_attempt_at
          t.datetime :processed_at
          t.string :locked_by
          t.jsonb :payload, null: false, default: {}
          t.text :last_error
          t.timestamps
        end

        add_index table_name,
                  %i[status next_attempt_at id],
                  name: 'idx_search_engine_outbox_pending'
        add_index table_name,
                  %i[collection document_id status id],
                  name: 'idx_search_engine_outbox_coalescing'
        add_index table_name,
                  :locked_at,
                  name: 'idx_search_engine_outbox_processing',
                  where: "status = 'processing'"
        add_index table_name,
                  :processed_at,
                  name: 'idx_search_engine_outbox_cleanup',
                  where: "status IN ('processed', 'superseded')"
      end

      # Create or replace a row-level PostgreSQL trigger that writes outbox events.
      #
      # @param table_name [String, Symbol] source table name
      # @param source_model [String, Symbol, Class] host model name recorded in each event
      # @param collection [String, Symbol] Typesense collection recorded in each event
      # @param record_id_sql [String, nil] trusted SQL expression for source record id
      # @param document_id_sql [String, nil] trusted SQL expression for Typesense document id
      # @param operations [Array<Symbol, String>] trigger operations: insert, update, delete
      # @param channel [String] PostgreSQL notification channel
      # @param outbox_table [String, Symbol] outbox table name
      # @return [void]
      def create_search_engine_outbox_trigger(
        table_name,
        source_model:,
        collection:,
        record_id_sql: nil,
        document_id_sql: nil,
        operations: %i[insert update delete],
        channel: SearchEngine.config.postgres_outbox.channel,
        outbox_table: SearchEngine.config.postgres_outbox.table_name
      )
        normalized_operations = normalize_search_engine_outbox_operations(operations)
        record_id_sql ||= 'record_data.id::text'
        document_id_sql ||= record_id_sql

        execute search_engine_outbox_trigger_sql(
          table_name: table_name,
          source_model: source_model,
          collection: collection,
          record_id_sql: record_id_sql,
          document_id_sql: document_id_sql,
          operations: normalized_operations,
          channel: channel,
          outbox_table: outbox_table
        )
      end

      # Drop the SearchEngine outbox trigger and function for a source table.
      #
      # @param table_name [String, Symbol] source table name
      # @return [void]
      def drop_search_engine_outbox_trigger(table_name)
        names = search_engine_outbox_trigger_names(table_name)

        execute <<~SQL
          DROP TRIGGER IF EXISTS #{connection.quote_table_name(names.fetch(:trigger))}
          ON #{connection.quote_table_name(table_name)};
          DROP FUNCTION IF EXISTS #{connection.quote_table_name(names.fetch(:function))}();
        SQL
      end

      private

      # rubocop:disable Metrics/MethodLength
      def search_engine_outbox_trigger_sql(
        table_name:,
        source_model:,
        collection:,
        record_id_sql:,
        document_id_sql:,
        operations:,
        channel:,
        outbox_table:
      )
        names = search_engine_outbox_trigger_names(table_name)
        quoted_outbox_table = connection.quote_table_name(outbox_table)
        quoted_function_name = connection.quote_table_name(names.fetch(:function))

        <<~SQL
          CREATE OR REPLACE FUNCTION #{quoted_function_name}()
          RETURNS trigger
          LANGUAGE plpgsql
          AS $$
          DECLARE
            record_data record;
            event_id bigint;
            event_operation text;
          BEGIN
            IF TG_OP = 'DELETE' THEN
              record_data := OLD;
              event_operation := 'delete';
            ELSE
              record_data := NEW;
              event_operation := 'upsert';
            END IF;

            INSERT INTO #{quoted_outbox_table} (
              source_table,
              source_model_name,
              collection,
              record_id,
              document_id,
              operation,
              status,
              payload,
              created_at,
              updated_at
            )
            VALUES (
              TG_TABLE_NAME,
              #{connection.quote(source_model.to_s)},
              #{connection.quote(collection.to_s)},
              (#{record_id_sql}),
              (#{document_id_sql}),
              event_operation,
              'pending',
              jsonb_build_object('trigger_operation', TG_OP),
              CURRENT_TIMESTAMP,
              CURRENT_TIMESTAMP
            )
            RETURNING id INTO event_id;

            PERFORM pg_notify(
              #{connection.quote(channel.to_s)},
              json_build_object(
                'id', event_id,
                'table', TG_TABLE_NAME,
                'collection', #{connection.quote(collection.to_s)}
              )::text
            );

            IF TG_OP = 'DELETE' THEN
              RETURN OLD;
            END IF;

            RETURN NEW;
          END;
          $$;

          #{search_engine_outbox_create_trigger_sql(
            names: names,
            table_name: table_name,
            operations: operations,
            quoted_function_name: quoted_function_name
          )}
        SQL
      end
      # rubocop:enable Metrics/MethodLength

      def search_engine_outbox_create_trigger_sql(names:, table_name:, operations:, quoted_function_name:)
        quoted_source_table = connection.quote_table_name(table_name)
        quoted_trigger_name = connection.quote_table_name(names.fetch(:trigger))

        <<~SQL
          DROP TRIGGER IF EXISTS #{quoted_trigger_name} ON #{quoted_source_table};

          CREATE TRIGGER #{quoted_trigger_name}
          AFTER #{operations.join(' OR ')} ON #{quoted_source_table}
          FOR EACH ROW
          EXECUTE FUNCTION #{quoted_function_name}();
        SQL
      end

      def normalize_search_engine_outbox_operations(operations)
        normalized = Array(operations).map do |operation|
          ALLOWED_TRIGGER_OPERATIONS[operation.to_s.downcase]
        end

        raise ArgumentError, 'operations must include at least one of: insert, update, delete' if normalized.empty?
        raise ArgumentError, 'operations may only include: insert, update, delete' if normalized.any?(&:nil?)

        normalized.uniq
      end

      def search_engine_outbox_trigger_names(table_name)
        suffix = table_name.to_s.gsub(/\W+/, '_').gsub(/\A_+|_+\z/, '')

        {
          function: "search_engine_outbox_#{suffix}_fn",
          trigger: "search_engine_outbox_#{suffix}_trigger"
        }
      end
    end
  end
end
