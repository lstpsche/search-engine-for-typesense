# frozen_string_literal: true

require 'test_helper'

class PostgresOutboxMigrationHelpersTest < Minitest::Test
  class FakeConnection
    def quote(value)
      "'#{value.to_s.gsub("'", "''")}'"
    end

    def quote_table_name(value)
      value.to_s.split('.').map { |part| %("#{part.gsub('"', '""')}") }.join('.')
    end
  end

  class FakeTable
    attr_reader :columns

    def initialize
      @columns = []
    end

    def method_missing(name, *args, **kwargs)
      columns << [name, args, kwargs]
    end

    def respond_to_missing?(_name, _include_private = false)
      true
    end
  end

  class FakeMigration
    include SearchEngine::PostgresOutbox::MigrationHelpers

    attr_reader :created_tables, :foreign_keys, :indexes, :executed_sql

    def initialize
      @created_tables = {}
      @foreign_keys = []
      @indexes = []
      @executed_sql = []
    end

    def connection
      @connection ||= FakeConnection.new
    end

    def create_table(name)
      table = FakeTable.new
      yield table
      created_tables[name] = table
    end

    def add_index(table, columns, **options)
      indexes << [table, columns, options]
    end

    def add_foreign_key(from_table, to_table, **options)
      foreign_keys << [from_table, to_table, options]
    end

    def execute(sql)
      executed_sql << sql
    end
  end

  def setup
    @migration = FakeMigration.new
  end

  def test_create_search_engine_outbox_events_creates_columns_and_indexes
    @migration.create_search_engine_outbox_events(table_name: :custom_outbox_events)

    table = @migration.created_tables.fetch(:custom_outbox_events)

    assert_includes table.columns, [:string, [:source_table], { null: false }]
    assert_includes table.columns, [:string, [:source_model_name], { null: false }]
    assert_includes table.columns, [:string, [:collection], { null: false }]
    assert_includes table.columns, [:string, [:record_id], { null: false }]
    assert_includes table.columns, [:string, [:document_id], { null: false }]
    assert_includes table.columns, [:string, [:operation], { null: false }]
    assert_includes table.columns, [:string, [:status], { null: false, default: 'pending' }]
    assert_includes table.columns, [:integer, [:attempts], { null: false, default: 0 }]
    assert_includes table.columns, [:jsonb, [:payload], { null: false, default: {} }]
    assert_includes table.columns, [:text, [:last_error], {}]
    assert_includes table.columns, [:timestamps, [], {}]

    assert_includes @migration.indexes,
                    [:custom_outbox_events, %i[status next_attempt_at id],
                     { name: 'idx_search_engine_outbox_pending' }]
    assert_includes @migration.indexes,
                    [:custom_outbox_events, %i[collection document_id status id],
                     { name: 'idx_search_engine_outbox_coalescing' }]
    assert_includes @migration.indexes,
                    [:custom_outbox_events, :locked_at,
                     { name: 'idx_search_engine_outbox_processing', where: "status = 'processing'" }]
    assert_includes @migration.indexes,
                    [:custom_outbox_events, :processed_at,
                     { name: 'idx_search_engine_outbox_cleanup', where: "status IN ('processed', 'superseded')" }]
  end

  def test_create_search_engine_outbox_deliveries_creates_columns_indexes_and_foreign_key
    @migration.create_search_engine_outbox_deliveries(
      table_name: :custom_outbox_deliveries,
      events_table_name: :custom_outbox_events
    )

    table = @migration.created_tables.fetch(:custom_outbox_deliveries)

    assert_includes table.columns, [:bigint, [:event_id], { null: false }]
    assert_includes table.columns, [:string, [:target_key], { null: false }]
    assert_includes table.columns, [:string, [:queue_name], { null: false }]
    assert_includes table.columns, [:string, [:status], { null: false, default: 'pending' }]
    assert_includes table.columns, [:integer, [:attempts], { null: false, default: 0 }]
    assert_includes table.columns, [:datetime, [:locked_at], {}]
    assert_includes table.columns, [:datetime, [:next_attempt_at], {}]
    assert_includes table.columns, [:datetime, [:processed_at], {}]
    assert_includes table.columns, [:string, [:locked_by], {}]
    assert_includes table.columns, [:text, [:last_error], {}]
    assert_includes table.columns, [:timestamps, [], {}]

    assert_includes @migration.indexes,
                    [:custom_outbox_deliveries, %i[event_id target_key],
                     { name: 'idx_se_outbox_deliveries_unique', unique: true }]
    assert_includes @migration.indexes,
                    [:custom_outbox_deliveries, %i[target_key status next_attempt_at id],
                     { name: 'idx_se_outbox_deliveries_pending' }]
    assert_includes @migration.indexes,
                    [:custom_outbox_deliveries, %i[target_key status event_id],
                     { name: 'idx_se_outbox_deliveries_coalescing' }]
    assert_includes @migration.indexes,
                    [:custom_outbox_deliveries, :locked_at,
                     { name: 'idx_se_outbox_deliveries_processing', where: "status = 'processing'" }]
    assert_includes @migration.indexes,
                    [:custom_outbox_deliveries, :processed_at,
                     { name: 'idx_se_outbox_deliveries_cleanup', where: "status IN ('processed', 'superseded')" }]
    assert_includes @migration.foreign_keys,
                    [:custom_outbox_deliveries, :custom_outbox_events,
                     { column: :event_id, on_delete: :cascade }]
  end

  def test_create_search_engine_outbox_trigger_generates_idempotent_row_trigger_sql
    @migration.create_search_engine_outbox_trigger(
      :products,
      source_model: 'Product',
      collection: 'products'
    )

    sql = @migration.executed_sql.join("\n")

    assert_includes sql, 'CREATE OR REPLACE FUNCTION "search_engine_outbox_products_fn"()'
    assert_includes sql, 'DROP TRIGGER IF EXISTS "search_engine_outbox_products_trigger" ON "products";'
    assert_includes sql, 'CREATE TRIGGER "search_engine_outbox_products_trigger"'
    assert_includes sql, 'AFTER INSERT OR UPDATE OR DELETE ON "products"'
    assert_includes sql, 'FOR EACH ROW'
    assert_includes sql, 'EXECUTE FUNCTION "search_engine_outbox_products_fn"();'
    assert_includes sql, 'INSERT INTO "search_engine_outbox_events"'
    refute_includes sql, 'search_engine_outbox_deliveries'
    assert_includes sql, "'Product'"
    assert_includes sql, "'products'"
    assert_includes sql, '(record_data.id::text)'
    assert_includes sql, "jsonb_build_object('trigger_operation', TG_OP)"
    assert_includes sql, 'PERFORM pg_notify('
    assert_includes sql, "'search_engine_outbox'"
    assert_includes sql, "'id', event_id"
    assert_includes sql, "'table', TG_TABLE_NAME"
    assert_includes sql, "'collection', 'products'"
    assert_includes sql, "RETURN OLD;\n"
    assert_includes sql, "RETURN NEW;\n"
  end

  def test_create_search_engine_outbox_trigger_quotes_names_and_literals
    @migration.create_search_engine_outbox_trigger(
      'public.order-items',
      source_model: "OrderItem'sModel",
      collection: 'order-items',
      channel: "search'engine",
      outbox_table: 'search.engine_outbox',
      document_id_sql: "record_data.order_id::text || '-' || record_data.id::text"
    )

    sql = @migration.executed_sql.join("\n")

    assert_includes sql, 'CREATE OR REPLACE FUNCTION "search_engine_outbox_public_order_items_fn"()'
    assert_includes sql, 'ON "public"."order-items"'
    assert_includes sql, 'INSERT INTO "search"."engine_outbox"'
    assert_includes sql, "'OrderItem''sModel'"
    assert_includes sql, "'search''engine'"
    assert_includes sql, "(record_data.order_id::text || '-' || record_data.id::text)"
  end

  def test_create_search_engine_outbox_trigger_accepts_custom_operations
    @migration.create_search_engine_outbox_trigger(
      :products,
      source_model: 'Product',
      collection: 'products',
      operations: %i[update delete]
    )

    assert_includes @migration.executed_sql.join("\n"), 'AFTER UPDATE OR DELETE ON "products"'
  end

  def test_create_search_engine_outbox_trigger_rejects_invalid_operations
    assert_raises(ArgumentError) do
      @migration.create_search_engine_outbox_trigger(
        :products,
        source_model: 'Product',
        collection: 'products',
        operations: [:truncate]
      )
    end
  end

  def test_drop_search_engine_outbox_trigger_drops_trigger_and_function
    @migration.drop_search_engine_outbox_trigger(:products)

    sql = @migration.executed_sql.join("\n")

    assert_includes sql, 'DROP TRIGGER IF EXISTS "search_engine_outbox_products_trigger"'
    assert_includes sql, 'ON "products";'
    assert_includes sql, 'DROP FUNCTION IF EXISTS "search_engine_outbox_products_fn"();'
  end

  def test_generator_templates_are_generic_and_reference_helpers
    templates_root = File.expand_path('../lib/generators/search_engine/postgres_outbox/templates', __dir__)

    table_template = File.read(File.join(templates_root, 'create_outbox_events.rb.tt'))
    trigger_template = File.read(File.join(templates_root, 'add_outbox_triggers.rb.tt'))

    assert_includes table_template, 'include SearchEngine::PostgresOutbox::MigrationHelpers'
    assert_includes table_template, 'create_search_engine_outbox_events'
    assert_includes table_template, 'create_search_engine_outbox_deliveries'
    assert_includes trigger_template, 'create_search_engine_outbox_trigger'
    assert_includes trigger_template, 'drop_search_engine_outbox_trigger'
    assert_includes trigger_template, 'document_id_sql'
    refute_includes trigger_template, 'create_search_engine_outbox_deliveries'
    refute_includes trigger_template, 'Novus'
    refute_includes table_template, 'Novus'
  end

  def test_generator_loads_with_rails_generator_dependencies
    require 'generators/search_engine/postgres_outbox/install_generator'

    assert SearchEngine::Generators::PostgresOutbox::InstallGenerator.source_root.end_with?('/templates')
  end
end
