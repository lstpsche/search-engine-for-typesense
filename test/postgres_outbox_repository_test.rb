# frozen_string_literal: true

require 'test_helper'

class PostgresOutboxRepositoryTest < Minitest::Test
  class FakeConnection
    attr_reader :executed_sql, :selected_sql

    def initialize(rows: [], row_sets: nil, data_source_exists: true)
      @rows = rows
      @row_sets = row_sets
      @data_source_exists = data_source_exists
      @executed_sql = []
      @selected_sql = []
    end

    def transaction
      yield
    end

    def select_all(sql)
      selected_sql << sql
      return @row_sets.shift if @row_sets

      @rows
    end

    def execute(sql)
      executed_sql << sql
    end

    def quote(value)
      return 'NULL' if value.nil?

      "'#{value.to_s.gsub("'", "''")}'"
    end

    def quote_table_name(value)
      %("#{value.to_s.gsub('"', '""')}")
    end

    def data_source_exists?(value)
      value.to_s == 'custom_outbox_drain_slots' && @data_source_exists
    end
  end

  def setup
    store_previous_outbox_config
    configure_test_outbox
  end

  def teardown
    restore_previous_outbox_config
  end

  def test_claim_pending_locks_pending_rows_with_skip_locked
    connection = FakeConnection.new(rows: [event_row(id: 1), event_row(id: 2)])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    events = repository.claim_pending(limit: 25, worker_id: 'worker-1')

    assert_equal [1, 2], events.map(&:id)
    assert_claim_select_sql(connection.selected_sql.first)
    assert_claim_update_sql(connection.executed_sql)
  end

  def test_claim_pending_uses_collection_batch_limits_when_limit_is_omitted
    SearchEngine.config.postgres_outbox.batch_sizes = { products: 2, brands: 1 }
    connection = FakeConnection.new(rows: [event_row(id: 1), event_row(id: 2)])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    events = repository.claim_pending(limit: nil, worker_id: 'worker-1')

    assert_equal [1, 2], events.map(&:id)
    assert_collection_limited_claim_sql(connection.selected_sql.first)
    assert_claim_update_sql(connection.executed_sql)
  end

  def test_claim_pending_does_not_supersede_when_no_rows_are_claimed
    connection = FakeConnection.new(rows: [])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    events = repository.claim_pending(limit: 25, worker_id: 'worker-1')

    assert_empty events
    assert_equal 1, connection.executed_sql.size
    assert_includes connection.executed_sql.first, "status = 'pending'"
  end

  def test_mark_methods_are_noops_for_empty_ids
    connection = FakeConnection.new
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    repository.mark_processed!([])
    repository.mark_superseded!([])
    repository.mark_retryable!([], error: 'nope')
    repository.mark_failed!([], error: 'nope')

    assert_empty connection.executed_sql
  end

  def test_mark_processed_and_superseded_statuses
    connection = FakeConnection.new
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    repository.mark_processed!([3])
    repository.mark_superseded!([4])

    assert_includes connection.executed_sql[0], "status = 'processed'"
    assert_includes connection.executed_sql[0], 'processed_at = CURRENT_TIMESTAMP'
    assert_includes connection.executed_sql[1], "status = 'superseded'"
  end

  def test_mark_retryable_increments_attempts_and_schedules_failure_threshold
    connection = FakeConnection.new
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    repository.mark_retryable!([5], error: StandardError.new('x' * 1200))

    sql = connection.executed_sql.last
    assert_includes sql, 'attempts = attempts + 1'
    assert_includes sql, "status = CASE WHEN attempts + 1 >= 3 THEN 'failed' ELSE 'pending' END"
    assert_includes sql, "next_attempt_at = CURRENT_TIMESTAMP + CASE attempts + 1 WHEN 1 THEN interval '12 seconds'"
    assert_includes sql, "WHEN 3 THEN interval '12 seconds'"
    assert_includes sql, "last_error = '#{'x' * 1000}'"
  end

  def test_mark_failed_sets_failed_status_and_error
    connection = FakeConnection.new
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    repository.mark_failed!([6], error: 'hard failure')

    sql = connection.executed_sql.last
    assert_includes sql, "status = 'failed'"
    assert_includes sql, "last_error = 'hard failure'"
  end

  def test_materialize_deliveries_inserts_missing_rows_for_configured_targets
    SearchEngine.config.postgres_outbox.delivery_targets = lambda do
      [
        { key: :target_1, queue_name: :queue_1 },
        SearchEngine::PostgresOutbox::DeliveryTarget.new(key: 'target_2', queue_name: 'queue_2')
      ]
    end
    connection = FakeConnection.new(rows: [event_row(id: 11)])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    rows = repository.materialize_deliveries!(limit: 25)

    assert_equal([11], rows.map { |row| row['id'] })
    assert_materialization_select_sql(connection.selected_sql.first)
    assert_includes connection.selected_sql.first, "VALUES ('target_1', 'queue_1'), ('target_2', 'queue_2')"
    assert_materialization_delivery_supersede_sql(connection.executed_sql[0])
    assert_supersede_sql(connection.executed_sql[1])
    assert_materialization_insert_sql(connection.executed_sql[2])
  end

  def test_materialize_deliveries_uses_collection_batch_limits_when_limit_is_omitted
    SearchEngine.config.postgres_outbox.batch_sizes = { products: 2, brands: 1 }
    SearchEngine.config.postgres_outbox.delivery_targets = lambda do
      [{ key: :target_1, queue_name: :queue_1 }]
    end
    connection = FakeConnection.new(rows: [event_row(id: 11)])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    repository.materialize_deliveries!

    assert_collection_limited_materialization_sql(connection.selected_sql.first)
    assert_materialization_delivery_supersede_sql(connection.executed_sql[0])
    assert_supersede_sql(connection.executed_sql[1])
    assert_materialization_insert_sql(connection.executed_sql[2])
  end

  def test_materialize_deliveries_scopes_targets_when_repository_has_target_key
    SearchEngine.config.postgres_outbox.delivery_targets = lambda do
      [
        { key: :target_1, queue_name: :queue_1 },
        SearchEngine::PostgresOutbox::DeliveryTarget.new(key: 'target_2', queue_name: 'queue_2')
      ]
    end
    connection = FakeConnection.new(rows: [event_row(id: 11)])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection, target_key: 'target_1')

    repository.materialize_deliveries!(limit: 25)

    assert_includes connection.selected_sql.first, "VALUES ('target_1', 'queue_1')"
    refute_includes connection.selected_sql.first, "'target_2'"
    assert_includes connection.executed_sql[2], "VALUES ('target_1', 'queue_1')"
    refute_includes connection.executed_sql[2], "'target_2'"
  end

  def test_materialize_deliveries_noops_when_no_targets_are_configured
    connection = FakeConnection.new
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection, target_key: 'target_1')

    repository.materialize_deliveries!

    assert_empty connection.executed_sql
  end

  def test_delivery_claim_uses_target_scoped_delivery_rows
    SearchEngine.config.postgres_outbox.delivery_targets = -> { [{ key: 'target_1', queue_name: 'queue_1' }] }
    rows = [event_row(id: 11).merge('delivery_id' => 101, 'target_key' => 'target_1')]
    connection = FakeConnection.new(rows: rows)
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection, target_key: 'target_1')

    events = repository.claim_pending(limit: 25, worker_id: 'worker-1')

    assert_equal [11], events.map(&:id)
    assert_equal [101], events.map(&:delivery_id)
    assert_equal ['target_1'], events.map(&:target_key)
    assert_equal 1, connection.selected_sql.size
    assert_delivery_claim_select_sql(connection.selected_sql.first)
    assert_delivery_claim_update_sql(connection.executed_sql)
  end

  def test_delivery_claim_uses_collection_batch_limits_when_limit_is_omitted
    SearchEngine.config.postgres_outbox.batch_sizes = { products: 2, brands: 1 }
    SearchEngine.config.postgres_outbox.delivery_targets = -> { [{ key: 'target_1', queue_name: 'queue_1' }] }
    rows = [event_row(id: 11).merge('delivery_id' => 101, 'target_key' => 'target_1')]
    connection = FakeConnection.new(rows: rows)
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection, target_key: 'target_1')

    events = repository.claim_pending(limit: nil, worker_id: 'worker-1')

    assert_equal [11], events.map(&:id)
    assert_collection_limited_delivery_claim_sql(connection.selected_sql.first)
    assert_delivery_claim_update_sql(connection.executed_sql)
  end

  def test_delivery_claim_materializes_bounded_deliveries_when_no_delivery_rows_are_due
    SearchEngine.config.postgres_outbox.delivery_targets = -> { [{ key: 'target_1', queue_name: 'queue_1' }] }
    rows = [
      [],
      [event_row(id: 11)],
      [event_row(id: 11).merge('delivery_id' => 101, 'target_key' => 'target_1')]
    ]
    connection = FakeConnection.new(row_sets: rows)
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection, target_key: 'target_1')

    events = repository.claim_pending(limit: 25, worker_id: 'worker-1')

    assert_equal [11], events.map(&:id)
    assert_delivery_claim_select_sql(connection.selected_sql[0])
    assert_materialization_select_sql(connection.selected_sql[1])
    assert_delivery_claim_select_sql(connection.selected_sql[2])
    assert_materialization_delivery_supersede_sql(connection.executed_sql[1])
    assert_supersede_sql(connection.executed_sql[2])
    assert_materialization_insert_sql(connection.executed_sql[3])
    assert_delivery_supersede_sql(connection.executed_sql[4])
    assert_includes connection.executed_sql[5], "status = 'processing'"
  end

  def test_delivery_claim_with_no_configured_targets_stays_in_delivery_mode
    connection = FakeConnection.new(rows: [])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection, target_key: 'target_1')

    events = repository.claim_pending(limit: 25, worker_id: 'worker-1')

    assert_empty events
    assert_equal 2, connection.selected_sql.size
    assert_empty connection.executed_sql.grep(/UPDATE "custom_outbox"\n          SET status = 'processing'/)
    assert_includes connection.executed_sql.first, 'UPDATE "custom_outbox_deliveries"'
  end

  def test_delivery_mark_processed_updates_target_delivery_and_refreshes_parent
    connection = FakeConnection.new
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection, target_key: 'target_1')

    repository.mark_processed!([11])

    assert_includes connection.executed_sql[0], 'UPDATE "custom_outbox_deliveries"'
    assert_includes connection.executed_sql[0], "status = 'processed'"
    assert_includes connection.executed_sql[0], "WHERE target_key = 'target_1'"
    assert_includes connection.executed_sql[0], "event_id IN ('11')"
    assert_parent_refresh_sql(connection.executed_sql[1])
  end

  def test_delivery_mark_retryable_updates_target_delivery_and_refreshes_parent
    connection = FakeConnection.new
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection, target_key: 'target_1')

    repository.mark_retryable!([12], error: 'temporary')

    assert_includes connection.executed_sql[0], 'UPDATE "custom_outbox_deliveries"'
    assert_includes connection.executed_sql[0], 'attempts = attempts + 1'
    assert_includes connection.executed_sql[0], "status = CASE WHEN attempts + 1 >= 3 THEN 'failed' ELSE 'pending' END"
    assert_includes connection.executed_sql[0], "WHERE target_key = 'target_1'"
    assert_includes connection.executed_sql[0], "last_error = 'temporary'"
    assert_parent_refresh_sql(connection.executed_sql[1])
  end

  def test_delivery_mark_failed_updates_target_delivery_and_refreshes_parent
    connection = FakeConnection.new
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection, target_key: 'target_1')

    repository.mark_failed!([13], error: 'hard failure')

    assert_includes connection.executed_sql[0], 'UPDATE "custom_outbox_deliveries"'
    assert_includes connection.executed_sql[0], "status = 'failed'"
    assert_includes connection.executed_sql[0], "WHERE target_key = 'target_1'"
    assert_includes connection.executed_sql[0], "last_error = 'hard failure'"
    assert_parent_refresh_sql(connection.executed_sql[1])
  end

  def test_drain_slots_table_exists_checks_configured_table
    connection = FakeConnection.new(data_source_exists: true)
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    assert repository.drain_slots_table_exists?
  end

  def test_drain_slots_table_exists_returns_false_when_missing
    connection = FakeConnection.new(data_source_exists: false)
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    refute repository.drain_slots_table_exists?
  end

  def test_acquire_drain_slots_ensures_resets_and_acquires_idle_slots
    rows = [
      { 'target_key' => 'target_1', 'slot' => 1, 'queue_name' => 'queue_1' },
      { 'target_key' => 'target_1', 'slot' => 2, 'queue_name' => 'queue_1' }
    ]
    connection = FakeConnection.new(rows: rows)
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)
    targets = [
      SearchEngine::PostgresOutbox::DeliveryTarget.new(key: 'target_1', queue_name: 'queue_1', parallelism: 2)
    ]

    acquired = repository.acquire_drain_slots!(targets: targets)

    assert_equal(
      [
        { target_key: 'target_1', slot: 1, queue_name: 'queue_1' },
        { target_key: 'target_1', slot: 2, queue_name: 'queue_1' }
      ],
      acquired
    )
    assert_drain_slot_insert_sql(connection.executed_sql[0])
    assert_stale_drain_slot_reset_sql(connection.executed_sql[1])
    assert_drain_slot_acquire_sql(connection.selected_sql.first)
  end

  def test_acquire_drain_slots_returns_empty_when_all_slots_are_occupied
    connection = FakeConnection.new(rows: [])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)
    targets = [{ key: 'target_1', queue_name: 'queue_1', parallelism: 2 }]

    assert_empty repository.acquire_drain_slots!(targets: targets)
    assert_drain_slot_acquire_sql(connection.selected_sql.first)
  end

  def test_start_drain_slot_marks_slot_processing
    connection = FakeConnection.new(rows: [{ 'target_key' => 'target_1', 'slot' => 2 }])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    assert repository.start_drain_slot!(target_key: 'target_1', slot: 2, worker_id: 'worker-1')

    sql = connection.selected_sql.last
    assert_includes sql, 'UPDATE "custom_outbox_drain_slots"'
    assert_includes sql, "status = 'processing'"
    assert_includes sql, "locked_by = 'worker-1'"
    assert_includes sql, 'started_at = CURRENT_TIMESTAMP'
    assert_includes sql, "target_key = 'target_1'"
    assert_includes sql, 'slot = 2'
    assert_includes sql, "AND status = 'queued'"
    assert_includes sql, 'RETURNING target_key, slot'
  end

  def test_start_drain_slot_returns_false_when_slot_is_not_queued
    connection = FakeConnection.new(rows: [])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    refute repository.start_drain_slot!(target_key: 'target_1', slot: 2, worker_id: 'worker-1')
  end

  def test_requeue_drain_slot_marks_owned_processing_slot_queued
    connection = FakeConnection.new(rows: [{ 'target_key' => 'target_1', 'slot' => 2 }])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    assert repository.requeue_drain_slot!(target_key: 'target_1', slot: 2, worker_id: 'worker-1')

    sql = connection.selected_sql.last
    assert_includes sql, 'UPDATE "custom_outbox_drain_slots"'
    assert_includes sql, "status = 'queued'"
    assert_includes sql, 'locked_by = NULL'
    assert_includes sql, 'enqueued_at = CURRENT_TIMESTAMP'
    assert_includes sql, "target_key = 'target_1'"
    assert_includes sql, 'slot = 2'
    assert_includes sql, "AND status = 'processing'"
    assert_includes sql, "AND locked_by = 'worker-1'"
    assert_includes sql, 'RETURNING target_key, slot'
  end

  def test_requeue_drain_slot_returns_false_when_worker_no_longer_owns_slot
    connection = FakeConnection.new(rows: [])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    refute repository.requeue_drain_slot!(target_key: 'target_1', slot: 2, worker_id: 'worker-1')
  end

  def test_release_drain_slot_marks_idle_and_records_error
    connection = FakeConnection.new
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    repository.release_drain_slot!(target_key: 'target_1', slot: 2, worker_id: 'worker-1', error: 'boom')

    sql = connection.executed_sql.last
    assert_includes sql, 'UPDATE "custom_outbox_drain_slots"'
    assert_includes sql, "status = 'idle'"
    assert_includes sql, 'locked_at = NULL'
    assert_includes sql, "last_error = 'boom'"
    assert_includes sql, "target_key = 'target_1'"
    assert_includes sql, 'slot = 2'
    assert_includes sql, "AND locked_by = 'worker-1'"
  end

  def test_release_requeued_drain_slot_marks_queued_slot_idle_and_records_error
    connection = FakeConnection.new
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    repository.release_requeued_drain_slot!(target_key: 'target_1', slot: 2, error: 'enqueue boom')

    sql = connection.executed_sql.last
    assert_includes sql, 'UPDATE "custom_outbox_drain_slots"'
    assert_includes sql, "status = 'idle'"
    assert_includes sql, 'locked_at = NULL'
    assert_includes sql, "last_error = 'enqueue boom'"
    assert_includes sql, "target_key = 'target_1'"
    assert_includes sql, 'slot = 2'
    assert_includes sql, "AND status = 'queued'"
    refute_includes sql, 'locked_by = \'worker-1\''
  end

  private

  def store_previous_outbox_config
    cfg = SearchEngine.config.postgres_outbox
    @previous_outbox_config = {
      table_name: cfg.table_name,
      delivery_table_name: cfg.delivery_table_name,
      drain_slot_table_name: cfg.drain_slot_table_name,
      delivery_targets: cfg.delivery_targets,
      batch_sizes: cfg.batch_sizes,
      max_attempts: cfg.max_attempts,
      retry_backoff: cfg.retry_backoff,
      processing_timeout_s: cfg.processing_timeout_s
    }
  end

  def configure_test_outbox
    cfg = SearchEngine.config.postgres_outbox
    cfg.table_name = 'custom_outbox'
    cfg.delivery_table_name = 'custom_outbox_deliveries'
    cfg.drain_slot_table_name = 'custom_outbox_drain_slots'
    cfg.delivery_targets = -> { [] }
    cfg.batch_sizes = {}
    cfg.max_attempts = 3
    cfg.retry_backoff = ->(_attempt) { 12 }
    cfg.processing_timeout_s = 30
  end

  def restore_previous_outbox_config
    cfg = SearchEngine.config.postgres_outbox
    @previous_outbox_config.each do |key, value|
      cfg.public_send("#{key}=", value)
    end
  end

  def assert_claim_select_sql(sql)
    assert_includes sql, 'FOR UPDATE SKIP LOCKED'
    assert_includes sql, "WHERE status = 'pending'"
    assert_includes sql, 'ROW_NUMBER() OVER'
    assert_includes sql, 'PARTITION BY collection, document_id'
    assert_includes sql, 'ranked_pending.row_number = 1'
    assert_includes sql, 'outbox.next_attempt_at <= CURRENT_TIMESTAMP'
    assert_includes sql, 'LIMIT 25'
  end

  def assert_collection_limited_claim_sql(sql)
    assert_includes sql, 'collection_limits(collection, batch_size) AS'
    assert_includes sql, "('brands', 1)"
    assert_includes sql, "('products', 2)"
    assert_includes sql, 'PARTITION BY latest_due.collection'
    assert_includes sql, 'collection_row_number <= collection_batch_size'
    assert_includes sql, 'COALESCE(collection_limits.batch_size, 1000)'
    assert_includes sql, 'LIMIT 1003'
    assert_includes sql, 'FOR UPDATE SKIP LOCKED'
    refute_includes sql, 'LIMIT 25'
  end

  def assert_claim_update_sql(executed_sql)
    assert_includes executed_sql.first, "status = 'pending'"
    assert_supersede_sql(executed_sql[1])
    assert_includes executed_sql.last, "status = 'processing'"
    assert_includes executed_sql.last, "locked_by = 'worker-1'"
    assert_includes executed_sql.last, 'WHERE id IN (\'1\', \'2\')'
  end

  def assert_supersede_sql(sql)
    assert_includes sql, "status = 'superseded'"
    assert_includes sql, "older.status = 'pending'"
    assert_includes sql, 'older.collection = latest.collection'
    assert_includes sql, 'older.document_id = latest.document_id'
    assert_includes sql, 'older.id < latest.id'
    assert_includes sql, 'VALUES'
  end

  def assert_delivery_claim_select_sql(sql)
    assert_includes sql, 'FOR UPDATE SKIP LOCKED'
    assert_includes sql, 'FROM "custom_outbox_deliveries" deliveries'
    assert_includes sql, 'INNER JOIN "custom_outbox" events'
    assert_includes sql, "deliveries.target_key = 'target_1'"
    assert_includes sql, "deliveries.status = 'pending'"
    assert_includes sql, 'PARTITION BY deliveries.target_key, events.collection, events.document_id'
    assert_includes sql, 'ranked_pending.row_number = 1'
    assert_includes sql, 'deliveries.next_attempt_at <= CURRENT_TIMESTAMP'
    assert_includes sql, 'LIMIT 25'
    assert_includes sql, 'deliveries.id AS delivery_id'
    assert_includes sql, 'deliveries.target_key'
  end

  def assert_collection_limited_delivery_claim_sql(sql)
    assert_includes sql, 'collection_limits(collection, batch_size) AS'
    assert_includes sql, "('brands', 1)"
    assert_includes sql, "('products', 2)"
    assert_includes sql, 'PARTITION BY latest_due.collection'
    assert_includes sql, 'collection_row_number <= collection_batch_size'
    assert_includes sql, 'COALESCE(collection_limits.batch_size, 1000)'
    assert_includes sql, 'deliveries.id AS delivery_id'
    assert_includes sql, 'LIMIT 1003'
    assert_includes sql, 'FOR UPDATE SKIP LOCKED'
    refute_includes sql, 'LIMIT 25'
  end

  def assert_delivery_claim_update_sql(executed_sql)
    assert_includes executed_sql[0], 'UPDATE "custom_outbox_deliveries"'
    assert_includes executed_sql[0], "target_key = 'target_1'"
    assert_delivery_supersede_sql(executed_sql[1])
    assert_includes executed_sql[2], 'UPDATE "custom_outbox_deliveries"'
    assert_includes executed_sql[2], "status = 'processing'"
    assert_includes executed_sql[2], "locked_by = 'worker-1'"
    assert_includes executed_sql[2], "WHERE id IN ('101')"
  end

  def assert_materialization_select_sql(sql)
    assert_includes sql, 'WITH target(target_key, queue_name) AS ('
    assert_includes sql, "outbox.status IN ('pending', 'processing', 'failed')"
    assert_includes sql, 'NOT EXISTS ('
    assert_includes sql, 'deliveries.event_id = outbox.id'
    assert_includes sql, 'deliveries.target_key = target.target_key'
    assert_includes sql, 'LIMIT 25'
    assert_includes sql, 'FOR UPDATE SKIP LOCKED'
    assert_includes sql, 'candidate_events AS MATERIALIZED'
    assert_includes sql, 'ROW_NUMBER() OVER'
    assert_includes sql, 'PARTITION BY collection, document_id'
    assert_includes sql, 'latest_candidate_ids'
    assert_includes sql, 'FROM "custom_outbox" outbox'
    assert_includes sql, 'INNER JOIN latest_candidate_ids'
    refute_includes sql, 'SELECT DISTINCT ON (collection, document_id) *'
  end

  def assert_collection_limited_materialization_sql(sql)
    assert_includes sql, 'WITH target(target_key, queue_name) AS ('
    assert_includes sql, 'collection_limits(collection, batch_size) AS'
    assert_includes sql, "('brands', 1)"
    assert_includes sql, "('products', 2)"
    assert_includes sql, 'candidate_events AS MATERIALIZED'
    assert_includes sql, 'latest_candidate_ids'
    assert_includes sql, 'PARTITION BY latest_candidate_ids.collection'
    assert_includes sql, 'collection_row_number <= collection_batch_size'
    assert_includes sql, 'COALESCE(collection_limits.batch_size, 1000)'
    assert_includes sql, 'LIMIT 1003'
    assert_includes sql, 'FOR UPDATE SKIP LOCKED'
    refute_includes sql, 'LIMIT 25'
  end

  def assert_materialization_insert_sql(sql)
    assert_includes sql, 'INSERT INTO "custom_outbox_deliveries"'
    assert_includes sql, 'INNER JOIN "custom_outbox" outbox'
    assert_includes sql, 'CROSS JOIN ('
    assert_includes sql, "VALUES ('target_1', 'queue_1')"
    assert_includes sql, 'ON CONFLICT (event_id, target_key) DO NOTHING'
  end

  def assert_materialization_delivery_supersede_sql(sql)
    assert_includes sql, 'updated_deliveries AS ('
    assert_includes sql, 'older_event_targets AS MATERIALIZED'
    assert_includes sql, 'UPDATE "custom_outbox_deliveries" older_deliveries'
    assert_includes sql, "status = 'superseded'"
    assert_includes sql, 'FROM latest'
    assert_includes sql, 'CROSS JOIN target'
    assert_includes sql, "VALUES ('target_1', 'queue_1')"
    assert_includes sql, 'INNER JOIN "custom_outbox" older_events'
    assert_includes sql, 'older_deliveries.event_id = older_event_targets.event_id'
    assert_includes sql, 'older_deliveries.target_key = older_event_targets.target_key'
    assert_includes sql, 'older_events.collection = latest.collection'
    assert_includes sql, 'older_events.document_id = latest.document_id'
    assert_includes sql, 'older_events.id < latest.id'
    refute_includes sql, 'FROM "custom_outbox" older_events,'
    assert_parent_refresh_sql(sql)
  end

  def assert_delivery_supersede_sql(sql)
    assert_includes sql, 'updated_deliveries AS ('
    assert_includes sql, 'older_event_targets AS MATERIALIZED'
    assert_includes sql, 'UPDATE "custom_outbox_deliveries" older_deliveries'
    assert_includes sql, "status = 'superseded'"
    assert_includes sql, 'older_deliveries.status = \'pending\''
    assert_includes sql, 'INNER JOIN "custom_outbox" older_events'
    assert_includes sql, 'older_deliveries.event_id = older_event_targets.event_id'
    assert_includes sql, 'older_deliveries.target_key = older_event_targets.target_key'
    assert_includes sql, 'older_events.collection = latest.collection'
    assert_includes sql, 'older_events.document_id = latest.document_id'
    assert_includes sql, 'older_events.id < latest.event_id'
    assert_includes sql, 'RETURNING older_deliveries.event_id'
    assert_includes sql, "('target_1', 'products', '11', '11', '101')"
    refute_includes sql, 'FROM "custom_outbox" older_events,'
    assert_parent_refresh_sql(sql)
  end

  def assert_parent_refresh_sql(sql)
    assert_includes sql, 'UPDATE "custom_outbox" events'
    assert_includes sql, 'FROM "custom_outbox_deliveries"'
    assert_includes sql, "WHEN COUNT(*) FILTER (WHERE status = 'failed') > 0 THEN 'failed'"
    assert_includes sql, "WHEN COUNT(*) FILTER (WHERE status IN ('pending', 'processing')) > 0 THEN 'pending'"
    assert_includes sql, "WHEN COUNT(*) FILTER (WHERE status = 'superseded') = COUNT(*) THEN 'superseded'"
    assert_includes sql, "WHEN COUNT(*) FILTER (WHERE status = 'processed') > 0 THEN 'processed'"
    assert_includes sql, "WHEN aggregate.status IN ('processed', 'superseded') THEN CURRENT_TIMESTAMP"
  end

  def assert_drain_slot_insert_sql(sql)
    assert_includes sql, 'INSERT INTO "custom_outbox_drain_slots"'
    assert_includes sql, "SELECT 'target_1'"
    assert_includes sql, "'queue_1'"
    assert_includes sql, 'FROM generate_series(1, 2) AS slot'
    assert_includes sql, 'ON CONFLICT (target_key, slot) DO UPDATE'
  end

  def assert_stale_drain_slot_reset_sql(sql)
    assert_includes sql, 'UPDATE "custom_outbox_drain_slots"'
    assert_includes sql, "status = 'idle'"
    assert_includes sql, "target_key IN ('target_1')"
    assert_includes sql, "status IN ('queued', 'processing')"
    assert_includes sql, "interval '30 seconds'"
  end

  def assert_drain_slot_acquire_sql(sql)
    assert_includes sql, 'WITH target(target_key, queue_name, parallelism) AS ('
    assert_includes sql, "VALUES ('target_1', 'queue_1', 2)"
    assert_includes sql, 'FROM "custom_outbox_drain_slots" slots'
    assert_includes sql, "slots.status = 'idle'"
    assert_includes sql, 'slots.slot <= target.parallelism'
    assert_includes sql, 'FOR UPDATE SKIP LOCKED'
    assert_includes sql, "SET status = 'queued'"
    assert_includes sql, 'enqueued_at = CURRENT_TIMESTAMP'
    assert_includes sql, 'RETURNING slots.target_key, slots.slot, slots.queue_name'
  end

  def event_row(id:)
    {
      'id' => id,
      'source_table' => 'products',
      'source_model_name' => 'Product',
      'collection' => 'products',
      'record_id' => id.to_s,
      'document_id' => id.to_s,
      'operation' => 'upsert',
      'attempts' => 0,
      'payload' => {}
    }
  end
end
