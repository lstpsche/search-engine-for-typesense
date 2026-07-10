# frozen_string_literal: true

require 'test_helper'

class PostgresOutboxRepositoryTest < Minitest::Test
  class FakeConnection
    attr_reader :executed_sql, :selected_sql, :transaction_events

    def initialize(
      rows: [],
      row_sets: nil,
      data_source_exists: true,
      data_sources: nil,
      supersede_parent_candidates: [],
      supersede_candidates: []
    )
      @rows = rows
      @row_sets = row_sets
      @data_source_exists = data_source_exists
      @data_sources = data_sources
      @supersede_parent_candidates = supersede_parent_candidates
      @supersede_candidates = supersede_candidates
      @executed_sql = []
      @selected_sql = []
      @transaction_events = []
      @transaction_depth = 0
    end

    def transaction
      transaction_events << [:begin]
      @transaction_depth += 1
      yield
    ensure
      @transaction_depth -= 1
      transaction_events << [:commit]
    end

    def select_all(sql)
      selected_sql << sql
      transaction_events << [:select, sql] if @transaction_depth.positive?
      if sql.include?('SELECT DISTINCT older_events.id AS event_id')
        return @supersede_candidates if sql.include?('older_deliveries.id AS delivery_id')

        return @supersede_parent_candidates
      end
      return @row_sets.shift if @row_sets

      @rows
    end

    def execute(sql)
      executed_sql << sql
      transaction_events << [:execute, sql] if @transaction_depth.positive?
    end

    def quote(value)
      return 'NULL' if value.nil?

      "'#{value.to_s.gsub("'", "''")}'"
    end

    def quote_table_name(value)
      %("#{value.to_s.gsub('"', '""')}")
    end

    def data_source_exists?(value)
      return @data_sources.include?(value.to_s) unless @data_sources.nil?

      %w[custom_outbox_deliveries custom_outbox_drain_slots].include?(value.to_s) && @data_source_exists
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

  def test_retire_delivery_target_dry_run_counts_without_mutation
    connection = FakeConnection.new(
      rows: [{ 'matched_nonterminal_deliveries' => 3, 'affected_parent_events' => 2 }]
    )
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    result = repository.retire_delivery_target!(
      target_key: 'retired_target',
      dry_run: true,
      reason: 'target removed from topology'
    )

    assert_equal(
      {
        target_key: 'retired_target',
        dry_run: true,
        matched_nonterminal_deliveries: 3,
        superseded_deliveries: 0,
        affected_parent_events: 2
      },
      result
    )
    assert_equal 1, connection.selected_sql.size
    assert_includes connection.selected_sql.first, "target_key = 'retired_target'"
    assert_includes connection.selected_sql.first, "status IN ('pending', 'processing', 'failed')"
    assert_empty connection.executed_sql
    assert_empty connection.transaction_events
  end

  def test_retire_delivery_target_reports_actual_race_safe_counts_and_applies_parent_first
    connection = FakeConnection.new(
      row_sets: [
        [{ 'matched_nonterminal_deliveries' => 3, 'affected_parent_events' => 2 }],
        [{ 'id' => 10 }, { 'id' => 11 }],
        [{ 'id' => 100, 'event_id' => 10 }, { 'id' => 101, 'event_id' => 11 }],
        [{ 'event_id' => 10 }, { 'event_id' => 11 }],
        []
      ]
    )
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    result = repository.retire_delivery_target!(
      target_key: 'retired_target',
      dry_run: false,
      reason: 'target removed from topology',
      operator: 'deploy-42'
    )

    assert_equal 2, result[:matched_nonterminal_deliveries]
    assert_equal 2, result[:superseded_deliveries]
    assert_equal 2, result[:affected_parent_events]
    assert_equal result[:matched_nonterminal_deliveries], result[:superseded_deliveries]
    assert_event_row_lock_sql(connection.executed_sql[0], event_ids: [10, 11])
    assert_delivery_row_lock_sql(connection.executed_sql[1], delivery_ids: [100, 101])
    update_sql = connection.selected_sql[3]
    assert_includes update_sql, "status = 'superseded'"
    assert_includes update_sql, 'processed_at = CURRENT_TIMESTAMP'
    assert_includes update_sql, 'next_attempt_at = NULL'
    assert_includes update_sql, 'locked_at = NULL'
    assert_includes update_sql, 'locked_by = NULL'
    assert_includes update_sql, '"reason":"target removed from topology"'
    assert_includes update_sql, '"operator":"deploy-42"'
    assert_parent_refresh_sql(connection.executed_sql[2])
    assert_equal(
      %i[begin select execute select execute select execute commit begin select commit],
      connection.transaction_events.map(&:first)
    )
    all_sql = (connection.selected_sql + connection.executed_sql).join("\n")
    refute_includes all_sql, 'custom_outbox_drain_slots'
  end

  def test_retire_delivery_target_rejects_still_configured_target_before_sql
    SearchEngine.config.postgres_outbox.delivery_targets = lambda do
      [{ key: 'active_target', queue_name: 'active_queue' }]
    end
    connection = FakeConnection.new
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    [true, false].each do |dry_run|
      error = assert_raises(ArgumentError) do
        repository.retire_delivery_target!(
          target_key: 'active_target',
          dry_run: dry_run,
          reason: 'should not retire',
          operator: 'deploy-42'
        )
      end
      assert_match(/still configured/, error.message)
    end

    assert_empty connection.selected_sql
    assert_empty connection.executed_sql
  end

  def test_retire_delivery_target_rechecks_configuration_immediately_before_update
    checks = 0
    SearchEngine.config.postgres_outbox.delivery_targets = lambda do
      checks += 1
      if checks >= 3
        [{ key: 'retired_target', queue_name: 'queue' }]
      else
        []
      end
    end
    connection = FakeConnection.new(
      row_sets: [
        [{ 'matched_nonterminal_deliveries' => 1, 'affected_parent_events' => 1 }],
        [{ 'id' => 10 }],
        [{ 'id' => 100, 'event_id' => 10 }]
      ]
    )
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    error = assert_raises(ArgumentError) do
      repository.retire_delivery_target!(
        target_key: 'retired_target',
        dry_run: false,
        reason: 'target removed',
        operator: 'deploy-42'
      )
    end

    assert_match(/still configured/, error.message)
    assert_equal 3, checks
    refute(connection.selected_sql.any? { |sql| sql.include?("SET status = 'superseded'") })
  end

  def test_retire_delivery_target_rejects_ambiguous_or_unsafe_text_inputs
    connection = FakeConnection.new
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)
    invalid_calls = [
      -> { repository.retire_delivery_target!(target_key: :target, dry_run: true, reason: 'removed') },
      -> { repository.retire_delivery_target!(target_key: Object.new, dry_run: true, reason: 'removed') },
      -> { repository.retire_delivery_target!(target_key: ' target', dry_run: true, reason: 'removed') },
      -> { repository.retire_delivery_target!(target_key: 'target ', dry_run: true, reason: 'removed') },
      -> { repository.retire_delivery_target!(target_key: "target\n", dry_run: true, reason: 'removed') },
      -> { repository.retire_delivery_target!(target_key: 'target', dry_run: true, reason: "bad\treason") },
      lambda do
        repository.retire_delivery_target!(
          target_key: 'target',
          dry_run: false,
          reason: 'removed',
          operator: "bad\noperator"
        )
      end
    ]

    invalid_calls.each { |call| assert_raises(ArgumentError, &call) }
    assert_empty connection.selected_sql
    assert_empty connection.executed_sql
  end

  def test_retire_delivery_target_strips_audit_text_and_persists_all_max_bound_values
    target = 't' * 255
    reason = 'r' * 500
    operator = 'o' * 255
    connection = FakeConnection.new(
      row_sets: [
        [{ 'matched_nonterminal_deliveries' => 1, 'affected_parent_events' => 1 }],
        [{ 'id' => 10 }],
        [{ 'id' => 100, 'event_id' => 10 }],
        [{ 'event_id' => 10 }],
        []
      ]
    )
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    result = repository.retire_delivery_target!(
      target_key: target,
      dry_run: false,
      reason: "  #{reason}  ",
      operator: "  #{operator}  "
    )

    assert_equal 1, result[:matched_nonterminal_deliveries]
    update_sql = connection.selected_sql[3]
    assert_includes update_sql, target
    assert_includes update_sql, reason
    assert_includes update_sql, operator
    assert_operator update_sql.length, :>, SearchEngine::PostgresOutbox::Repository::ERROR_LIMIT
  end

  def test_retire_delivery_target_validates_explicit_bounded_audit_arguments
    connection = FakeConnection.new
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    missing_operator = assert_raises(ArgumentError) do
      repository.retire_delivery_target!(target_key: 'target', dry_run: false, reason: 'removed')
    end
    invalid_dry_run = assert_raises(ArgumentError) do
      repository.retire_delivery_target!(target_key: 'target', dry_run: nil, reason: 'removed')
    end
    long_target = assert_raises(ArgumentError) do
      repository.retire_delivery_target!(target_key: 't' * 256, dry_run: true, reason: 'removed')
    end
    long_reason = assert_raises(ArgumentError) do
      repository.retire_delivery_target!(target_key: 'target', dry_run: true, reason: 'r' * 501)
    end
    long_operator = assert_raises(ArgumentError) do
      repository.retire_delivery_target!(
        target_key: 'target',
        dry_run: false,
        reason: 'removed',
        operator: 'o' * 256
      )
    end

    assert_equal 'operator must be present', missing_operator.message
    assert_equal 'dry_run must be true or false', invalid_dry_run.message
    assert_equal 'target_key must be at most 255 characters', long_target.message
    assert_equal 'reason must be at most 500 characters', long_reason.message
    assert_equal 'operator must be at most 255 characters', long_operator.message
    assert_empty connection.selected_sql
    assert_empty connection.executed_sql
  end

  def test_materialize_deliveries_inserts_missing_rows_for_configured_targets
    SearchEngine.config.postgres_outbox.delivery_targets = lambda do
      [
        { key: :target_1, queue_name: :queue_1 },
        SearchEngine::PostgresOutbox::DeliveryTarget.new(key: 'target_2', queue_name: 'queue_2')
      ]
    end
    connection = FakeConnection.new(
      rows: [event_row(id: 11)],
      supersede_parent_candidates: [{ 'event_id' => 10 }],
      supersede_candidates: [{ 'event_id' => 10, 'latest_event_id' => 11, 'delivery_id' => 100 }]
    )
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    rows = repository.materialize_deliveries!(limit: 25)

    assert_equal([11], rows.map { |row| row['id'] })
    assert_materialization_select_sql(connection.selected_sql[0], lock: false)
    assert_supersede_parent_ids_sql(connection.selected_sql[1])
    assert_materialization_select_sql(connection.selected_sql[2], lock: true, event_ids: [11])
    assert_materialization_supersede_candidates_sql(connection.selected_sql[3])
    assert_includes connection.selected_sql.first, "VALUES ('target_1', 'queue_1'), ('target_2', 'queue_2')"
    assert_event_row_lock_sql(connection.executed_sql[0], event_ids: [11, 10])
    assert_delivery_row_lock_sql(connection.executed_sql[1], delivery_ids: [100])
    assert_supersede_by_id_sql(connection.executed_sql[2], event_ids: [10])
    assert_pending_delivery_supersede_sql(connection.executed_sql[3], delivery_ids: [100])
    assert_parent_refresh_sql(connection.executed_sql[4])
    assert_materialization_insert_sql(connection.executed_sql[5])
    assert_parent_first_supersede_transaction(connection)
  end

  def test_materialize_deliveries_uses_collection_batch_limits_when_limit_is_omitted
    SearchEngine.config.postgres_outbox.batch_sizes = { products: 2, brands: 1 }
    SearchEngine.config.postgres_outbox.delivery_targets = lambda do
      [{ key: :target_1, queue_name: :queue_1 }]
    end
    connection = FakeConnection.new(rows: [event_row(id: 11)])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    repository.materialize_deliveries!

    assert_collection_limited_materialization_sql(connection.selected_sql[0], lock: false)
    assert_collection_limited_materialization_sql(connection.selected_sql[2], lock: true, event_ids: [11])
    assert_event_row_lock_sql(connection.executed_sql[0], event_ids: [11])
    assert_materialization_insert_sql(connection.executed_sql[1])
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
    assert_includes connection.executed_sql[1], "VALUES ('target_1', 'queue_1')"
    refute_includes connection.executed_sql[1], "'target_2'"
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
    assert_equal ['worker-1'], events.map(&:delivery_lease_owner)
    assert_equal 4, connection.selected_sql.size
    assert_delivery_claim_select_sql(connection.selected_sql[0], lock: false)
    assert_supersede_parent_ids_sql(connection.selected_sql[1])
    assert_delivery_claim_select_sql(connection.selected_sql[2], lock: false, delivery_ids: [101])
    assert_delivery_supersede_candidates_sql(connection.selected_sql[3])
    assert_delivery_claim_update_sql(connection.executed_sql)
  end

  def test_delivery_claim_recomputes_supersede_candidates_after_acquiring_parent_locks
    SearchEngine.config.postgres_outbox.delivery_targets = -> { [{ key: 'target_1', queue_name: 'queue_1' }] }
    rows = [event_row(id: 11).merge('delivery_id' => 101, 'target_key' => 'target_1')]
    connection = FakeConnection.new(
      rows: rows,
      supersede_parent_candidates: [{ 'event_id' => 10 }],
      supersede_candidates: [{ 'event_id' => 10, 'latest_event_id' => 11, 'delivery_id' => 100 }]
    )
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection, target_key: 'target_1')

    events = repository.claim_pending(limit: 25, worker_id: 'worker-1')

    assert_equal [11], events.map(&:id)
    assert_delivery_claim_select_sql(connection.selected_sql[0], lock: false)
    assert_supersede_parent_ids_sql(connection.selected_sql[1])
    assert_delivery_claim_select_sql(connection.selected_sql[2], lock: false, delivery_ids: [101])
    assert_delivery_supersede_candidates_sql(connection.selected_sql[3])
    assert_event_row_lock_sql(connection.executed_sql[1], event_ids: [11, 10])
    assert_delivery_row_lock_sql(connection.executed_sql[2], delivery_ids: [100, 101])
    assert_supersede_by_id_sql(connection.executed_sql[3], event_ids: [10])
    assert_pending_delivery_supersede_sql(connection.executed_sql[4], delivery_ids: [100])
    assert_parent_refresh_sql(connection.executed_sql[5])
    assert_includes connection.executed_sql[6], "status = 'processing'"
    assert_equal(
      %i[begin execute select select execute execute execute execute execute commit],
      connection.transaction_events.map(&:first)
    )
  end

  def test_delivery_claim_supersedes_an_older_pending_parent_without_a_delivery
    SearchEngine.config.postgres_outbox.delivery_targets = -> { [{ key: 'target_1', queue_name: 'queue_1' }] }
    rows = [event_row(id: 11).merge('delivery_id' => 101, 'target_key' => 'target_1')]
    connection = FakeConnection.new(
      rows: rows,
      supersede_parent_candidates: [{ 'event_id' => 10 }],
      supersede_candidates: [{ 'event_id' => 10, 'latest_event_id' => 11, 'delivery_id' => nil }]
    )
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection, target_key: 'target_1')

    events = repository.claim_pending(limit: 25, worker_id: 'worker-1')

    assert_equal [11], events.map(&:id)
    assert_event_row_lock_sql(connection.executed_sql[1], event_ids: [11, 10])
    assert_delivery_row_lock_sql(connection.executed_sql[2], delivery_ids: [101])
    assert_supersede_by_id_sql(connection.executed_sql[3], event_ids: [10])
    assert_parent_refresh_sql(connection.executed_sql[4])
    assert_includes connection.executed_sql[5], "status = 'processing'"
    refute(connection.executed_sql.any? { |sql| sql.include?("WHERE id IN ('100')") })
  end

  def test_delivery_claim_does_not_supersede_when_preview_is_invalidated_under_parent_lock
    SearchEngine.config.postgres_outbox.delivery_targets = -> { [{ key: 'target_1', queue_name: 'queue_1' }] }
    preview = event_row(id: 11).merge('delivery_id' => 101, 'target_key' => 'target_1')
    connection = FakeConnection.new(
      row_sets: [[preview], []],
      supersede_parent_candidates: [{ 'event_id' => 10 }],
      supersede_candidates: [{ 'event_id' => 10, 'latest_event_id' => 11, 'delivery_id' => 100 }]
    )
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection, target_key: 'target_1')

    rows = repository.send(:claim_pending_delivery_rows, limit: 25, worker_id: 'worker-1')

    assert_empty rows
    assert_delivery_claim_select_sql(connection.selected_sql[0], lock: false)
    assert_supersede_parent_ids_sql(connection.selected_sql[1])
    assert_delivery_claim_select_sql(connection.selected_sql[2], lock: false, delivery_ids: [101])
    assert_event_row_lock_sql(connection.executed_sql[0], event_ids: [11, 10])
    assert_equal 1, connection.executed_sql.size
    refute(connection.executed_sql.any? { |sql| sql.include?("status = 'superseded'") })
    refute(connection.executed_sql.any? { |sql| sql.include?("status = 'processing'") })
  end

  def test_delivery_claim_uses_collection_batch_limits_when_limit_is_omitted
    SearchEngine.config.postgres_outbox.batch_sizes = { products: 2, brands: 1 }
    SearchEngine.config.postgres_outbox.delivery_targets = -> { [{ key: 'target_1', queue_name: 'queue_1' }] }
    rows = [event_row(id: 11).merge('delivery_id' => 101, 'target_key' => 'target_1')]
    connection = FakeConnection.new(rows: rows)
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection, target_key: 'target_1')

    events = repository.claim_pending(limit: nil, worker_id: 'worker-1')

    assert_equal [11], events.map(&:id)
    assert_collection_limited_delivery_claim_sql(connection.selected_sql[0], lock: false)
    assert_collection_limited_delivery_claim_sql(connection.selected_sql[2], lock: false, delivery_ids: [101])
    assert_delivery_claim_update_sql(connection.executed_sql)
  end

  def test_delivery_claim_materializes_bounded_deliveries_when_no_delivery_rows_are_due
    SearchEngine.config.postgres_outbox.delivery_targets = -> { [{ key: 'target_1', queue_name: 'queue_1' }] }
    rows = [
      [],
      [event_row(id: 11)],
      [event_row(id: 11)],
      [event_row(id: 11).merge('delivery_id' => 101, 'target_key' => 'target_1')],
      [event_row(id: 11).merge('delivery_id' => 101, 'target_key' => 'target_1')]
    ]
    connection = FakeConnection.new(row_sets: rows)
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection, target_key: 'target_1')

    events = repository.claim_pending(limit: 25, worker_id: 'worker-1')

    assert_equal [11], events.map(&:id)
    assert_delivery_claim_select_sql(connection.selected_sql[0], lock: false)
    assert_materialization_select_sql(connection.selected_sql[1], lock: false)
    assert_materialization_select_sql(connection.selected_sql[3], lock: true, event_ids: [11])
    assert_delivery_claim_select_sql(connection.selected_sql[5], lock: false)
    assert_delivery_claim_select_sql(connection.selected_sql[7], lock: false, delivery_ids: [101])
    assert_event_row_lock_sql(connection.executed_sql[1], event_ids: [11])
    assert_materialization_insert_sql(connection.executed_sql[2])
    assert_event_row_lock_sql(connection.executed_sql[3], event_ids: [11])
    assert_delivery_row_lock_sql(connection.executed_sql[4], delivery_ids: [101])
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

  def test_delivery_claim_reset_stale_processing_does_not_lock_or_refresh_parent_rows
    connection = FakeConnection.new(rows: [])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection, target_key: 'target_1')

    repository.claim_pending(limit: 25, worker_id: 'worker-1')

    assert_stale_delivery_reset_sql(connection.executed_sql.first)
  end

  def test_delivery_mark_processed_updates_target_delivery_and_refreshes_parent
    connection = FakeConnection.new(rows: [{ 'id' => 11 }])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection, target_key: 'target_1')
    event = claimed_event(id: 11, delivery_id: 101, lease_owner: 'lease-a')

    acknowledged = repository.mark_processed!([event])

    assert_equal [11], acknowledged
    assert_claimed_delivery_mutation_sql(connection.selected_sql[0], event, status: 'processed')
    assert_event_row_lock_sql(connection.executed_sql[0], event_ids: [11])
    assert_parent_refresh_sql(connection.executed_sql[1])
    assert_delivery_status_refresh_transaction(connection)
  end

  def test_delivery_mark_retryable_updates_target_delivery_and_refreshes_parent
    connection = FakeConnection.new(rows: [{ 'id' => 12 }])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection, target_key: 'target_1')
    event = claimed_event(id: 12, delivery_id: 102, lease_owner: 'lease-a')

    acknowledged = repository.mark_retryable!([event], error: 'temporary')

    assert_equal [12], acknowledged
    sql = connection.selected_sql[0]
    assert_claimed_delivery_mutation_sql(sql, event)
    assert_includes sql, 'attempts = attempts + 1'
    assert_includes sql, "status = CASE WHEN attempts + 1 >= 3 THEN 'failed' ELSE 'pending' END"
    assert_includes sql, "last_error = 'temporary'"
    assert_event_row_lock_sql(connection.executed_sql[0], event_ids: [12])
    assert_parent_refresh_sql(connection.executed_sql[1])
    assert_delivery_status_refresh_transaction(connection)
  end

  def test_delivery_mark_failed_updates_target_delivery_and_refreshes_parent
    connection = FakeConnection.new(rows: [{ 'id' => 13 }])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection, target_key: 'target_1')
    event = claimed_event(id: 13, delivery_id: 103, lease_owner: 'lease-a')

    acknowledged = repository.mark_failed!([event], error: 'hard failure')

    assert_equal [13], acknowledged
    sql = connection.selected_sql[0]
    assert_claimed_delivery_mutation_sql(sql, event, status: 'failed')
    assert_includes sql, "last_error = 'hard failure'"
    assert_event_row_lock_sql(connection.executed_sql[0], event_ids: [13])
    assert_parent_refresh_sql(connection.executed_sql[1])
    assert_delivery_status_refresh_transaction(connection)
  end

  def test_stale_delivery_success_updates_zero_rows_and_does_not_refresh_parent
    connection = FakeConnection.new(rows: [])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection, target_key: 'target_1')
    stale_event = claimed_event(id: 11, delivery_id: 101, lease_owner: 'lease-a')

    acknowledged = repository.mark_processed!([stale_event])

    assert_empty acknowledged
    assert_claimed_delivery_mutation_sql(connection.selected_sql.first, stale_event, status: 'processed')
    assert_equal 1, connection.executed_sql.size
    assert_event_row_lock_sql(connection.executed_sql.first, event_ids: [11])
    assert_equal %i[begin execute select commit], connection.transaction_events.map(&:first)
  end

  def test_reclaimed_delivery_accepts_only_the_new_lease_owner
    connection = FakeConnection.new(row_sets: [[], [{ 'id' => 11 }]])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection, target_key: 'target_1')
    stale_event = claimed_event(id: 11, delivery_id: 101, lease_owner: 'lease-a')
    reclaimed_event = claimed_event(id: 11, delivery_id: 101, lease_owner: 'lease-b')

    stale_acknowledgements = repository.mark_processed!([stale_event])
    current_acknowledgements = repository.mark_processed!([reclaimed_event])

    assert_empty stale_acknowledgements
    assert_equal [11], current_acknowledgements
    assert_claimed_delivery_mutation_sql(connection.selected_sql[0], stale_event, status: 'processed')
    assert_claimed_delivery_mutation_sql(connection.selected_sql[1], reclaimed_event, status: 'processed')
    assert_equal 3, connection.executed_sql.size
    assert_event_row_lock_sql(connection.executed_sql[0], event_ids: [11])
    assert_event_row_lock_sql(connection.executed_sql[1], event_ids: [11])
    assert_parent_refresh_sql(connection.executed_sql[2])
  end

  def test_stale_failure_cannot_overwrite_a_newer_successful_delivery
    connection = FakeConnection.new(row_sets: [[{ 'id' => 11 }], []])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection, target_key: 'target_1')
    current_event = claimed_event(id: 11, delivery_id: 101, lease_owner: 'lease-b')
    stale_event = claimed_event(id: 11, delivery_id: 101, lease_owner: 'lease-a')

    processed = repository.mark_processed!([current_event])
    failed = repository.mark_failed!([stale_event], error: 'late failure')

    assert_equal [11], processed
    assert_empty failed
    assert_claimed_delivery_mutation_sql(connection.selected_sql[0], current_event, status: 'processed')
    assert_claimed_delivery_mutation_sql(connection.selected_sql[1], stale_event, status: 'failed')
    assert_equal 3, connection.executed_sql.size
    assert_event_row_lock_sql(connection.executed_sql[0], event_ids: [11])
    assert_parent_refresh_sql(connection.executed_sql[1])
    assert_event_row_lock_sql(connection.executed_sql[2], event_ids: [11])
  end

  def test_delivery_lease_renewal_is_fenced_and_does_not_refresh_parent
    connection = FakeConnection.new(rows: [{ 'id' => 11 }])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection, target_key: 'target_1')
    event = claimed_event(id: 11, delivery_id: 101, lease_owner: 'lease-a')

    renewed = repository.renew_leases!([event])

    assert_equal [11], renewed
    sql = connection.selected_sql.first
    assert_claimed_delivery_mutation_sql(sql, event)
    assert_includes sql, 'SET locked_at = CURRENT_TIMESTAMP,'
    assert_empty connection.executed_sql
  end

  def test_target_scoped_delivery_mutations_reject_parent_event_ids
    repository = SearchEngine::PostgresOutbox::Repository.new(
      connection: FakeConnection.new,
      target_key: 'target_1'
    )

    error = assert_raises(ArgumentError) { repository.mark_processed!([11]) }

    assert_equal 'target-scoped delivery mutations require claimed Event objects', error.message
  end

  def test_refresh_terminal_delivery_event_statuses_noops_without_delivery_table
    connection = FakeConnection.new(data_sources: ['custom_outbox_drain_slots'])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    refreshed = repository.refresh_terminal_delivery_event_statuses!(retention_s: 3600)

    assert_equal 0, refreshed
    assert_empty connection.selected_sql
    assert_empty connection.executed_sql
  end

  def test_refresh_terminal_delivery_event_statuses_refreshes_bounded_terminal_parents
    connection = FakeConnection.new(rows: [{ 'id' => 11 }, { 'id' => 12 }])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    refreshed = repository.refresh_terminal_delivery_event_statuses!(retention_s: 3600, limit: 50)

    assert_equal 2, refreshed
    assert_terminal_delivery_refresh_sql(connection.selected_sql.first, limit: 50, retention_s: 3600)
  end

  def test_refresh_terminal_delivery_event_statuses_uses_default_batch_limit
    connection = FakeConnection.new(rows: [])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    repository.refresh_terminal_delivery_event_statuses!(retention_s: 60)

    assert_terminal_delivery_refresh_sql(connection.selected_sql.first, limit: 1000, retention_s: 60)
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
    assert_includes sql, 'candidate_events AS MATERIALIZED'
    assert_includes sql, 'FOR UPDATE SKIP LOCKED'
    assert_includes sql, "outbox.status = 'pending'"
    assert_includes sql, 'ROW_NUMBER() OVER'
    assert_includes sql, 'PARTITION BY collection, document_id'
    assert_includes sql, 'FROM candidate_events'
    assert_includes sql, 'ranked_candidate_events.row_number = 1'
    assert_includes sql, 'outbox.next_attempt_at <= CURRENT_TIMESTAMP'
    assert_processing_event_exclusion_sql(sql)
    assert_latest_parent_event_lookup_sql(sql)
    assert_bounded_candidate_sql(sql, candidate_limit: 100, selected_limit: 25)
    assert_includes sql, 'LIMIT 25'
  end

  def assert_collection_limited_claim_sql(sql)
    assert_includes sql, 'collection_limits(collection, batch_size) AS'
    assert_includes sql, "('brands', 1)"
    assert_includes sql, "('products', 2)"
    assert_includes sql, 'PARTITION BY latest_due.collection'
    assert_includes sql, 'collection_row_number <= collection_batch_size'
    assert_includes sql, 'COALESCE(collection_limits.batch_size, 1000)'
    assert_processing_event_exclusion_sql(sql)
    assert_latest_parent_event_lookup_sql(sql)
    assert_bounded_candidate_sql(sql, candidate_limit: 4012, selected_limit: 1003)
    assert_includes sql, 'LIMIT 4012'
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
    assert_includes executed_sql.last, "AND status = 'pending'"
  end

  def assert_supersede_sql(sql)
    assert_includes sql, "status = 'superseded'"
    assert_includes sql, "older.status = 'pending'"
    assert_includes sql, 'older.collection = latest.collection'
    assert_includes sql, 'older.document_id = latest.document_id'
    assert_includes sql, 'older.id < latest.id'
    assert_includes sql, 'VALUES'
  end

  def assert_delivery_claim_select_sql(sql, lock:, delivery_ids: nil)
    assert_includes sql, 'candidate_deliveries AS MATERIALIZED'
    assert_includes sql, 'FROM "custom_outbox_deliveries" deliveries'
    assert_includes sql, 'INNER JOIN "custom_outbox" events'
    assert_includes sql, "deliveries.target_key = 'target_1'"
    assert_includes sql, "deliveries.status = 'pending'"
    assert_includes sql, 'PARTITION BY target_key, collection, document_id'
    assert_includes sql, 'FROM candidate_deliveries'
    assert_includes sql, 'ranked_candidate_deliveries.row_number = 1'
    assert_includes sql, 'deliveries.next_attempt_at <= CURRENT_TIMESTAMP'
    assert_processing_delivery_exclusion_sql(sql)
    assert_latest_delivery_lookup_sql(sql, lock: lock)
    assert_bounded_candidate_sql(sql, candidate_limit: 100, selected_limit: 25) if lock
    assert_includes sql, 'LIMIT 25'
    assert_includes sql, 'deliveries.id AS delivery_id'
    assert_processing_delivery_exclusion_sql(sql)
    assert_includes sql, 'deliveries.target_key'
    assert_delivery_lock_contract(sql, lock: lock, delivery_ids: delivery_ids)
  end

  def assert_collection_limited_delivery_claim_sql(sql, lock:, delivery_ids: nil)
    assert_includes sql, 'collection_limits(collection, batch_size) AS'
    assert_includes sql, "('brands', 1)"
    assert_includes sql, "('products', 2)"
    assert_includes sql, 'PARTITION BY latest_due.collection'
    assert_includes sql, 'collection_row_number <= collection_batch_size'
    assert_includes sql, 'COALESCE(collection_limits.batch_size, 1000)'
    assert_includes sql, 'deliveries.id AS delivery_id'
    assert_latest_delivery_lookup_sql(sql, lock: lock)
    assert_bounded_candidate_sql(sql, candidate_limit: 4012, selected_limit: 1003) if lock
    assert_includes sql, 'LIMIT 4012'
    assert_includes sql, 'LIMIT 1003'
    refute_includes sql, 'LIMIT 25'
    assert_delivery_lock_contract(sql, lock: lock, delivery_ids: delivery_ids)
  end

  def assert_bounded_candidate_sql(sql, candidate_limit:, selected_limit:)
    candidate_limit_position = sql.index("LIMIT #{candidate_limit}")
    lock_position = sql.index('SKIP LOCKED')
    ranking_position = sql.index('ROW_NUMBER() OVER')
    selected_limit_position = sql.rindex("LIMIT #{selected_limit}")

    refute_nil candidate_limit_position
    refute_nil lock_position
    refute_nil ranking_position
    refute_nil selected_limit_position
    assert_operator candidate_limit_position, :<, ranking_position
    assert_operator lock_position, :<, ranking_position
    assert_operator ranking_position, :<, selected_limit_position
  end

  def assert_latest_parent_event_lookup_sql(sql)
    assert_includes sql, 'CROSS JOIN LATERAL'
    assert_includes sql, 'FROM "custom_outbox" latest_pending'
    assert_includes sql, 'latest_pending.collection = ranked_candidate_events.collection'
    assert_includes sql, 'latest_pending.document_id = ranked_candidate_events.document_id'
    assert_includes sql, "latest_pending.status = 'pending'"
    assert_includes sql, 'ORDER BY latest_pending.id DESC'
    assert_includes sql, 'latest_event.next_attempt_at <= CURRENT_TIMESTAMP'
    assert_includes sql, 'FOR UPDATE OF outbox SKIP LOCKED'
  end

  def assert_latest_delivery_lookup_sql(sql, lock:)
    assert_includes sql, 'CROSS JOIN LATERAL'
    assert_includes sql, 'FROM "custom_outbox" latest_events'
    assert_includes sql, 'INNER JOIN "custom_outbox_deliveries" latest_deliveries'
    assert_includes sql, 'latest_deliveries.target_key = ranked_candidate_deliveries.target_key'
    assert_includes sql, 'latest_events.collection = ranked_candidate_deliveries.collection'
    assert_includes sql, 'latest_events.document_id = ranked_candidate_deliveries.document_id'
    assert_includes sql, 'ORDER BY latest_events.id DESC, latest_deliveries.id DESC'
    assert_includes sql, 'latest_delivery.next_attempt_at <= CURRENT_TIMESTAMP'
    if lock
      assert_includes sql, 'FOR UPDATE OF deliveries SKIP LOCKED'
    else
      refute_includes sql, 'FOR UPDATE OF deliveries SKIP LOCKED'
    end
  end

  def assert_processing_event_exclusion_sql(sql)
    assert_includes sql, 'FROM "custom_outbox" processing_events'
    assert_includes sql, "processing_events.status = 'processing'"
    assert_includes sql, 'processing_events.collection = outbox.collection'
    assert_includes sql, 'processing_events.document_id = outbox.document_id'
  end

  def assert_processing_delivery_exclusion_sql(sql)
    assert_includes sql, 'FROM "custom_outbox_deliveries" processing_deliveries'
    assert_includes sql, 'INNER JOIN "custom_outbox" processing_events'
    assert_includes sql, 'processing_deliveries.target_key = deliveries.target_key'
    assert_includes sql, "processing_deliveries.status = 'processing'"
    assert_includes sql, 'processing_events.collection = events.collection'
    assert_includes sql, 'processing_events.document_id = events.document_id'
  end

  def assert_claimed_delivery_mutation_sql(sql, event, status: nil)
    assert_includes sql, 'WITH claimed_deliveries(delivery_id, event_id, lease_owner) AS'
    assert_includes(
      sql,
      "VALUES ('#{event.delivery_id}'::bigint, '#{event.id}'::bigint, '#{event.delivery_lease_owner}')"
    )
    assert_includes sql, 'UPDATE "custom_outbox_deliveries" deliveries'
    assert_includes sql, "status = '#{status}'" if status
    assert_includes sql, 'deliveries.id = claimed_deliveries.delivery_id'
    assert_includes sql, 'deliveries.event_id = claimed_deliveries.event_id'
    assert_includes sql, "deliveries.target_key = 'target_1'"
    assert_includes sql, "deliveries.status = 'processing'"
    assert_includes sql, 'deliveries.locked_by = claimed_deliveries.lease_owner'
    assert_includes sql, 'RETURNING deliveries.event_id AS id'
  end

  def assert_delivery_claim_update_sql(executed_sql)
    assert_includes executed_sql[0], 'UPDATE "custom_outbox_deliveries"'
    assert_includes executed_sql[0], "target_key = 'target_1'"
    assert_event_row_lock_sql(executed_sql[1], event_ids: [11])
    assert_delivery_row_lock_sql(executed_sql[2], delivery_ids: [101])
    assert_includes executed_sql[3], 'UPDATE "custom_outbox_deliveries"'
    assert_includes executed_sql[3], "status = 'processing'"
    assert_includes executed_sql[3], "locked_by = 'worker-1'"
    assert_includes executed_sql[3], "WHERE id IN ('101')"
    assert_includes executed_sql[3], "AND target_key = 'target_1'"
    assert_includes executed_sql[3], "AND status = 'pending'"
  end

  def assert_materialization_select_sql(sql, lock:, event_ids: nil)
    assert_includes sql, 'WITH target(target_key, queue_name) AS ('
    assert_includes sql, "outbox.status IN ('pending', 'processing', 'failed')"
    assert_includes sql, 'NOT EXISTS ('
    assert_includes sql, 'deliveries.event_id = outbox.id'
    assert_includes sql, 'deliveries.target_key = target.target_key'
    assert_includes sql, 'LIMIT 25'
    assert_materialization_lock_contract(sql, lock: lock, event_ids: event_ids)
    assert_includes sql, 'candidate_events AS MATERIALIZED'
    assert_includes sql, 'ROW_NUMBER() OVER'
    assert_includes sql, 'PARTITION BY collection, document_id'
    assert_includes sql, 'latest_candidate_ids'
    assert_includes sql, 'FROM "custom_outbox" outbox'
    assert_includes sql, 'INNER JOIN latest_candidate_ids'
    assert_newer_event_exclusion_sql(sql)
    refute_includes sql, 'SELECT DISTINCT ON (collection, document_id) *'
  end

  def assert_collection_limited_materialization_sql(sql, lock:, event_ids: nil)
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
    assert_newer_event_exclusion_sql(sql)
    assert_materialization_lock_contract(sql, lock: lock, event_ids: event_ids)
    refute_includes sql, 'LIMIT 25'
  end

  def assert_materialization_insert_sql(sql)
    assert_includes sql, 'INSERT INTO "custom_outbox_deliveries"'
    assert_includes sql, 'INNER JOIN "custom_outbox" outbox'
    assert_includes sql, 'CROSS JOIN ('
    assert_includes sql, "VALUES ('target_1', 'queue_1')"
    assert_includes sql, "VALUES ('11'::bigint)"
    assert_includes sql, 'ON CONFLICT (event_id, target_key) DO NOTHING'
  end

  def assert_materialization_lock_contract(sql, lock:, event_ids:)
    if lock
      assert_includes sql, 'FOR UPDATE SKIP LOCKED'
      assert_includes sql, "outbox.id IN (#{event_ids.map { |id| "'#{id}'" }.join(', ')})"
    else
      refute_includes sql, 'FOR UPDATE SKIP LOCKED'
      refute_includes sql, 'outbox.id IN ('
    end
  end

  def assert_delivery_lock_contract(sql, lock:, delivery_ids:)
    if lock
      assert_includes sql, 'FOR UPDATE OF deliveries SKIP LOCKED'
    else
      refute_includes sql, 'FOR UPDATE OF deliveries SKIP LOCKED'
    end

    if delivery_ids
      assert_includes sql, "deliveries.id IN (#{delivery_ids.map { |id| "'#{id}'" }.join(', ')})"
      assert_includes sql, 'latest_delivery.id = ranked_candidate_deliveries.delivery_id'
    else
      refute_includes sql, 'deliveries.id IN ('
      refute_includes sql, 'latest_delivery.id = ranked_candidate_deliveries.delivery_id'
    end
  end

  def assert_parent_first_supersede_transaction(connection)
    assert_equal(
      %i[begin execute select select execute execute execute execute execute commit],
      connection.transaction_events.map(&:first)
    )
    assert_event_row_lock_sql(connection.transaction_events[1].last, event_ids: [11, 10])
    assert_materialization_select_sql(connection.transaction_events[2].last, lock: true, event_ids: [11])
    assert_materialization_supersede_candidates_sql(connection.transaction_events[3].last)
    assert_delivery_row_lock_sql(connection.transaction_events[4].last, delivery_ids: [100])
    assert_supersede_by_id_sql(connection.transaction_events[5].last, event_ids: [10])
    assert_pending_delivery_supersede_sql(connection.transaction_events[6].last, delivery_ids: [100])
    assert_parent_refresh_sql(connection.transaction_events[7].last)
    assert_materialization_insert_sql(connection.transaction_events[8].last)
  end

  def assert_supersede_parent_ids_sql(sql)
    assert_includes sql, 'SELECT DISTINCT older_events.id AS event_id'
    assert_includes sql, 'INNER JOIN "custom_outbox" older_events'
    assert_includes sql, 'older_events.collection = latest.collection'
    assert_includes sql, 'older_events.document_id = latest.document_id'
    assert_includes sql, 'older_events.id < latest.id'
    assert_includes sql, 'ORDER BY event_id ASC'
    refute_includes sql, 'older_deliveries'
    refute_includes sql, 'FOR UPDATE'
  end

  def assert_materialization_supersede_candidates_sql(sql)
    assert_includes sql, 'SELECT DISTINCT older_events.id AS event_id'
    assert_includes sql, 'latest.id AS latest_event_id'
    assert_includes sql, 'older_deliveries.id AS delivery_id'
    assert_includes sql, 'LEFT JOIN "custom_outbox_deliveries" older_deliveries'
    assert_includes sql, 'older_events.id < latest.id'
    assert_includes sql, "older_events.status = 'pending'"
    assert_includes sql, "older_deliveries.target_key IN ('target_1', 'target_2')"
    assert_includes sql, 'ORDER BY event_id ASC'
    assert_includes sql, "('products', '11', '11'::bigint)"
    refute_includes sql, 'FOR UPDATE'
  end

  def assert_delivery_supersede_candidates_sql(sql)
    assert_includes sql, 'SELECT DISTINCT older_events.id AS event_id'
    assert_includes sql, 'latest.event_id AS latest_event_id'
    assert_includes sql, 'older_deliveries.id AS delivery_id'
    assert_includes sql, 'LEFT JOIN "custom_outbox_deliveries" older_deliveries'
    assert_includes sql, 'older_events.id < latest.event_id'
    assert_includes sql, "older_deliveries.status = 'pending'"
    assert_includes sql, "older_events.status = 'pending'"
    assert_includes sql, 'ORDER BY event_id ASC'
    assert_includes sql, "'11'::bigint, '101'::bigint"
    refute_includes sql, 'FOR UPDATE'
  end

  def assert_newer_event_exclusion_sql(sql)
    assert_includes sql, 'FROM "custom_outbox" newer_events'
    assert_includes sql, 'newer_events.collection = outbox.collection'
    assert_includes sql, 'newer_events.document_id = outbox.document_id'
    assert_includes sql, 'newer_events.id > outbox.id'
  end

  def assert_pending_delivery_supersede_sql(sql, delivery_ids:)
    assert_includes sql, 'UPDATE "custom_outbox_deliveries"'
    assert_includes sql, "status = 'superseded'"
    assert_includes sql, "WHERE id IN (#{delivery_ids.map { |id| "'#{id}'" }.join(', ')})"
    assert_includes sql, "AND status = 'pending'"
    refute_includes sql, 'UPDATE "custom_outbox" events'
  end

  def assert_delivery_row_lock_sql(sql, delivery_ids:)
    assert_includes sql, 'SELECT id'
    assert_includes sql, 'FROM "custom_outbox_deliveries"'
    assert_includes sql, "WHERE id IN (#{delivery_ids.map { |id| "'#{id}'" }.join(', ')})"
    assert_includes sql, 'ORDER BY id ASC'
    assert_includes sql, 'FOR UPDATE'
  end

  def assert_supersede_by_id_sql(sql, event_ids:)
    assert_includes sql, 'UPDATE "custom_outbox"'
    assert_includes sql, "WHERE id IN (#{event_ids.map { |id| "'#{id}'" }.join(', ')})"
    assert_includes sql, "status = 'superseded'"
    assert_includes sql, "AND status = 'pending'"
  end

  def assert_stale_delivery_reset_sql(sql)
    assert_includes sql, 'UPDATE "custom_outbox_deliveries"'
    assert_includes sql, "status = 'pending'"
    assert_includes sql, "target_key = 'target_1'"
    assert_includes sql, "status = 'processing'"
    assert_includes sql, "interval '30 seconds'"
    refute_includes sql, 'UPDATE "custom_outbox" events'
    refute_includes sql, 'RETURNING event_id'
  end

  def assert_delivery_status_refresh_transaction(connection)
    assert_equal(
      %i[begin execute select execute commit],
      connection.transaction_events.map(&:first)
    )
    assert_equal connection.executed_sql.first, connection.transaction_events[1].last
    assert_equal connection.selected_sql.last, connection.transaction_events[2].last
    assert_equal connection.executed_sql.last, connection.transaction_events[3].last
  end

  def assert_event_row_lock_sql(sql, event_ids:)
    assert_includes sql, 'SELECT id'
    assert_includes sql, 'FROM "custom_outbox"'
    assert_includes sql, "WHERE id IN (#{event_ids.map { |id| "'#{id}'" }.join(', ')})"
    assert_includes sql, 'ORDER BY id ASC'
    assert_includes sql, 'FOR UPDATE'
  end

  def assert_terminal_delivery_refresh_sql(sql, limit:, retention_s:)
    assert_includes sql, 'WITH candidate_events AS MATERIALIZED'
    assert_includes sql, 'FROM "custom_outbox" events'
    assert_includes sql, "events.status NOT IN ('processed', 'superseded')"
    assert_includes sql, 'EXISTS ('
    assert_includes sql, 'NOT EXISTS ('
    assert_includes sql, "deliveries.status IN ('pending', 'processing', 'failed')"
    assert_includes sql, "LIMIT #{limit}"
    assert_includes sql, 'FOR UPDATE SKIP LOCKED'
    assert_includes sql, 'eligible_events AS ('
    assert_includes sql, 'SELECT MAX(deliveries.processed_at)'
    assert_includes sql, "deliveries.status IN ('processed', 'superseded')"
    assert_includes sql, "interval '#{retention_s} seconds'"
    assert_includes sql, 'event_id IN (SELECT id FROM eligible_events)'
    assert_includes sql, 'updated_events AS ('
    assert_includes sql, 'RETURNING events.id'
    assert_parent_refresh_sql(sql)
  end

  def assert_parent_refresh_sql(sql)
    assert_includes sql, 'UPDATE "custom_outbox" events'
    assert_includes sql, 'FROM "custom_outbox_deliveries"'
    assert_includes sql, "WHEN COUNT(*) FILTER (WHERE status = 'failed') > 0 THEN 'failed'"
    assert_includes sql, "WHEN COUNT(*) FILTER (WHERE status IN ('pending', 'processing')) > 0 THEN 'pending'"
    assert_includes sql, "WHEN COUNT(*) FILTER (WHERE status = 'superseded') = COUNT(*) THEN 'superseded'"
    assert_includes sql, "WHEN COUNT(*) FILTER (WHERE status = 'processed') > 0 THEN 'processed'"
    assert_includes(
      sql,
      "MAX(processed_at) FILTER (WHERE status IN ('processed', 'superseded')) AS terminal_processed_at"
    )
    assert_includes sql, 'THEN COALESCE(aggregate.terminal_processed_at, CURRENT_TIMESTAMP)'
    refute_includes sql, "THEN CURRENT_TIMESTAMP\n                ELSE NULL"
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

  def claimed_event(
    id:,
    delivery_id:,
    lease_owner:,
    target_key: 'target_1',
    operation: 'upsert',
    document_id: nil
  )
    SearchEngine::PostgresOutbox::Event.new(
      event_row(id: id, operation: operation, document_id: document_id).merge(
        'delivery_id' => delivery_id,
        'target_key' => target_key,
        'delivery_lease_owner' => lease_owner
      )
    )
  end

  def event_row(id:, operation: 'upsert', document_id: nil)
    {
      'id' => id,
      'source_table' => 'products',
      'source_model_name' => 'Product',
      'collection' => 'products',
      'record_id' => id.to_s,
      'document_id' => document_id || id.to_s,
      'operation' => operation,
      'attempts' => 0,
      'payload' => {}
    }
  end
end
