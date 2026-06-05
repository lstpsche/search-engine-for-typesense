# frozen_string_literal: true

require 'test_helper'

class PostgresOutboxRepositoryTest < Minitest::Test
  class FakeConnection
    attr_reader :executed_sql, :selected_sql

    def initialize(rows: [])
      @rows = rows
      @executed_sql = []
      @selected_sql = []
    end

    def transaction
      yield
    end

    def select_all(sql)
      selected_sql << sql
      @rows
    end

    def execute(sql)
      executed_sql << sql
    end

    def quote(value)
      "'#{value.to_s.gsub("'", "''")}'"
    end

    def quote_table_name(value)
      %("#{value.to_s.gsub('"', '""')}")
    end
  end

  def setup
    @previous_table = SearchEngine.config.postgres_outbox.table_name
    @previous_attempts = SearchEngine.config.postgres_outbox.max_attempts
    @previous_backoff = SearchEngine.config.postgres_outbox.retry_backoff

    SearchEngine.config.postgres_outbox.table_name = 'custom_outbox'
    SearchEngine.config.postgres_outbox.max_attempts = 3
    SearchEngine.config.postgres_outbox.retry_backoff = ->(_attempt) { 12 }
  end

  def teardown
    SearchEngine.config.postgres_outbox.table_name = @previous_table
    SearchEngine.config.postgres_outbox.max_attempts = @previous_attempts
    SearchEngine.config.postgres_outbox.retry_backoff = @previous_backoff
  end

  def test_claim_pending_locks_pending_rows_with_skip_locked
    connection = FakeConnection.new(rows: [event_row(id: 1), event_row(id: 2)])
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    events = repository.claim_pending(limit: 25, worker_id: 'worker-1')

    assert_equal [1, 2], events.map(&:id)
    assert_includes connection.selected_sql.first, 'FOR UPDATE SKIP LOCKED'
    assert_includes connection.selected_sql.first, "WHERE status = 'pending'"
    assert_includes connection.selected_sql.first, 'next_attempt_at <= CURRENT_TIMESTAMP'
    assert_includes connection.selected_sql.first, 'LIMIT 25'

    assert_includes connection.executed_sql.first, "status = 'pending'"
    assert_includes connection.executed_sql.last, "status = 'processing'"
    assert_includes connection.executed_sql.last, "locked_by = 'worker-1'"
    assert_includes connection.executed_sql.last, 'WHERE id IN (\'1\', \'2\')'
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

  private

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
