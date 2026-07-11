# frozen_string_literal: true

require 'test_helper'
require 'json'
require 'open3'
require 'securerandom'
require 'timeout'

class PostgresOutboxRepositoryPostgresqlTest < Minitest::Test
  INTEGRATION_ENV = 'POSTGRES_OUTBOX_POSTGRESQL_INTEGRATION'
  DATABASE_URL_ENV = 'POSTGRES_OUTBOX_POSTGRESQL_URL'

  class PsqlSession
    attr_reader :application_name

    def initialize(database_url:, application_name:)
      @application_name = application_name
      env = {
        'PGAPPNAME' => application_name,
        'PGCONNECT_TIMEOUT' => '2'
      }
      command = [
        'psql',
        '--no-psqlrc',
        '--quiet',
        '--tuples-only',
        '--no-align',
        '--set=ON_ERROR_STOP=1',
        "--dbname=#{database_url}"
      ]
      @stdin, @output, @wait_thread = Open3.popen2e(env, *command)
      @mutex = Mutex.new
      @closed = false
    end

    def run(sql)
      @mutex.synchronize do
        raise 'psql session is closed' if @closed

        marker = "__search_engine_end_#{SecureRandom.hex(12)}__"
        @stdin.puts("#{sql.rstrip}\n;")
        @stdin.puts("\\echo #{marker}")
        @stdin.flush

        read_until(marker)
      end
    end

    def close
      return if @closed

      @closed = true
      @stdin.puts('\\q') unless @stdin.closed?
      @stdin.close unless @stdin.closed?
      @output.close unless @output.closed?
      @wait_thread.join(1)
    rescue IOError, Errno::EPIPE
      nil
    end

    private

    def read_until(marker)
      lines = []
      loop do
        line = @output.gets
        raise "psql exited before completing command: #{lines.join("\n")}" unless line

        value = line.chomp
        break if value == marker

        lines << value unless value.empty?
      end
      lines
    end
  end

  class PsqlConnection
    attr_accessor :before_execute

    def initialize(session)
      @session = session
    end

    def transaction
      @session.run('BEGIN')
      result = yield
      @session.run('COMMIT')
      result
    rescue Exception # rubocop:disable Lint/RescueException -- rollback must cover assertion/timeouts in test threads
      begin
        @session.run('ROLLBACK')
      rescue StandardError
        nil
      end
      raise
    end

    def select_all(sql)
      invoke_hook(sql)
      json = @session.run(<<~SQL).join
        WITH __search_engine_result AS MATERIALIZED (
          #{sql.rstrip.delete_suffix(';')}
        )
        SELECT COALESCE(json_agg(__search_engine_result)::text, '[]')
        FROM __search_engine_result
      SQL
      JSON.parse(json || '[]')
    end

    def execute(sql)
      invoke_hook(sql)
      @session.run(sql)
      nil
    end

    def select_value(sql)
      invoke_hook(sql)
      @session.run(sql).last
    end

    def quote(value)
      return 'NULL' if value.nil?

      "'#{value.to_s.gsub("'", "''")}'"
    end

    def quote_table_name(value)
      value.to_s.split('.').map { |part| %("#{part.gsub('"', '""')}") }.join('.')
    end

    def data_source_exists?(_value)
      true
    end

    private

    def invoke_hook(sql)
      before_execute&.call(sql)
    end
  end

  def setup
    skip "Set #{INTEGRATION_ENV}=true to run PostgreSQL lock-order tests" unless ENV[INTEGRATION_ENV] == 'true'

    @database_url = ENV.fetch(DATABASE_URL_ENV, 'postgres')
    skip 'PostgreSQL 16 is required for lock-order tests' unless postgresql_16?

    store_previous_outbox_config
    @schema = "search_engine_lock_#{Process.pid}_#{SecureRandom.hex(5)}"
    @sessions = []
    @control = new_session('control')
    @control.run(schema_sql)
    configure_outbox
  end

  def teardown
    drop_schema
    @sessions&.reverse_each(&:close)
    restore_previous_outbox_config
  end

  def test_materialization_waits_on_parent_before_locking_an_older_delivery
    seed_materialization_rows
    blocker = new_session('trigger_blocker')
    worker = new_session('materializer')
    connection = PsqlConnection.new(worker)
    parent_lock_attempted = Queue.new
    connection.before_execute = one_shot_parent_lock_hook(parent_lock_attempted)
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: connection)

    blocker.run("BEGIN; SET LOCAL lock_timeout = '1500ms'; SELECT id FROM #{events_table} WHERE id = 1 FOR UPDATE")
    result = run_in_thread { repository.materialize_deliveries!(limit: 10) }

    wait_for_parent_lock(parent_lock_attempted, result)
    wait_until_lock_blocked(worker.application_name)
    blocker.run("SELECT id FROM #{deliveries_table} WHERE id = 101 FOR UPDATE")
    blocker.run('COMMIT')

    rows = thread_result(result)
    assert_equal([2], rows.map { |row| row.fetch('id').to_i })
    assert_equal 'superseded', scalar("SELECT status FROM #{events_table} WHERE id = 1")
    assert_equal 'superseded', scalar("SELECT status FROM #{deliveries_table} WHERE id = 101")
    assert_equal 'pending', scalar("SELECT status FROM #{deliveries_table} WHERE event_id = 2")
  ensure
    rollback(blocker)
  end

  def test_claim_supersede_revalidates_then_waits_on_parent_before_locking_deliveries
    seed_claim_rows
    blocker = new_session('ack_blocker')
    worker = new_session('claimer')
    connection = PsqlConnection.new(worker)
    parent_lock_attempted = Queue.new
    connection.before_execute = one_shot_parent_lock_hook(parent_lock_attempted)
    repository = SearchEngine::PostgresOutbox::Repository.new(
      connection: connection,
      target_key: 'target_1'
    )

    blocker.run("BEGIN; SET LOCAL lock_timeout = '1500ms'; SELECT id FROM #{events_table} WHERE id = 2 FOR UPDATE")
    result = run_in_thread { repository.claim_pending(limit: 10, worker_id: 'worker-1') }

    wait_for_parent_lock(parent_lock_attempted, result)
    wait_until_lock_blocked(worker.application_name)
    blocker.run("SELECT id FROM #{deliveries_table} WHERE id = 102 FOR UPDATE")
    blocker.run('COMMIT')

    events = thread_result(result)
    assert_equal([2], events.map { |event| event.id.to_i })
    assert_equal 'superseded', scalar("SELECT status FROM #{events_table} WHERE id = 1")
    assert_equal 'superseded', scalar("SELECT status FROM #{deliveries_table} WHERE id = 101")
    assert_equal 'processing', scalar("SELECT status FROM #{deliveries_table} WHERE id = 102")
    assert_equal 'worker-1', scalar("SELECT locked_by FROM #{deliveries_table} WHERE id = 102")
  ensure
    rollback(blocker)
  end

  def test_collection_limited_materialization_and_claim_use_the_same_parent_first_protocol
    SearchEngine.config.postgres_outbox.batch_sizes = { products: 1 }
    seed_materialization_rows
    worker = new_session('collection_limited')
    connection = PsqlConnection.new(worker)

    materialized = SearchEngine::PostgresOutbox::Repository
                   .new(connection: connection)
                   .materialize_deliveries!(limit: nil)
    claimed = SearchEngine::PostgresOutbox::Repository
              .new(connection: connection, target_key: 'target_1')
              .claim_pending(limit: nil, worker_id: 'worker-1')

    assert_equal([2], materialized.map { |row| row.fetch('id').to_i })
    assert_equal([2], claimed.map { |event| event.id.to_i })
  end

  def test_claim_recomputes_an_older_delivery_inserted_while_waiting_for_its_parent
    seed_newer_claim_only
    blocker = new_session('materializer_race')
    worker = new_session('claimer_recompute')
    connection = PsqlConnection.new(worker)
    parent_lock_attempted = Queue.new
    connection.before_execute = one_shot_parent_lock_hook(parent_lock_attempted)
    repository = SearchEngine::PostgresOutbox::Repository.new(
      connection: connection,
      target_key: 'target_1'
    )

    blocker.run("BEGIN; SELECT id FROM #{events_table} WHERE id = 1 FOR UPDATE")
    result = run_in_thread { repository.claim_pending(limit: 10, worker_id: 'worker-1') }

    wait_for_parent_lock(parent_lock_attempted, result)
    wait_until_lock_blocked(worker.application_name)
    blocker.run(<<~SQL)
      INSERT INTO #{deliveries_table} (id, event_id, target_key, queue_name, status)
      VALUES (101, 1, 'target_1', 'queue_1', 'pending')
    SQL
    blocker.run('COMMIT')

    events = thread_result(result)
    assert_equal([2], events.map { |event| event.id.to_i })
    assert_equal 'superseded', scalar("SELECT status FROM #{events_table} WHERE id = 1")
    assert_equal 'superseded', scalar("SELECT status FROM #{deliveries_table} WHERE id = 101")
    assert_equal 'processing', scalar("SELECT status FROM #{deliveries_table} WHERE id = 102")
  ensure
    rollback(blocker)
  end

  def test_claiming_a_newer_delivery_prevents_materializing_an_older_missing_delivery
    seed_newer_claim_only
    worker = new_session('no_stale_materialization')
    connection = PsqlConnection.new(worker)

    claimed = SearchEngine::PostgresOutbox::Repository
              .new(connection: connection, target_key: 'target_1')
              .claim_pending(limit: 10, worker_id: 'worker-1')
    materialized = SearchEngine::PostgresOutbox::Repository
                   .new(connection: connection)
                   .materialize_deliveries!(limit: 10)

    assert_equal([2], claimed.map { |event| event.id.to_i })
    assert_empty materialized
    assert_equal 'superseded', scalar("SELECT status FROM #{events_table} WHERE id = 1")
    assert_equal '0', scalar("SELECT COUNT(*) FROM #{deliveries_table} WHERE event_id = 1")
  end

  def test_target_retirement_is_exact_audited_idempotent_and_refreshes_parents
    seed_retirement_rows
    SearchEngine.config.postgres_outbox.delivery_targets = lambda do
      [{ key: 'target_2', queue_name: 'queue_2' }]
    end
    worker = new_session('target_retirement')
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: PsqlConnection.new(worker))

    dry_run = repository.retire_delivery_target!(
      target_key: 'target_1',
      dry_run: true,
      reason: 'topology migration'
    )
    assert_retirement_dry_run(dry_run)

    applied = repository.retire_delivery_target!(
      target_key: 'target_1',
      dry_run: false,
      reason: 'topology migration',
      operator: 'deploy-42'
    )
    assert_retirement_applied(applied)

    repeated = repository.retire_delivery_target!(
      target_key: 'target_1',
      dry_run: false,
      reason: 'topology migration',
      operator: 'deploy-42'
    )
    assert_equal 0, repeated[:matched_nonterminal_deliveries]
    assert_equal 0, repeated[:superseded_deliveries]
    assert_equal 0, repeated[:affected_parent_events]
  end

  def test_exclusive_pause_blocks_claims_and_claims_resume_after_cutover
    seed_claim_rows
    pause_session = new_session('pause_claims')
    claim_session = new_session('paused_claimer')
    pause_repository = SearchEngine::PostgresOutbox::Repository.new(connection: PsqlConnection.new(pause_session))
    claim_repository = SearchEngine::PostgresOutbox::Repository.new(
      connection: PsqlConnection.new(claim_session),
      target_key: 'target_1'
    )
    entered = Queue.new
    release = Queue.new

    pause_result = run_in_thread do
      pause_repository.with_delivery_target_claims_paused(target_key: 'target_1', timeout_s: 2) do
        entered << true
        release.pop
        :cutover_complete
      end
    end
    Timeout.timeout(2) { entered.pop }

    assert_empty claim_repository.claim_pending(limit: 10, worker_id: 'worker-paused')
    assert_equal '0', scalar("SELECT COUNT(*) FROM #{deliveries_table} WHERE status = 'processing'")

    release << true
    assert_equal :cutover_complete, thread_result(pause_result)
    resumed = claim_repository.claim_pending(limit: 10, worker_id: 'worker-resumed')
    assert_equal([2], resumed.map { |event| event.id.to_i })
    assert_equal 'worker-resumed', scalar("SELECT locked_by FROM #{deliveries_table} WHERE id = 102")
  ensure
    release << true if release
  end

  def test_exclusive_pause_waits_for_processing_delivery_to_acknowledge
    seed_claim_rows
    worker_session = new_session('processing_worker')
    pause_session = new_session('quiescence_waiter')
    worker_repository = SearchEngine::PostgresOutbox::Repository.new(
      connection: PsqlConnection.new(worker_session),
      target_key: 'target_1'
    )
    claimed = worker_repository.claim_pending(limit: 10, worker_id: 'worker-1')
    assert_equal([2], claimed.map { |event| event.id.to_i })

    count_observed = Queue.new
    pause_connection = PsqlConnection.new(pause_session)
    pause_connection.before_execute = lambda do |sql|
      count_observed << true if sql.include?('SELECT COUNT(*)')
    end
    pause_repository = SearchEngine::PostgresOutbox::Repository.new(connection: pause_connection)
    entered = Queue.new
    pause_result = run_in_thread do
      pause_repository.with_delivery_target_claims_paused(
        target_key: 'target_1', timeout_s: 2, poll_interval_s: 0.01
      ) do
        entered << true
        nil
      end
    end

    Timeout.timeout(2) { count_observed.pop }
    assert_raises(ThreadError) { entered.pop(true) }
    assert_equal [2], worker_repository.mark_processed!(claimed).map(&:to_i)
    assert_nil thread_result(pause_result)
    assert Timeout.timeout(2) { entered.pop }
  end

  def test_shared_direct_writer_and_exclusive_pause_block_each_other
    writer_session = new_session('shared_writer')
    pause_session = new_session('writer_pause')
    writer_repository = SearchEngine::PostgresOutbox::Repository.new(connection: PsqlConnection.new(writer_session))
    exclusive_attempted = Queue.new
    pause_connection = PsqlConnection.new(pause_session)
    pause_connection.before_execute = lambda do |sql|
      exclusive_attempted << true if sql.include?('pg_try_advisory_lock(')
    end
    pause_repository = SearchEngine::PostgresOutbox::Repository.new(connection: pause_connection)
    writer_entered = Queue.new
    release_writer = Queue.new

    writer_result = run_in_thread do
      writer_repository.with_delivery_target_writes_allowed(target_key: 'target_1', timeout_s: 2) do
        writer_entered << true
        release_writer.pop
        nil
      end
    end
    Timeout.timeout(2) { writer_entered.pop }

    pause_entered = Queue.new
    pause_result = run_in_thread do
      pause_repository.with_delivery_target_claims_paused(
        target_key: 'target_1', timeout_s: 2, poll_interval_s: 0.01
      ) do
        pause_entered << true
        nil
      end
    end
    Timeout.timeout(2) { exclusive_attempted.pop }
    assert_raises(ThreadError) { pause_entered.pop(true) }

    release_writer << true
    assert_nil thread_result(writer_result)
    assert_nil thread_result(pause_result)
    assert Timeout.timeout(2) { pause_entered.pop }
  ensure
    release_writer << true if release_writer
  end

  def test_exclusive_pause_blocks_shared_direct_writer
    pause_session = new_session('exclusive_cutover')
    writer_session = new_session('blocked_writer')
    pause_repository = SearchEngine::PostgresOutbox::Repository.new(connection: PsqlConnection.new(pause_session))
    shared_attempted = Queue.new
    writer_connection = PsqlConnection.new(writer_session)
    writer_connection.before_execute = lambda do |sql|
      shared_attempted << true if sql.include?('pg_try_advisory_lock_shared')
    end
    writer_repository = SearchEngine::PostgresOutbox::Repository.new(connection: writer_connection)
    pause_entered = Queue.new
    release_pause = Queue.new

    pause_result = run_in_thread do
      pause_repository.with_delivery_target_claims_paused(target_key: 'target_1', timeout_s: 2) do
        pause_entered << true
        release_pause.pop
        nil
      end
    end
    Timeout.timeout(2) { pause_entered.pop }

    writer_entered = Queue.new
    writer_result = run_in_thread do
      writer_repository.with_delivery_target_writes_allowed(
        target_key: 'target_1', timeout_s: 2, poll_interval_s: 0.01
      ) do
        writer_entered << true
        nil
      end
    end
    Timeout.timeout(2) { shared_attempted.pop }
    assert_raises(ThreadError) { writer_entered.pop(true) }

    release_pause << true
    assert_nil thread_result(pause_result)
    assert_nil thread_result(writer_result)
    assert Timeout.timeout(2) { writer_entered.pop }
  ensure
    release_pause << true if release_pause
  end

  def test_advisory_guard_timeout_does_not_yield_and_block_exception_releases_lock
    blocker_session = new_session('timeout_blocker')
    timeout_session = new_session('timeout_waiter')
    recovery_session = new_session('exception_recovery')
    blocker_repository = SearchEngine::PostgresOutbox::Repository.new(
      connection: PsqlConnection.new(blocker_session)
    )
    entered = Queue.new
    release = Queue.new
    blocker_result = run_in_thread do
      blocker_repository.with_delivery_target_writes_allowed(target_key: 'target_1', timeout_s: 2) do
        entered << true
        release.pop
        nil
      end
    end
    Timeout.timeout(2) { entered.pop }

    yielded = false
    timeout_repository = SearchEngine::PostgresOutbox::Repository.new(connection: PsqlConnection.new(timeout_session))
    assert_raises(SearchEngine::Errors::Timeout) do
      timeout_repository.with_delivery_target_claims_paused(
        target_key: 'target_1', timeout_s: 0.05, poll_interval_s: 0.01
      ) { yielded = true }
    end
    refute yielded

    release << true
    assert_nil thread_result(blocker_result)
    original = RuntimeError.new('cutover failed')
    recovery_repository = SearchEngine::PostgresOutbox::Repository.new(
      connection: PsqlConnection.new(recovery_session)
    )
    raised = assert_raises(RuntimeError) do
      recovery_repository.with_delivery_target_claims_paused(target_key: 'target_1', timeout_s: 1) do
        raise original
      end
    end
    assert_same original, raised
    assert_equal :reacquired,
                 recovery_repository.with_delivery_target_writes_allowed(
                   target_key: 'target_1', timeout_s: 1
                 ) { :reacquired }
  ensure
    release << true if release
  end

  def test_postgres_releases_delivery_target_lock_when_session_dies
    dead_session = new_session('dead_lock_owner')
    recovery_session = new_session('dead_lock_recovery')
    repository = SearchEngine::PostgresOutbox::Repository.new(connection: PsqlConnection.new(recovery_session))
    key = repository.send(:delivery_target_advisory_lock_key, 'target_1')
    dead_session.run("SELECT pg_advisory_lock(#{key})")

    dead_session.close

    result = repository.with_delivery_target_writes_allowed(target_key: 'target_1', timeout_s: 1) { :recovered }
    assert_equal :recovered, result
  end

  def test_bounded_stale_reclaim_leaves_unselected_rows_processing_and_blocks_cutover
    seed_stale_claim_rows
    worker_session = new_session('bounded_stale_reclaim')
    pause_session = new_session('stale_cutover_waiter')
    worker_repository = SearchEngine::PostgresOutbox::Repository.new(
      connection: PsqlConnection.new(worker_session),
      target_key: 'target_1'
    )

    claimed = worker_repository.claim_pending(limit: 1, worker_id: 'recovery-worker')

    assert_equal([301], claimed.map { |event| event.delivery_id.to_i })
    assert_equal [31], worker_repository.mark_processed!(claimed).map(&:to_i)
    assert_equal '0', scalar("SELECT COUNT(*) FROM #{deliveries_table} WHERE status = 'pending'")
    assert_equal '2', scalar("SELECT COUNT(*) FROM #{deliveries_table} WHERE status = 'processing'")
    assert_equal 'old-worker', scalar("SELECT locked_by FROM #{deliveries_table} WHERE id = 302")

    yielded = false
    pause_repository = SearchEngine::PostgresOutbox::Repository.new(connection: PsqlConnection.new(pause_session))
    assert_raises(SearchEngine::Errors::Timeout) do
      pause_repository.with_delivery_target_claims_paused(
        target_key: 'target_1', timeout_s: 0.05, poll_interval_s: 0.01
      ) { yielded = true }
    end
    refute yielded
  end

  def test_stale_older_delete_is_reclaimed_before_newer_recreation
    seed_stale_delete_then_recreation
    worker_session = new_session('stale_delete_ordering')
    repository = SearchEngine::PostgresOutbox::Repository.new(
      connection: PsqlConnection.new(worker_session),
      target_key: 'target_1'
    )

    first = repository.claim_pending(limit: 10, worker_id: 'recovery-worker')
    assert_equal([401], first.map { |event| event.delivery_id.to_i })
    assert_equal %i[delete], first.map(&:operation)
    assert_equal [41], repository.mark_processed!(first).map(&:to_i)

    second = repository.claim_pending(limit: 10, worker_id: 'recovery-worker')
    assert_equal([402], second.map { |event| event.delivery_id.to_i })
    assert_equal %i[upsert], second.map(&:operation)
    assert_equal [42], repository.mark_processed!(second).map(&:to_i)

    assert_equal '0', scalar("SELECT COUNT(*) FROM #{deliveries_table} WHERE status = 'processing'")
    assert_equal '2', scalar("SELECT COUNT(*) FROM #{deliveries_table} WHERE status = 'processed'")
  end

  def test_stale_reclaim_revalidates_lease_after_parent_wait
    seed_single_stale_claim
    blocker = new_session('stale_parent_blocker')
    claimant_session = new_session('stale_reclaimer')
    heartbeat_session = new_session('stale_heartbeat')
    claimant_connection = PsqlConnection.new(claimant_session)
    parent_lock_attempted = Queue.new
    claimant_connection.before_execute = one_shot_parent_lock_hook(parent_lock_attempted)
    claimant = SearchEngine::PostgresOutbox::Repository.new(
      connection: claimant_connection,
      target_key: 'target_1'
    )
    heartbeat = SearchEngine::PostgresOutbox::Repository.new(
      connection: PsqlConnection.new(heartbeat_session),
      target_key: 'target_1'
    )
    stale_event = SearchEngine::PostgresOutbox::Event.new(
      'id' => 51,
      'source_table' => 'products',
      'source_model_name' => 'Product',
      'collection' => 'products',
      'record_id' => '51',
      'document_id' => 'sku-51',
      'operation' => 'upsert',
      'attempts' => 0,
      'payload' => {},
      'delivery_id' => 501,
      'target_key' => 'target_1',
      'delivery_lease_owner' => 'old-worker'
    )

    blocker.run("BEGIN; SELECT id FROM #{events_table} WHERE id = 51 FOR UPDATE")
    result = run_in_thread { claimant.claim_pending(limit: 10, worker_id: 'new-worker') }
    wait_for_parent_lock(parent_lock_attempted, result)
    wait_until_lock_blocked(claimant_session.application_name)

    assert_equal [51], heartbeat.renew_leases!([stale_event]).map(&:to_i)
    blocker.run('COMMIT')

    assert_empty thread_result(result)
    assert_equal 'old-worker', scalar("SELECT locked_by FROM #{deliveries_table} WHERE id = 501")
    assert_equal 'processing', scalar("SELECT status FROM #{deliveries_table} WHERE id = 501")
  ensure
    rollback(blocker)
  end

  private

  def postgresql_16?
    output, status = Open3.capture2e(
      { 'PGCONNECT_TIMEOUT' => '2' },
      'psql',
      '--no-psqlrc',
      '--quiet',
      '--tuples-only',
      '--no-align',
      "--dbname=#{@database_url}",
      '--command=SHOW server_version'
    )
    status.success? && output.strip.start_with?('16.')
  rescue Errno::ENOENT
    false
  end

  def new_session(label)
    session = PsqlSession.new(
      database_url: @database_url,
      application_name: "search_engine_#{label}_#{SecureRandom.hex(4)}"
    )
    @sessions << session
    session
  end

  def one_shot_parent_lock_hook(queue)
    called = false
    lambda do |sql|
      next if called || !sql.include?("FROM #{events_table}") || !sql.include?('FOR UPDATE')

      called = true
      queue << true
    end
  end

  def run_in_thread
    Queue.new.tap do |result|
      Thread.new do
        result << [:ok, yield]
      rescue StandardError => error
        result << [:error, error]
      end
    end
  end

  def thread_result(result)
    type, value = Timeout.timeout(10) { result.pop }
    raise value if type == :error

    value
  end

  def wait_for_parent_lock(parent_lock_attempted, result)
    Timeout.timeout(5) do
      loop do
        return parent_lock_attempted.pop(true)
      rescue ThreadError
        begin
          type, value = result.pop(true)
          raise value if type == :error

          raise 'repository call completed before attempting its parent lock'
        rescue ThreadError
          sleep 0.01
        end
      end
    end
  end

  def wait_until_lock_blocked(application_name)
    Timeout.timeout(5) do
      loop do
        wait_type = scalar(<<~SQL)
          SELECT COALESCE(wait_event_type, '')
          FROM pg_stat_activity
          WHERE application_name = '#{application_name}'
        SQL
        return if wait_type == 'Lock'

        sleep 0.01
      end
    end
  end

  def scalar(sql)
    @control.run(sql).last
  end

  def rollback(session)
    session&.run('ROLLBACK')
  rescue StandardError
    nil
  end

  def drop_schema
    return unless @control && @schema

    @control.run("DROP SCHEMA IF EXISTS #{@schema} CASCADE")
  rescue StandardError
    nil
  end

  def schema_sql
    <<~SQL
      CREATE SCHEMA #{@schema};
      CREATE TABLE #{events_table} (
        id bigint PRIMARY KEY,
        source_table text NOT NULL,
        source_model_name text NOT NULL,
        collection text NOT NULL,
        record_id text NOT NULL,
        document_id text NOT NULL,
        operation text NOT NULL,
        status text NOT NULL,
        attempts integer NOT NULL DEFAULT 0,
        next_attempt_at timestamptz,
        locked_at timestamptz,
        locked_by text,
        processed_at timestamptz,
        last_error text,
        payload jsonb NOT NULL DEFAULT '{}'::jsonb,
        created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
      CREATE TABLE #{deliveries_table} (
        id bigserial PRIMARY KEY,
        event_id bigint NOT NULL REFERENCES #{events_table}(id),
        target_key text NOT NULL,
        queue_name text NOT NULL,
        status text NOT NULL,
        attempts integer NOT NULL DEFAULT 0,
        next_attempt_at timestamptz,
        locked_at timestamptz,
        locked_by text,
        processed_at timestamptz,
        last_error text,
        created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (event_id, target_key)
      )
    SQL
  end

  def seed_materialization_rows
    insert_events
    @control.run(<<~SQL)
      INSERT INTO #{deliveries_table} (id, event_id, target_key, queue_name, status)
      VALUES (101, 1, 'target_1', 'queue_1', 'pending')
    SQL
  end

  def seed_claim_rows
    insert_events
    @control.run(<<~SQL)
      INSERT INTO #{deliveries_table} (id, event_id, target_key, queue_name, status)
      VALUES (101, 1, 'target_1', 'queue_1', 'pending'),
             (102, 2, 'target_1', 'queue_1', 'pending')
    SQL
  end

  def seed_newer_claim_only
    insert_events
    @control.run(<<~SQL)
      INSERT INTO #{deliveries_table} (id, event_id, target_key, queue_name, status)
      VALUES (102, 2, 'target_1', 'queue_1', 'pending')
    SQL
  end

  def seed_retirement_rows
    @control.run(<<~SQL)
      INSERT INTO #{events_table} (
        id, source_table, source_model_name, collection, record_id, document_id, operation, status
      )
      VALUES (10, 'products', 'Product', 'products', '10', 'sku-10', 'upsert', 'pending'),
             (11, 'products', 'Product', 'products', '11', 'sku-11', 'upsert', 'pending'),
             (12, 'products', 'Product', 'products', '12', 'sku-12', 'upsert', 'failed'),
             (13, 'products', 'Product', 'products', '13', 'sku-13', 'upsert', 'processed');

      INSERT INTO #{deliveries_table} (
        id, event_id, target_key, queue_name, status, locked_at, locked_by, processed_at, last_error
      )
      VALUES (201, 10, 'target_1', 'queue_1', 'pending', NULL, NULL, NULL, NULL),
             (202, 10, 'target_2', 'queue_2', 'pending', NULL, NULL, NULL, NULL),
             (203, 11, 'target_1', 'queue_1', 'processing', CURRENT_TIMESTAMP, 'worker-1', NULL, NULL),
             (204, 12, 'target_1', 'queue_1', 'failed', NULL, NULL, NULL, 'old failure'),
             (205, 13, 'target_1', 'queue_1', 'processed', NULL, NULL, CURRENT_TIMESTAMP, NULL)
    SQL
  end

  def seed_stale_claim_rows
    @control.run(<<~SQL)
      INSERT INTO #{events_table} (
        id, source_table, source_model_name, collection, record_id, document_id, operation, status
      )
      VALUES (31, 'products', 'Product', 'products', '31', 'sku-31', 'upsert', 'pending'),
             (32, 'products', 'Product', 'products', '32', 'sku-32', 'upsert', 'pending'),
             (33, 'products', 'Product', 'products', '33', 'sku-33', 'upsert', 'pending');

      INSERT INTO #{deliveries_table} (
        id, event_id, target_key, queue_name, status, locked_at, locked_by
      )
      VALUES (301, 31, 'target_1', 'queue_1', 'processing',
              CURRENT_TIMESTAMP - interval '60 seconds', 'old-worker'),
             (302, 32, 'target_1', 'queue_1', 'processing',
              CURRENT_TIMESTAMP - interval '60 seconds', 'old-worker'),
             (303, 33, 'target_1', 'queue_1', 'processing',
              CURRENT_TIMESTAMP - interval '60 seconds', 'old-worker')
    SQL
  end

  def seed_stale_delete_then_recreation
    @control.run(<<~SQL)
      INSERT INTO #{events_table} (
        id, source_table, source_model_name, collection, record_id, document_id, operation, status
      )
      VALUES (41, 'products', 'Product', 'products', '41', 'recreated-sku', 'delete', 'pending'),
             (42, 'products', 'Product', 'products', '42', 'recreated-sku', 'upsert', 'pending');

      INSERT INTO #{deliveries_table} (
        id, event_id, target_key, queue_name, status, locked_at, locked_by
      )
      VALUES (401, 41, 'target_1', 'queue_1', 'processing',
              CURRENT_TIMESTAMP - interval '60 seconds', 'old-worker'),
             (402, 42, 'target_1', 'queue_1', 'pending', NULL, NULL)
    SQL
  end

  def seed_single_stale_claim
    @control.run(<<~SQL)
      INSERT INTO #{events_table} (
        id, source_table, source_model_name, collection, record_id, document_id, operation, status
      )
      VALUES (51, 'products', 'Product', 'products', '51', 'sku-51', 'upsert', 'pending');

      INSERT INTO #{deliveries_table} (
        id, event_id, target_key, queue_name, status, locked_at, locked_by
      )
      VALUES (501, 51, 'target_1', 'queue_1', 'processing',
              CURRENT_TIMESTAMP - interval '60 seconds', 'old-worker')
    SQL
  end

  def assert_retirement_dry_run(result)
    assert_equal 3, result[:matched_nonterminal_deliveries]
    assert_equal 0, result[:superseded_deliveries]
    assert_equal 3, result[:affected_parent_events]
    assert_equal 'pending', scalar("SELECT status FROM #{deliveries_table} WHERE id = 201")
  end

  def assert_retirement_applied(result)
    assert_equal 3, result[:matched_nonterminal_deliveries]
    assert_equal result[:matched_nonterminal_deliveries], result[:superseded_deliveries]
    assert_equal 3, result[:affected_parent_events]
    assert_equal '3', scalar(<<~SQL)
      SELECT COUNT(*)
      FROM #{deliveries_table}
      WHERE target_key = 'target_1'
        AND status = 'superseded'
    SQL
    assert_equal 'pending', scalar("SELECT status FROM #{deliveries_table} WHERE id = 202")
    assert_equal 'processed', scalar("SELECT status FROM #{deliveries_table} WHERE id = 205")
    assert_equal 'pending', scalar("SELECT status FROM #{events_table} WHERE id = 10")
    assert_equal 'superseded', scalar("SELECT status FROM #{events_table} WHERE id = 11")
    assert_equal 'superseded', scalar("SELECT status FROM #{events_table} WHERE id = 12")
    assert_equal '<null>', scalar("SELECT COALESCE(locked_by, '<null>') FROM #{deliveries_table} WHERE id = 203")
    assert_retirement_audit
  end

  def assert_retirement_audit
    audit = JSON.parse(scalar("SELECT last_error FROM #{deliveries_table} WHERE id = 201"))
    assert_equal 'delivery_target_retired', audit.fetch('action')
    assert_equal 'target_1', audit.fetch('target_key')
    assert_equal 'topology migration', audit.fetch('reason')
    assert_equal 'deploy-42', audit.fetch('operator')
  end

  def insert_events
    @control.run(<<~SQL)
      INSERT INTO #{events_table} (
        id, source_table, source_model_name, collection, record_id, document_id, operation, status
      )
      VALUES (1, 'products', 'Product', 'products', '1', 'sku-1', 'upsert', 'pending'),
             (2, 'products', 'Product', 'products', '2', 'sku-1', 'upsert', 'pending')
    SQL
  end

  def events_table
    %("#{@schema}"."events")
  end

  def deliveries_table
    %("#{@schema}"."deliveries")
  end

  def store_previous_outbox_config
    config = SearchEngine.config.postgres_outbox
    @previous_outbox_config = {
      table_name: config.table_name,
      delivery_table_name: config.delivery_table_name,
      delivery_targets: config.delivery_targets,
      batch_size: config.batch_size,
      batch_sizes: config.batch_sizes,
      processing_timeout_s: config.processing_timeout_s
    }
  end

  def configure_outbox
    config = SearchEngine.config.postgres_outbox
    config.table_name = "#{@schema}.events"
    config.delivery_table_name = "#{@schema}.deliveries"
    config.delivery_targets = -> { [{ key: 'target_1', queue_name: 'queue_1' }] }
    config.batch_size = 10
    config.batch_sizes = {}
    config.processing_timeout_s = 30
  end

  def restore_previous_outbox_config
    return unless @previous_outbox_config

    config = SearchEngine.config.postgres_outbox
    @previous_outbox_config.each do |key, value|
      config.public_send("#{key}=", value)
    end
  end
end
