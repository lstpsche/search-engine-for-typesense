# frozen_string_literal: true

require 'test_helper'
require 'search_engine/sources/base'
require 'search_engine/sources/sql_source'

class SqlSourceTest < Minitest::Test
  class Result
    def initialize(rows)
      @rows = rows
    end

    def ntuples
      @rows.size
    end

    def to_a
      @rows
    end

    def values
      @rows.map(&:values)
    end
  end

  class Connection
    attr_reader :commands, :timeout
    attr_accessor :transaction_status, :fail_declare

    def initialize
      @commands = []
      @transaction_status = 0
      @timeout = '3s'
      @fetches = 0
    end

    def exec(sql)
      @commands << sql
      case sql
      when 'BEGIN READ ONLY'
        @transaction_status = 2
        @previous_timeout = @timeout
      when 'COMMIT', 'ROLLBACK'
        @transaction_status = 0
        @timeout = @previous_timeout
      when /FETCH/
        @fetches += 1
        rows = @fetches == 1 ? [{ 'id' => '1' }] : []
        Result.new(rows)
      end
    end

    def exec_params(sql, params)
      @commands << sql
      if sql.start_with?('SELECT set_config')
        raise 'timeout set outside transaction' if transaction_status.zero?

        @timeout = params.first
      elsif fail_declare
        raise fail_declare
      end
    end
  end

  def stream(connection, **options, &block)
    SearchEngine::Sources::SqlSource.new(sql: 'SELECT 1', **options)
                                    .send(:stream_postgres, connection, partition: nil, cursor: nil, &block)
  end

  def test_rejects_caller_transaction_without_executing_sql
    connection = Connection.new
    connection.transaction_status = 2
    assert_raises(SearchEngine::Errors::InvalidParams) { stream(connection) {} }
    assert_empty connection.commands
    assert_equal 2, connection.transaction_status
  end

  def test_timeout_is_local_and_restored_after_success
    connection = Connection.new
    stream(connection, statement_timeout_ms: 10) do |rows|
      assert_equal '10', connection.timeout
      assert_equal [{ 'id' => '1' }], rows
    end
    assert_equal '3s', connection.timeout
    assert_equal 'COMMIT', connection.commands.last
  end

  def test_consumer_failure_rolls_back_and_restores_timeout
    connection = Connection.new
    failure = RuntimeError.new('consumer failed')
    error = assert_raises(RuntimeError) do
      stream(connection, statement_timeout_ms: 10) { raise failure }
    end
    assert_same failure, error
    assert_equal 'ROLLBACK', connection.commands.last
    assert_equal '3s', connection.timeout
  end

  def test_declare_failure_rolls_back
    connection = Connection.new
    connection.fail_declare = RuntimeError.new('bad SQL')
    assert_raises(RuntimeError) { stream(connection) {} }
    assert_equal 'ROLLBACK', connection.commands.last
  end
end
