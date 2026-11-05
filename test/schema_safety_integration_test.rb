# frozen_string_literal: true

require 'test_helper'
require 'rake'
require 'timeout'
require_relative 'support/typesense_integration'

class SchemaSafetyIntegrationTest < Minitest::Test
  include TypesenseIntegration

  class Inventory < SearchEngine::Base
    class << self
      attr_accessor :rows_by_partition
    end

    identify_by { |row| row.fetch(:id) }
    attribute :quantity, :integer
    schema_retention keep_last: 0

    index do
      partitions { Inventory.rows_by_partition.keys }
      partition_fetch { |partition| [Inventory.rows_by_partition.fetch(partition)] }
      map { |row| { quantity: row.fetch(:quantity) } }
    end
  end

  def setup
    @original_guard = SearchEngine.config.schema.around_rebuild
    setup_typesense
    SearchEngine.config.schema.around_rebuild = nil
    @logical = "schema_fixture_#{Process.pid}_#{SecureRandom.hex(6)}"
    Inventory.collection(@logical)
  end

  def teardown
    SearchEngine.config.schema.around_rebuild = @original_guard
    @owned_collections.concat(generations) if @logical
    teardown_typesense
  end

  def test_forced_schema_task_rejects_partial_and_failed_imports
    invalid = { id: 'invalid', quantity: 'invalid' }
    Inventory.rows_by_partition = {
      first: [{ id: 'first', quantity: 1 }],
      second: [{ id: 'second', quantity: 2 }, invalid]
    }
    source = create_generation('20250101')
    import_fixture(source, [{ id: 'old', quantity: 10 }, { id: 'other', quantity: 11 }])
    point_alias(source)

    status, error = run_apply_task(force_rebuild: false)
    assert_equal 0, status, error
    status, error = run_apply_task(force_rebuild: true)
    assert_equal 1, status, error
    assert_match(/1 failed document/, error)
    Inventory.rows_by_partition = { only: [invalid] }
    status, error = run_apply_task(force_rebuild: true)
    assert_equal 1, status, error
    assert_match(/1 failed document/, error)

    assert_equal [source], generations
    assert_equal source, @client.resolve_alias(@logical)
    assert_equal 2, @client.retrieve_collection_schema(@logical).fetch(:num_documents)
    assert_equal 10, @client.retrieve_document(collection: @logical, id: 'old').fetch(:quantity)
  end

  def test_forced_schema_task_cuts_over_and_applies_retention_after_a_complete_import
    Inventory.rows_by_partition = { first: [{ id: 'one', quantity: 1 }], second: [{ id: 'two', quantity: 2 }] }
    source = create_generation('20250101')
    point_alias(source)

    status, error = run_apply_task(force_rebuild: true)

    assert_equal 0, status, error
    current = @client.resolve_alias(@logical)
    refute_equal source, current
    assert_equal [current], generations
    assert_equal %w[one two], search_ids
  end

  def test_lost_alias_acknowledgement_preserves_the_live_generation
    source = create_generation('20250101')
    point_alias(source)
    upsert = @client.method(:upsert_alias)
    failure = SearchEngine::Errors::Timeout.new('alias response lost')
    lost_response = lambda do |logical, physical|
      upsert.call(logical, physical)
      raise failure
    end

    @client.stub(:upsert_alias, lost_response) do
      raised = assert_raises(SearchEngine::Errors::Timeout) do
        SearchEngine::Schema.apply!(Inventory, force_rebuild: true) do |physical|
          import_fixture(physical, [{ id: 'new', quantity: 42 }])
        end
      end
      assert_same failure, raised
    end

    current = @client.resolve_alias(@logical)
    refute_equal source, current
    assert_equal [source, current].sort, generations.sort
    assert_equal ['new'], search_ids
  end

  def test_competing_guarded_rollbacks_only_accept_one_destination
    oldest, previous, source = %w[20250101 20250102 20250103].map { |date| create_generation(date) }
    point_alias(source)
    waiting = Queue.new
    lock = Mutex.new
    SearchEngine.config.schema.around_rebuild = lambda do |collection:, &rollback|
      waiting << collection
      lock.synchronize(&rollback)
    end

    workers = lock.synchronize do
      threads = [oldest, previous].map do |destination|
        Thread.new do
          SearchEngine::Schema.rollback(Inventory, to: destination, expected_current: source)
        rescue SearchEngine::Schema::RollbackConflict => error
          error
        end
      end
      Timeout.timeout(10) { 2.times { assert_equal @logical, waiting.pop } }
      threads
    end

    success, conflicts = workers.map(&:value).partition { |result| result.is_a?(Hash) }
    assert_equal 1, success.size
    assert_equal 1, conflicts.size
    assert success.first.fetch(:changed)
    assert_equal success.first.fetch(:new_target), @client.resolve_alias(@logical)
  end

  private

  def create_generation(date)
    name = "#{@logical}_#{date}_000000_001"
    @client.create_collection(SearchEngine::Schema.compile(Inventory).merge(name: name))
    name
  end

  def point_alias(target)
    @client.upsert_alias(@logical, target)
    @owned_aliases << @logical
  end

  def generations
    @client.list_collections.map { |schema| schema.fetch(:name) }.select { |name| name.start_with?("#{@logical}_") }
  end

  def search_ids
    result = request_json(:get, "/collections/#{@logical}/documents/search", params: { q: '*', use_cache: false })
    result.fetch('hits').map { |hit| hit.fetch('document').fetch('id') }.sort
  end

  def run_apply_task(force_rebuild:)
    previous_force = ENV['FORCE_REBUILD']
    ENV['FORCE_REBUILD'] = force_rebuild.to_s
    exit_status = nil
    _, error = capture_io do
      Rake.with_application do
        Rake::Task.define_task(:environment)
        load File.expand_path('../lib/tasks/search_engine.rake', __dir__)
        exit_status = assert_raises(SystemExit) { Rake::Task['search_engine:schema:apply'].invoke(@logical) }.status
      end
    end
    [exit_status, error]
  ensure
    ENV['FORCE_REBUILD'] = previous_force
  end
end
