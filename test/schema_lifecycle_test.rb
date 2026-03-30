# frozen_string_literal: true

require 'test_helper'

class SchemaLifecycleTest < Minitest::Test
  class Product < SearchEngine::Base
    collection 'products_lifecycle'
    identify_by :id
    attribute :name, :string
  end

  # Simple fake client to simulate Typesense behavior
  class FakeClient
    attr_reader :created, :upserts, :deleted
    attr_accessor :collections, :alias_target, :schemas

    def initialize(collections: [], alias_target: nil, schemas: {})
      @collections = collections.dup # Array of Hashes { name: '...' }
      @alias_target = alias_target
      @schemas = schemas # Hash{ String => Hash } — keyed by name passed to retrieve
      @created = []
      @upserts = []
      @deleted = []
    end

    def resolve_alias(_logical)
      @alias_target
    end

    def upsert_alias(logical, physical)
      @upserts << [logical, physical]
      @alias_target = physical
      { name: logical, collection_name: physical }
    end

    def create_collection(schema)
      @created << schema[:name]
      @collections << { name: schema[:name] }
      schema
    end

    def delete_collection(name, timeout_ms: nil) # rubocop:disable Lint/UnusedMethodArgument -- matches real client signature
      @deleted << name
      @collections.reject! { |c| (c[:name] || c['name']) == name }
      { name: name, status: 200 }
    end

    def list_collections(*)
      @collections
    end

    def retrieve_collection_schema(name)
      @schemas[name.to_s]
    end
  end

  def setup
    # Global default: keep none unless overridden
    SearchEngine.configure { |c| c.schema.retention.keep_last = 0 }
  end

  def test_name_generator_sequence_increments_on_conflict
    fixed = Time.utc(2025, 1, 31, 23, 59, 59)
    logical = Product.collection
    prefix = format('%<logical>s_%<fixed>s_', logical: logical, fixed: fixed.strftime('%Y%m%d_%H%M%S'))
    existing = [
      { name: "#{prefix}001" },
      { name: "#{prefix}003" }
    ]

    client = FakeClient.new(collections: existing, alias_target: nil)

    Time.stub(:now, fixed) do
      result = SearchEngine::Schema.apply!(Product, client: client) { |_name| }
      assert_match(/^products_lifecycle_\d{8}_\d{6}_\d{3}$/i, result[:new_physical])
      assert_equal("#{prefix}002", result[:new_physical])
    end
  end

  def test_alias_swap_is_idempotent_when_already_pointing
    client = FakeClient.new(collections: [], alias_target: nil)
    forced = 'products_lifecycle_20250101_000000_001'

    SearchEngine::Schema.stub(:generate_physical_name, forced) do
      # Pre-point alias to the same physical
      client.alias_target = forced
      res = SearchEngine::Schema.apply!(Product, client: client) { |_name| }
      assert_equal(forced, res[:alias_target])
      assert_empty(client.upserts, 'no alias upsert when already pointing to new physical')
    end
  end

  def test_retention_deletes_only_matching_and_not_alias_target
    # Create a spread of physicals and non-matching names
    names = %w[
      products_lifecycle_20250101_000001_001
      products_lifecycle_20250102_000001_001
      products_lifecycle_20250103_000001_001
      other_20250101_000001_001
    ].map { |n| { name: n } }
    client = FakeClient.new(collections: names, alias_target: 'products_lifecycle_20250103_000001_001')

    # Per-collection keep_last=1
    Product.schema_retention(keep_last: 1)

    forced = 'products_lifecycle_20250104_000001_001'
    SearchEngine::Schema.stub(:generate_physical_name, forced) do
      SearchEngine::Schema.apply!(Product, client: client) { |_name| }
    end

    # After swap, alias points to forced; ensure only older matching beyond 1 kept were deleted
    deleted = client.deleted
    refute_includes(deleted, 'products_lifecycle_20250104_000001_001')
    refute_includes(deleted, 'products_lifecycle_20250103_000001_001')
    refute_includes(deleted, 'other_20250101_000001_001')
    assert_includes(deleted, 'products_lifecycle_20250101_000001_001')
  end

  def test_rollback_swaps_back_to_previous
    names = %w[
      products_lifecycle_20250101_000001_001
      products_lifecycle_20250102_000001_001
    ].map { |n| { name: n } }
    client = FakeClient.new(collections: names, alias_target: 'products_lifecycle_20250102_000001_001')

    res = SearchEngine::Schema.rollback(Product, client: client)
    assert_equal('products_lifecycle', res[:logical])
    assert_equal('products_lifecycle_20250101_000001_001', res[:new_target])
    assert_equal([%w[products_lifecycle products_lifecycle_20250101_000001_001]], client.upserts)
  end

  def test_rollback_raises_when_no_previous
    names = [%w[products_lifecycle_20250101_000001_001]].flatten.map { |n| { name: n } }
    client = FakeClient.new(collections: names, alias_target: 'products_lifecycle_20250101_000001_001')
    assert_raises(ArgumentError) { SearchEngine::Schema.rollback(Product, client: client) }
  end

  def test_reindex_failure_cleans_up_physical
    client = FakeClient.new(collections: [], alias_target: nil)
    assert_raises RuntimeError do
      SearchEngine::Schema.apply!(Product, client: client) { |_name| raise 'boom' }
    end
    assert_empty(client.upserts)
    refute_empty(client.created)
    assert_equal(client.created, client.deleted)
  end

  def test_swap_failure_cleans_up_physical
    client = FakeClient.new(collections: [], alias_target: nil)
    forced = 'products_lifecycle_20250101_000001_001'

    # Monkey-patch upsert to raise API error
    def client.upsert_alias(_logical, _physical)
      raise SearchEngine::Errors::Api.new('fail', status: 500, body: nil)
    end

    assert_raises(SearchEngine::Errors::Api) do
      SearchEngine::Schema.stub(:generate_physical_name, forced) do
        SearchEngine::Schema.apply!(Product, client: client) { |_name| }
      end
    end

    assert_equal([forced], client.deleted)
  end

  # --- cleanup_logical_collection_conflict! tests ---

  def test_cleanup_skips_when_alias_resolves_transparently
    physical = 'products_lifecycle_20250101_000001_001'
    # retrieve_collection_schema returns physical name (alias was resolved, no bare collection)
    client = FakeClient.new(
      alias_target: physical,
      schemas: { 'products_lifecycle' => { name: physical, fields: [] } }
    )

    SearchEngine::Schema.send(:cleanup_logical_collection_conflict!, 'products_lifecycle', client: client)
    assert_empty(client.deleted, 'should NOT delete when alias resolves transparently')
  end

  def test_cleanup_deletes_bare_collection_that_shadows_alias
    physical = 'products_lifecycle_20250101_000001_001'
    # retrieve_collection_schema returns logical name (bare collection exists)
    client = FakeClient.new(
      collections: [{ name: 'products_lifecycle' }, { name: physical }],
      alias_target: physical,
      schemas: { 'products_lifecycle' => { name: 'products_lifecycle', fields: [] } }
    )

    SearchEngine::Schema.send(:cleanup_logical_collection_conflict!, 'products_lifecycle', client: client)
    assert_includes(client.deleted, 'products_lifecycle', 'should delete bare collection shadowing alias')
  end

  def test_cleanup_skips_when_no_alias_exists
    client = FakeClient.new(
      alias_target: nil,
      schemas: { 'products_lifecycle' => { name: 'products_lifecycle', fields: [] } }
    )

    SearchEngine::Schema.send(:cleanup_logical_collection_conflict!, 'products_lifecycle', client: client)
    assert_empty(client.deleted, 'should NOT delete when no alias exists')
  end

  def test_cleanup_skips_when_alias_points_to_logical_name
    # Edge case: alias target equals logical name
    client = FakeClient.new(
      alias_target: 'products_lifecycle',
      schemas: { 'products_lifecycle' => { name: 'products_lifecycle', fields: [] } }
    )

    SearchEngine::Schema.send(:cleanup_logical_collection_conflict!, 'products_lifecycle', client: client)
    assert_empty(client.deleted, 'should NOT delete when alias points to logical name itself')
  end

  def test_cleanup_skips_when_schema_not_found
    physical = 'products_lifecycle_20250101_000001_001'
    # retrieve_collection_schema returns nil (neither bare collection nor alias resolves)
    client = FakeClient.new(alias_target: physical, schemas: {})

    SearchEngine::Schema.send(:cleanup_logical_collection_conflict!, 'products_lifecycle', client: client)
    assert_empty(client.deleted, 'should NOT delete when schema lookup returns nil')
  end

  def test_cleanup_handles_string_keys_in_schema_response
    physical = 'products_lifecycle_20250101_000001_001'
    # Schema returned with string keys (as from real Typesense API)
    client = FakeClient.new(
      alias_target: physical,
      schemas: { 'products_lifecycle' => { 'name' => physical, 'fields' => [] } }
    )

    SearchEngine::Schema.send(:cleanup_logical_collection_conflict!, 'products_lifecycle', client: client)
    assert_empty(client.deleted, 'should handle string keys and skip alias-resolved schema')
  end
end
