# frozen_string_literal: true

require 'test_helper'

class SchemaDiffTest < Minitest::Test
  class Product < SearchEngine::Base
    collection 'diff_products'
    identify_by :id
    attribute :name, :string
    attribute :active, :boolean
    attribute :price, :float
    attribute :created_at, :time
  end

  class FakeNotFound < StandardError
    def http_code
      404
    end

    def body
      { 'message' => 'not found' }
    end
  end

  class AliasRef
    def initialize(name, mapping)
      @name = name
      @mapping = mapping
    end

    def retrieve
      physical = @mapping[@name]
      raise FakeNotFound unless physical

      { 'name' => @name, 'collection_name' => physical }
    end
  end

  class Aliases
    def initialize(mapping)
      @mapping = mapping
    end

    def [](name)
      AliasRef.new(name, @mapping)
    end
  end

  class CollectionRef
    def initialize(name, schemas)
      @name = name
      @schemas = schemas
    end

    def retrieve
      schema = @schemas[@name]
      raise FakeNotFound unless schema

      schema
    end
  end

  class Collections
    def initialize(schemas)
      @schemas = schemas
    end

    def [](name)
      CollectionRef.new(name, @schemas)
    end
  end

  class FakeTypesense
    attr_reader :aliases, :collections

    def initialize(alias_map:, collection_schemas: {})
      @aliases = Aliases.new(alias_map)
      @collections = Collections.new(collection_schemas)
    end
  end

  def build_live(fields)
    { 'name' => 'diff_products', 'fields' => fields }
  end

  def compiled_fields
    [
      { 'name' => 'id', 'type' => 'int64' },
      { 'name' => 'name', 'type' => 'string' },
      { 'name' => 'active', 'type' => 'bool' },
      { 'name' => 'price', 'type' => 'float' },
      { 'name' => 'created_at', 'type' => 'int64' }
    ]
  end

  def new_client(fake)
    SearchEngine::Client.new(typesense_client: fake)
  end

  def test_diff_no_changes_when_live_matches_compiled
    fake = FakeTypesense.new(alias_map: {}, collection_schemas: {
                               'diff_products' => build_live(compiled_fields)
                             }
    )

    result = SearchEngine::Schema.diff(Product, client: new_client(fake))
    diff = result[:diff]

    assert_equal({ name: 'diff_products', physical: 'diff_products' }, diff[:collection])
    assert_empty diff[:added_fields]
    assert_empty diff[:removed_fields]
    assert_empty diff[:changed_fields]
    assert_equal "Collection: diff_products\nNo changes", result[:pretty]
  end

  def test_diff_added_removed_changed_fields
    live_fields = [
      { 'name' => 'id', 'type' => 'int64' },
      { 'name' => 'name', 'type' => 'string' },
      # active removed
      { 'name' => 'price', 'type' => 'int32' }, # changed type
      { 'name' => 'old_attr', 'type' => 'string' }
    ]

    fake = FakeTypesense.new(alias_map: {}, collection_schemas: {
                               'diff_products' => build_live(live_fields)
                             }
    )

    result = SearchEngine::Schema.diff(Product, client: new_client(fake))
    diff = result[:diff]

    added_names = diff[:added_fields].map { |f| f[:name] }
    removed_names = diff[:removed_fields].map { |f| f[:name] }

    assert_includes added_names, 'active'
    assert_includes added_names, 'created_at'
    assert_includes removed_names, 'old_attr'

    assert_equal({ 'price' => { 'type' => %w[float int32] } }, diff[:changed_fields])
  end

  def test_diff_resolves_alias_to_physical
    fake = FakeTypesense.new(alias_map: { 'diff_products' => 'diff_products_v1' }, collection_schemas: {
                               'diff_products_v1' => build_live(compiled_fields)
                             }
    )

    result = SearchEngine::Schema.diff(Product, client: new_client(fake))
    diff = result[:diff]

    assert_equal({ name: 'diff_products', physical: 'diff_products_v1' }, diff[:collection])
    assert_equal "Collection: diff_products -> diff_products_v1\nNo changes", result[:pretty]
  end

  def test_diff_missing_collection_returns_added_fields
    fake = FakeTypesense.new(alias_map: {}, collection_schemas: {})

    result = SearchEngine::Schema.diff(Product, client: new_client(fake))
    diff = result[:diff]

    assert_equal({ name: 'diff_products', physical: 'diff_products' }, diff[:collection])
    expected_added = [
      { name: 'name', type: 'string' },
      { name: 'active', type: 'bool' }
    ]
    assert_equal expected_added, diff[:added_fields]
    assert_empty diff[:removed_fields]
    assert_empty diff[:changed_fields]
    assert_equal({ live: :missing }, diff[:collection_options])
  end
end
