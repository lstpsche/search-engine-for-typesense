# frozen_string_literal: true

require 'test_helper'

class SchemaCompileTest < Minitest::Test
  class Product < SearchEngine::Base
    collection 'schema_products'
    identify_by :id
    attribute :name, :string
    attribute :active, :boolean
    attribute :price, :float
    attribute :created_at, :time
  end

  def test_compile_builds_typesense_schema
    schema = SearchEngine::Schema.compile(Product)

    assert_equal 'schema_products', schema[:name]
    assert_equal [
      { name: 'name', type: 'string' },
      { name: 'active', type: 'bool' },
      { name: 'price', type: 'float' },
      { name: 'created_at', type: 'int64' }
    ], schema[:fields]

    assert schema.frozen?
    assert schema[:fields].frozen?
  end
end
