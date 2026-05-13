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

  class GeoPlace < SearchEngine::Base
    collection 'schema_geo_places'
    identify_by :id
    attribute :name, :string
    attribute :location, :geopoint
    attribute :locations, [:geopoint]
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

  def test_geopoint_attribute_compiles_to_geopoint_type
    schema = SearchEngine::Schema.compile(GeoPlace)
    fields = schema[:fields]

    geo_field = fields.find { |f| f[:name] == 'location' }
    assert_equal 'geopoint', geo_field[:type]
  end

  def test_geopoint_array_attribute_compiles_to_geopoint_array_type
    schema = SearchEngine::Schema.compile(GeoPlace)
    fields = schema[:fields]

    geo_array_field = fields.find { |f| f[:name] == 'locations' }
    assert_equal 'geopoint[]', geo_array_field[:type]
  end
end
