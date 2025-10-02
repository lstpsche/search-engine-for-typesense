# frozen_string_literal: true

require 'test_helper'

class ParserTest < Minitest::Test
  class Product < SearchEngine::Base
    collection 'products'
    attribute :id, :integer
    attribute :name, :string
    attribute :active, :boolean
    attribute :price, :float
    attribute :brand_id, :integer
    attribute :created_at, :time
  end

  def test_hash_scalar_to_eq
    node = SearchEngine::DSL::Parser.parse({ id: 1 }, klass: Product)
    assert_kind_of SearchEngine::AST::Eq, node
    assert_equal 'id', node.field
    assert_equal 1, node.value
  end

  def test_hash_array_to_in
    node = SearchEngine::DSL::Parser.parse({ brand_id: [1, 2] }, klass: Product)
    assert_kind_of SearchEngine::AST::In, node
    assert_equal 'brand_id', node.field
    assert_equal [1, 2], node.values
  end

  def test_hash_multiple_keys_returns_array
    nodes = SearchEngine::DSL::Parser.parse({ id: 1, active: true }, klass: Product)
    assert_kind_of Array, nodes
    assert_equal 2, nodes.length
    assert(nodes.any? { |n| n.is_a?(SearchEngine::AST::Eq) && n.field == 'id' })
    assert(nodes.any? { |n| n.is_a?(SearchEngine::AST::Eq) && n.field == 'active' })
  end

  def test_fragment_args_operators
    assert_kind_of SearchEngine::AST::Gt, SearchEngine::DSL::Parser.parse(['price > ?', 100], klass: Product)
    assert_kind_of SearchEngine::AST::Gte, SearchEngine::DSL::Parser.parse(['price >= ?', 100], klass: Product)
    assert_kind_of SearchEngine::AST::Lt, SearchEngine::DSL::Parser.parse(['price < ?', 10], klass: Product)
    assert_kind_of SearchEngine::AST::Lte, SearchEngine::DSL::Parser.parse(['price <= ?', 10], klass: Product)
    assert_kind_of SearchEngine::AST::In, SearchEngine::DSL::Parser.parse(['brand_id IN ?', [1, 2, 3]], klass: Product)
    assert_kind_of SearchEngine::AST::NotIn,
                   SearchEngine::DSL::Parser.parse(['brand_id NOT IN ?', [1, 2, 3]], klass: Product)
    assert_kind_of SearchEngine::AST::Matches,
                   SearchEngine::DSL::Parser.parse(['name MATCHES ?', 'mil.*'], klass: Product)
    assert_kind_of SearchEngine::AST::Prefix, SearchEngine::DSL::Parser.parse(['name PREFIX ?', 'mil'], klass: Product)
  end

  def test_raw_string_returns_raw
    node = SearchEngine::DSL::Parser.parse('brand_id:=[1,2,3]', klass: Product)
    assert_kind_of SearchEngine::AST::Raw, node
  end

  def test_unknown_field_in_hash_raises
    error = assert_raises(ArgumentError) do
      SearchEngine::DSL::Parser.parse({ unknown: 1 }, klass: Product)
    end
    assert_match(/Unknown attribute/, error.message)
    assert_match(/Product/, error.message)
  end

  def test_unknown_field_in_template_raises
    error = assert_raises(ArgumentError) do
      SearchEngine::DSL::Parser.parse(['unknown > ?', 10], klass: Product)
    end
    assert_match(/Unknown attribute/, error.message)
  end

  def test_placeholder_mismatch_raises
    error = assert_raises(ArgumentError) do
      SearchEngine::DSL::Parser.parse('price > ?', args: [], klass: Product)
    end
    assert_match(/expected 1 args/, error.message)
  end

  def test_boolean_coercion_from_string_when_boolean_type
    node = SearchEngine::DSL::Parser.parse(['active = ?', 'true'], klass: Product)
    assert_kind_of SearchEngine::AST::Eq, node
    assert_equal true, node.value
  end

  def test_date_coercion_to_time_utc
    d = Date.new(2020, 1, 1)
    node = SearchEngine::DSL::Parser.parse(['created_at >= ?', d], klass: Product)
    assert_kind_of SearchEngine::AST::Gte, node
    assert_kind_of Time, node.value
    assert_equal true, node.value.utc?
  end

  def test_list_parsing_multiple_inputs
    nodes = SearchEngine::DSL::Parser.parse_list(
      [
        { id: 1 },
        ['price > ?', 100],
        'brand_id:=[1,2]'
      ],
      klass: Product
    )

    assert_equal 3, nodes.length
    assert(nodes.any? { |n| n.is_a?(SearchEngine::AST::Eq) })
    assert(nodes.any? { |n| n.is_a?(SearchEngine::AST::Gt) })
    assert(nodes.any? { |n| n.is_a?(SearchEngine::AST::Raw) })
  end
end
