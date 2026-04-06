# frozen_string_literal: true

require 'test_helper'

class RelationWhereASTTest < Minitest::Test
  class Product < SearchEngine::Base
    collection 'products_relation'
    identify_by :id
    attribute :active, :boolean
    attribute :price, :float
    attribute :brand_id, :integer
  end

  def test_where_populates_ast_and_strings
    r_1 = Product.all
    r_2 = r_1.where({ id: 1 }, ['price > ?', 100], 'brand_id:=[1,2]')

    # Immutability
    refute_equal r_1.object_id, r_2.object_id

    # String fragments preserved for back-compat
    params = r_2.to_typesense_params
    assert_match(/id:=/, params[:filter_by])
    # When using template with placeholders, current sanitizer keeps operator tokens
    assert_match(/price:>\d+/, params[:filter_by])

    # AST present in internal state and public reader
    state = r_2.instance_variable_get(:@state)
    ast_state = Array(state[:ast])
    assert_equal 3, ast_state.length
    assert_equal 3, r_2.ast.length

    # No-op chain preserves AST state
    r_3 = r_2.where
    state_3 = r_3.instance_variable_get(:@state)
    assert_equal ast_state, state_3[:ast]
  end

  def test_where_template_in_keeps_array_bind
    rel = Product.all.where('brand_id IN ?', [1, 2])

    node = rel.ast.last
    assert_kind_of SearchEngine::AST::In, node
    assert_equal 'brand_id', node.field
    assert_equal [1, 2], node.values
  end

  def test_where_not_template_in_keeps_array_bind
    rel = Product.all.where.not('brand_id IN ?', [1, 2])

    node = rel.ast.last
    assert_kind_of SearchEngine::AST::NotIn, node
    assert_equal 'brand_id', node.field
    assert_equal [1, 2], node.values
  end
end
