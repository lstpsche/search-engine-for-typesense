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

  def test_where_wrapped_nested_template_list_preserves_ast_nodes
    rel = Product.all.where([['price > ?', 100], ['brand_id IN ?', [1, 2]]])

    assert_equal 2, rel.ast.length

    gt_node = rel.ast.find { |n| n.is_a?(SearchEngine::AST::Gt) }
    in_node = rel.ast.find { |n| n.is_a?(SearchEngine::AST::In) }

    refute_nil gt_node
    assert_equal 'price', gt_node.field
    assert_equal 100.0, gt_node.value

    refute_nil in_node
    assert_equal 'brand_id', in_node.field
    assert_equal [1, 2], in_node.values
  end

  def test_where_not_wrapped_nested_template_list_preserves_negation
    rel = Product.all.where.not([['brand_id IN ?', [1, 2]], ['active = ?', true]])

    assert_equal 2, rel.ast.length

    not_in = rel.ast.find { |n| n.is_a?(SearchEngine::AST::NotIn) }
    not_eq = rel.ast.find { |n| n.is_a?(SearchEngine::AST::NotEq) }

    refute_nil not_in
    assert_equal 'brand_id', not_in.field
    assert_equal [1, 2], not_in.values

    refute_nil not_eq
    assert_equal 'active', not_eq.field
    assert_equal true, not_eq.value
  end

  def test_where_empty_array_is_noop_like_active_record
    rel = Product.all

    assert_same rel, rel.where([])
  end

  def test_where_not_empty_array_raises_like_active_record
    error = assert_raises(ArgumentError) { Product.all.where.not([]) }

    assert_match(/Unsupported argument type:  \(NilClass\)/, error.message)
  end

  def test_where_nested_empty_array_raises_like_active_record
    error = assert_raises(ArgumentError) { Product.all.where([[]]) }

    assert_match(/Unsupported argument type: \[\] \(Array\)/, error.message)
  end

  def test_where_not_nested_empty_array_raises_like_active_record
    error = assert_raises(ArgumentError) { Product.all.where.not([[]]) }

    assert_match(/Unsupported argument type: \[\] \(Array\)/, error.message)
  end
end
