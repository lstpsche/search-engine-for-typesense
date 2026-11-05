# frozen_string_literal: true

require 'test_helper'

class RelationRechainersTest < Minitest::Test
  class Product < SearchEngine::Base
    collection 'products_rechainers'
    identify_by :id
    attribute :name, :string
    attribute :active, :boolean
    attribute :price, :float
    attribute :brand_id, :integer
  end

  def test_reselect_replaces_and_normalizes
    r_1 = Product.all.select(:id, :name)
    r_2 = r_1.reselect(' name ', :id, :name, :id)

    # Immutability
    refute_equal r_1.object_id, r_2.object_id

    # Replaced selection (first occurrence preserved, deduped)
    params_1 = r_1.to_typesense_params
    params_2 = r_2.to_typesense_params
    assert_equal 'id,name', params_1[:include_fields]
    assert_equal 'name,id', params_2[:include_fields]
  end

  def test_reselect_empty_raises
    r = Product.all.select(:id)
    assert_raises(ArgumentError) { r.reselect }
  end

  def test_rewhere_clears_previous_predicates_and_sets_new_ast
    r_1 = Product.all.where(id: 1).where(['price > ?', 100])
    r_2 = r_1.rewhere(active: true)

    # Immutability
    refute_equal r_1.object_id, r_2.object_id

    # Previous AST cleared, new AST present
    assert_equal 1, r_2.ast.length
    compiled = r_2.to_typesense_params[:filter_by]
    assert_equal 'active:=true', compiled

    # Legacy filters cleared as well
    state = r_2.instance_variable_get(:@state)
    assert_equal [], Array(state[:filters])
  end

  def test_rewhere_blank_raises
    r = Product.all.where(id: 1)
    assert_raises(ArgumentError) { r.rewhere(nil) }
    assert_raises(ArgumentError) { r.rewhere('') }
    assert_raises(ArgumentError) { r.rewhere([]) }
    assert_raises(ArgumentError) { r.rewhere({}) }
  end

  def test_unscope_where_clears_predicates
    r_1 = Product.all.where(id: 1).where(['price > ?', 100])
    r_2 = r_1.unscope(:where)

    refute_equal r_1.object_id, r_2.object_id

    params = r_2.to_typesense_params
    refute params.key?(:filter_by)

    state = r_2.instance_variable_get(:@state)
    assert_equal [], Array(state[:ast])
    assert_equal [], Array(state[:filters])
  end

  def test_unscope_order_select_and_pagination
    r_1 = Product.all.order(name: :asc).select(:id, :name).page(2).per(10).limit(5).offset(5)
    r_2 = r_1.unscope(:order, :select, :limit, :offset)

    p_1 = r_1.to_typesense_params
    p_2 = r_2.to_typesense_params

    # order cleared
    assert p_1.key?(:sort_by)
    refute p_2.key?(:sort_by)

    # select cleared
    assert p_1.key?(:include_fields)
    refute p_2.key?(:include_fields)

    # limit/offset cleared; page/per still present
    assert_equal 2, p_2[:page]
    assert_equal 10, p_2[:per_page]
  end

  def test_offset_is_preserved_by_hit_limits_and_preset_pagination
    window = Product.all.offset(1).limit(7)
    params = window.limit_hits(3).to_typesense_params.to_h
    assert_equal({ offset: 1, limit: 3 }, params.slice(:offset, :limit, :page, :per_page))
    assert_equal({ offset: 0 }, Product.offset(0).to_typesense_params.to_h.slice(:offset, :limit))

    preset = window.preset(:catalog, mode: :only)
    assert_equal({ offset: 1, limit: 7 }, preset.to_typesense_params.to_h.slice(:offset, :limit))
    multi = SearchEngine::Multi.new.add(:products, preset)
    payload = multi.to_payloads(common: { 'page' => 4, 'per_page' => 8 }).first
    assert_equal({ offset: 1, limit: 7 }, payload.slice(:offset, :limit, :page, :per_page))
  end

  def test_unscope_page_and_per
    r_1 = Product.all.page(3).per(15)
    r_2 = r_1.unscope(:page, :per)

    p = r_2.to_typesense_params
    refute p.key?(:page)
    refute p.key?(:per_page)
  end

  def test_unscope_unknown_raises
    r = Product.all
    error = assert_raises(ArgumentError) { r.unscope(:foo) }
    assert_match(/unscope: unknown part/i, error.message)
  end

  def test_composition_reselect_unscope_rewhere
    r = Product.all.select(:id).order(name: :asc)
    r = r.reselect(:name).unscope(:order).rewhere(['price > ?', 50])

    params = r.to_typesense_params
    assert_equal 'name', params[:include_fields]
    refute params.key?(:sort_by)
    assert_equal 'price:>50', params[:filter_by]
  end
end
