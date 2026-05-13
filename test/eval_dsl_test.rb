# frozen_string_literal: true

require 'test_helper'

class EvalDslTest < Minitest::Test
  class EvalVenue < SearchEngine::Base
    collection 'eval_venues_dsl'
    identify_by :id
    attribute :name, :string
    attribute :location, :geopoint
    attribute :rating, :float
  end

  # ---------------------------------------------------------------------------
  # order_eval — compilation
  # ---------------------------------------------------------------------------

  def test_order_eval_simple_expression
    rel = EvalVenue.all.order_eval('location:(54.72,25.35, 54.72,25.22, 54.67,25.22, 54.67,25.35)')
    params = rel.to_typesense_params

    assert_equal '_eval(location:(54.72,25.35, 54.72,25.22, 54.67,25.22, 54.67,25.35)):desc', params[:sort_by]
  end

  def test_order_eval_default_direction_is_desc
    rel = EvalVenue.all.order_eval('rating:>4.5')
    params = rel.to_typesense_params

    assert_match(/:desc\z/, params[:sort_by])
  end

  def test_order_eval_asc_direction
    rel = EvalVenue.all.order_eval('rating:>4.5', direction: :asc)
    params = rel.to_typesense_params

    assert_equal '_eval(rating:>4.5):asc', params[:sort_by]
  end

  def test_order_eval_weighted_expression
    rel = EvalVenue.all.order_eval(
      [
        { expr: 'location:(54.72,25.35, 54.72,25.22, 54.67,25.22, 54.67,25.35)', weight: 3 },
        { expr: 'rating:>4.5', weight: 1 }
      ]
    )
    params = rel.to_typesense_params

    expected = '_eval([ (location:(54.72,25.35, 54.72,25.22, 54.67,25.22, 54.67,25.35)):3, (rating:>4.5):1 ]):desc'
    assert_equal expected, params[:sort_by]
  end

  def test_order_eval_chains_with_order_geo
    rel = EvalVenue.all
                   .order_eval('location:(54.72,25.35, 54.72,25.22, 54.67,25.22, 54.67,25.35)')
                   .order_geo(:location, from: { lat: 54.69, lng: 25.28 })
    params = rel.to_typesense_params

    sort = params[:sort_by]
    assert_match(/\A_eval\(/, sort)
    assert_match(/location\(54\.69, 25\.28\):asc/, sort)
  end

  def test_order_eval_chains_with_standard_order
    rel = EvalVenue.all
                   .order_eval('rating:>4.5')
                   .order('name:asc')
    params = rel.to_typesense_params

    assert_match(/_eval\(rating:>4\.5\):desc/, params[:sort_by])
    assert_match(/name:asc/, params[:sort_by])
  end

  def test_order_eval_preserves_immutability
    original = EvalVenue.all
    _chained = original.order_eval('rating:>4.5')

    refute original.to_typesense_params.key?(:sort_by)
  end

  # ---------------------------------------------------------------------------
  # order_eval — validation errors
  # ---------------------------------------------------------------------------

  def test_order_eval_raises_on_blank_expression
    err = assert_raises(ArgumentError) do
      EvalVenue.all.order_eval('  ')
    end

    assert_match(/expression must not be blank/, err.message)
  end

  def test_order_eval_raises_on_invalid_direction
    err = assert_raises(ArgumentError) do
      EvalVenue.all.order_eval('rating:>4.5', direction: :random)
    end

    assert_match(/order_eval: direction must be :asc or :desc/, err.message)
  end

  def test_order_eval_raises_on_non_string_non_array
    err = assert_raises(ArgumentError) do
      EvalVenue.all.order_eval(42)
    end

    assert_match(/expression must be a String or Array/, err.message)
  end

  def test_order_eval_raises_on_empty_array
    err = assert_raises(ArgumentError) do
      EvalVenue.all.order_eval([])
    end

    assert_match(/weighted form expects a non-empty Array/, err.message)
  end

  def test_order_eval_raises_on_missing_expr_in_weighted
    err = assert_raises(ArgumentError) do
      EvalVenue.all.order_eval([{ expr: '', weight: 1 }])
    end

    assert_match(/entry 0 must have a non-blank :expr/, err.message)
  end

  def test_order_eval_raises_on_zero_weight
    err = assert_raises(ArgumentError) do
      EvalVenue.all.order_eval([{ expr: 'rating:>4.5', weight: 0 }])
    end

    assert_match(/entry 0 :weight must be a positive Integer/, err.message)
  end

  def test_order_eval_raises_on_negative_weight
    err = assert_raises(ArgumentError) do
      EvalVenue.all.order_eval([{ expr: 'rating:>4.5', weight: -1 }])
    end

    assert_match(/entry 0 :weight must be a positive Integer/, err.message)
  end

  # ---------------------------------------------------------------------------
  # Coverage gap CG#2: nil expression
  # ---------------------------------------------------------------------------

  def test_order_eval_raises_on_nil_expression
    assert_raises(ArgumentError) do
      EvalVenue.all.order_eval(nil)
    end
  end
end
