# frozen_string_literal: true

require 'test_helper'

class GeoDslTest < Minitest::Test
  class GeoVenue < SearchEngine::Base
    collection 'geo_venues_dsl'
    identify_by :id
    attribute :name, :string
    attribute :location, :geopoint
    attribute :rating, :float
  end

  # ---------------------------------------------------------------------------
  # where_geo — radius filter
  # ---------------------------------------------------------------------------

  def test_where_geo_radius_compiles_filter_by
    rel = GeoVenue.all.where_geo(:location, within_radius: { lat: 54.69, lng: 25.28, radius: '10 km' })
    params = rel.to_typesense_params

    assert_equal 'location:(54.69, 25.28, 10 km)', params[:filter_by]
  end

  def test_where_geo_radius_with_decimal_radius
    rel = GeoVenue.all.where_geo(:location, within_radius: { lat: 54.69, lng: 25.28, radius: '2.5 mi' })
    params = rel.to_typesense_params

    assert_equal 'location:(54.69, 25.28, 2.5 mi)', params[:filter_by]
  end

  # ---------------------------------------------------------------------------
  # where_geo — polygon filter
  # ---------------------------------------------------------------------------

  def test_where_geo_polygon_compiles_filter_by
    points = [[54.72, 25.35], [54.72, 25.22], [54.67, 25.22], [54.67, 25.35]]
    rel = GeoVenue.all.where_geo(:location, within_polygon: points)
    params = rel.to_typesense_params

    assert_equal 'location:(54.72, 25.35, 54.72, 25.22, 54.67, 25.22, 54.67, 25.35)', params[:filter_by]
  end

  def test_where_geo_polygon_with_3_points
    points = [[54.72, 25.35], [54.72, 25.22], [54.67, 25.22]]
    rel = GeoVenue.all.where_geo(:location, within_polygon: points)
    params = rel.to_typesense_params

    assert_equal 'location:(54.72, 25.35, 54.72, 25.22, 54.67, 25.22)', params[:filter_by]
  end

  # ---------------------------------------------------------------------------
  # where_geo — chaining
  # ---------------------------------------------------------------------------

  def test_where_geo_chains_with_where
    rel = GeoVenue.all
                  .where(rating: 5.0)
                  .where_geo(:location, within_radius: { lat: 54.69, lng: 25.28, radius: '10 km' })
    params = rel.to_typesense_params

    assert_match(/rating:=5\.0/, params[:filter_by])
    assert_match(/location:\(54\.69, 25\.28, 10 km\)/, params[:filter_by])
    assert_match(/&&/, params[:filter_by])
  end

  def test_where_geo_preserves_immutability
    original = GeoVenue.all
    _chained = original.where_geo(:location, within_radius: { lat: 54.69, lng: 25.28, radius: '10 km' })

    refute original.to_typesense_params.key?(:filter_by)
  end

  # ---------------------------------------------------------------------------
  # where_geo — validation errors
  # ---------------------------------------------------------------------------

  def test_where_geo_raises_on_non_geopoint_field
    err = assert_raises(ArgumentError) do
      GeoVenue.all.where_geo(:name, within_radius: { lat: 54.69, lng: 25.28, radius: '10 km' })
    end

    assert_match(/field :name must be declared as :geopoint/, err.message)
  end

  def test_where_geo_raises_on_lat_out_of_range
    err = assert_raises(ArgumentError) do
      GeoVenue.all.where_geo(:location, within_radius: { lat: 91, lng: 25.28, radius: '10 km' })
    end

    assert_match(/lat must be a number in \[-90, 90\]/, err.message)
  end

  def test_where_geo_raises_on_negative_lat_out_of_range
    err = assert_raises(ArgumentError) do
      GeoVenue.all.where_geo(:location, within_radius: { lat: -91, lng: 25.28, radius: '10 km' })
    end

    assert_match(/lat must be a number in \[-90, 90\]/, err.message)
  end

  def test_where_geo_raises_on_lng_out_of_range
    err = assert_raises(ArgumentError) do
      GeoVenue.all.where_geo(:location, within_radius: { lat: 54.69, lng: 181, radius: '10 km' })
    end

    assert_match(/lng must be a number in \[-180, 180\]/, err.message)
  end

  def test_where_geo_raises_on_invalid_radius_format
    err = assert_raises(ArgumentError) do
      GeoVenue.all.where_geo(:location, within_radius: { lat: 54.69, lng: 25.28, radius: '10' })
    end

    assert_match(/radius must be a string like '10 km' or '5 mi'/, err.message)
  end

  def test_where_geo_raises_on_numeric_radius
    err = assert_raises(ArgumentError) do
      GeoVenue.all.where_geo(:location, within_radius: { lat: 54.69, lng: 25.28, radius: 10 })
    end

    assert_match(/radius must be a string/, err.message)
  end

  def test_where_geo_raises_on_polygon_fewer_than_3_points
    err = assert_raises(ArgumentError) do
      GeoVenue.all.where_geo(:location, within_polygon: [[54.72, 25.35], [54.72, 25.22]])
    end

    assert_match(/polygon must have >= 3 points/, err.message)
  end

  def test_where_geo_raises_on_malformed_polygon_point
    err = assert_raises(ArgumentError) do
      GeoVenue.all.where_geo(:location, within_polygon: [[54.72, 25.35], [54.72], [54.67, 25.22]])
    end

    assert_match(/polygon point 1 must be \[lat, lng\]/, err.message)
  end

  def test_where_geo_raises_when_both_radius_and_polygon_given
    err = assert_raises(ArgumentError) do
      GeoVenue.all.where_geo(
        :location,
        within_radius: { lat: 54.69, lng: 25.28, radius: '10 km' },
        within_polygon: [[54.72, 25.35], [54.72, 25.22], [54.67, 25.22]]
      )
    end

    assert_match(/mutually exclusive/, err.message)
  end

  def test_where_geo_raises_when_neither_radius_nor_polygon_given
    err = assert_raises(ArgumentError) do
      GeoVenue.all.where_geo(:location)
    end

    assert_match(/provide either within_radius: or within_polygon:/, err.message)
  end

  def test_where_geo_accepts_boundary_coordinates
    rel = GeoVenue.all.where_geo(:location, within_radius: { lat: 90, lng: -180, radius: '1 km' })
    params = rel.to_typesense_params

    assert_equal 'location:(90, -180, 1 km)', params[:filter_by]
  end

  def test_where_geo_accepts_negative_boundary_coordinates
    rel = GeoVenue.all.where_geo(:location, within_radius: { lat: -90, lng: 180, radius: '1 km' })
    params = rel.to_typesense_params

    assert_equal 'location:(-90, 180, 1 km)', params[:filter_by]
  end

  # ---------------------------------------------------------------------------
  # order_geo — compilation
  # ---------------------------------------------------------------------------

  def test_order_geo_basic_compiles_sort_by
    rel = GeoVenue.all.order_geo(:location, from: { lat: 54.69, lng: 25.28 })
    params = rel.to_typesense_params

    assert_equal 'location(54.69, 25.28):asc', params[:sort_by]
  end

  def test_order_geo_default_direction_is_asc
    rel = GeoVenue.all.order_geo(:location, from: { lat: 54.69, lng: 25.28 })
    params = rel.to_typesense_params

    assert_match(/:asc\z/, params[:sort_by])
  end

  def test_order_geo_desc_direction
    rel = GeoVenue.all.order_geo(:location, from: { lat: 54.69, lng: 25.28 }, direction: :desc)
    params = rel.to_typesense_params

    assert_equal 'location(54.69, 25.28):desc', params[:sort_by]
  end

  def test_order_geo_with_exclude_radius
    rel = GeoVenue.all.order_geo(:location, from: { lat: 54.69, lng: 25.28 }, exclude_radius: '2 km')
    params = rel.to_typesense_params

    assert_equal 'location(54.69, 25.28, exclude_radius: 2 km):asc', params[:sort_by]
  end

  def test_order_geo_with_precision
    rel = GeoVenue.all.order_geo(:location, from: { lat: 54.69, lng: 25.28 }, precision: '1 km')
    params = rel.to_typesense_params

    assert_equal 'location(54.69, 25.28, precision: 1 km):asc', params[:sort_by]
  end

  def test_order_geo_with_exclude_radius_and_precision
    rel = GeoVenue.all.order_geo(
      :location,
      from: { lat: 54.69, lng: 25.28 },
      exclude_radius: '2 km',
      precision: '1 km'
    )
    params = rel.to_typesense_params

    assert_equal 'location(54.69, 25.28, exclude_radius: 2 km, precision: 1 km):asc', params[:sort_by]
  end

  def test_order_geo_chains_with_order
    rel = GeoVenue.all
                  .order('rating:desc')
                  .order_geo(:location, from: { lat: 54.69, lng: 25.28 })
    params = rel.to_typesense_params

    assert_match(/rating:desc/, params[:sort_by])
    assert_match(/location\(54\.69, 25\.28\):asc/, params[:sort_by])
  end

  def test_order_geo_preserves_immutability
    original = GeoVenue.all
    _chained = original.order_geo(:location, from: { lat: 54.69, lng: 25.28 })

    refute original.to_typesense_params.key?(:sort_by)
  end

  # ---------------------------------------------------------------------------
  # order_geo — validation errors
  # ---------------------------------------------------------------------------

  def test_order_geo_raises_on_non_geopoint_field
    err = assert_raises(ArgumentError) do
      GeoVenue.all.order_geo(:name, from: { lat: 54.69, lng: 25.28 })
    end

    assert_match(/order_geo: field :name must be declared as :geopoint/, err.message)
  end

  def test_order_geo_raises_on_invalid_direction
    err = assert_raises(ArgumentError) do
      GeoVenue.all.order_geo(:location, from: { lat: 54.69, lng: 25.28 }, direction: :sideways)
    end

    assert_match(/order_geo: direction must be :asc or :desc/, err.message)
  end

  def test_order_geo_raises_on_invalid_coordinates
    err = assert_raises(ArgumentError) do
      GeoVenue.all.order_geo(:location, from: { lat: 200, lng: 25.28 })
    end

    assert_match(/order_geo: lat must be a number in \[-90, 90\]/, err.message)
  end

  def test_order_geo_raises_on_invalid_exclude_radius
    err = assert_raises(ArgumentError) do
      GeoVenue.all.order_geo(:location, from: { lat: 54.69, lng: 25.28 }, exclude_radius: '2')
    end

    assert_match(/order_geo: radius must be a string like '10 km' or '5 mi'/, err.message)
  end

  # ---------------------------------------------------------------------------
  # Combined geo + eval DSL chaining
  # ---------------------------------------------------------------------------

  def test_full_geo_chain_compiles
    rel = GeoVenue.all
                  .where(rating: 5.0)
                  .where_geo(:location, within_radius: { lat: 54.69, lng: 25.28, radius: '50 km' })
                  .order_eval('location:(54.72,25.35, 54.72,25.22, 54.67,25.22, 54.67,25.35)')
                  .order_geo(:location, from: { lat: 54.69, lng: 25.28 })
    params = rel.to_typesense_params

    assert_match(/rating:=5\.0/, params[:filter_by])
    assert_match(/location:\(54\.69, 25\.28, 50 km\)/, params[:filter_by])
    assert_match(/_eval\(/, params[:sort_by])
    assert_match(/location\(54\.69, 25\.28\):asc/, params[:sort_by])
  end
end
