# frozen_string_literal: true

require 'test_helper'
require 'json'

class GeoIntegrationTest < Minitest::Test
  COLLECTION = "geo_integration_test_#{Process.pid}".freeze

  class Venue < SearchEngine::Base
    collection GeoIntegrationTest::COLLECTION
    identify_by :id
    attribute :name, :string
    attribute :location, :geopoint
    attribute :rating, :float
  end

  CITY_CENTER = { lat: 54.6858, lng: 25.2877 }.freeze

  POLYGON = [
    [54.695, 25.300],
    [54.695, 25.275],
    [54.675, 25.275],
    [54.675, 25.300]
  ].freeze

  DOCUMENTS = [
    { id: '1', name: 'Cathedral Square',  location: [54.6858, 25.2877], rating: 4.8 },
    { id: '2', name: 'Gediminas Tower',   location: [54.6867, 25.2912], rating: 4.6 },
    { id: '3', name: 'Town Hall',         location: [54.6791, 25.2868], rating: 4.3 },
    { id: '4', name: 'Trakai Castle',     location: [54.6522, 24.9336], rating: 4.9 },
    { id: '5', name: 'Kaunas Old Town',   location: [54.8985, 23.8858], rating: 4.5 }
  ].freeze

  IN_POLYGON_IDS  = %w[1 2 3].freeze
  OUT_POLYGON_IDS = %w[4 5].freeze
  RADIUS_5KM      = { lat: 54.6858, lng: 25.2877, radius: '5 km' }.freeze

  def setup
    skip 'Set TYPESENSE_INTEGRATION=true to run' unless ENV['TYPESENSE_INTEGRATION']

    @original_test_mode = SearchEngine.config.test_mode
    @original_client    = SearchEngine.config.client

    SearchEngine.config.test_mode = false
    SearchEngine.config.typesense.host     = ENV.fetch('TYPESENSE_HOST', 'localhost')
    SearchEngine.config.typesense.port     = Integer(ENV.fetch('TYPESENSE_PORT', 8108))
    SearchEngine.config.typesense.protocol = ENV.fetch('TYPESENSE_PROTOCOL', 'http')
    SearchEngine.config.typesense.api_key  = ENV.fetch('TYPESENSE_API_KEY', 'test_api_key')

    @client = SearchEngine::Client.new
    SearchEngine.config.client = @client

    schema = {
      name: COLLECTION,
      fields: [
        { name: 'id',       type: 'string' },
        { name: 'name',     type: 'string' },
        { name: 'location', type: 'geopoint' },
        { name: 'rating',   type: 'float' }
      ]
    }

    begin
      @client.delete_collection(COLLECTION)
    rescue StandardError # rubocop:disable Lint/SuppressedException
    end
    @client.create_collection(schema)

    jsonl = DOCUMENTS.map { |doc| JSON.generate(doc) }.join("\n")
    @client.import_documents(collection: COLLECTION, jsonl: jsonl, action: :upsert)
  end

  def teardown
    return unless ENV['TYPESENSE_INTEGRATION']

    begin
      @client&.delete_collection(COLLECTION)
    rescue StandardError # rubocop:disable Lint/SuppressedException
    end
    SearchEngine.config.test_mode = @original_test_mode
    SearchEngine.config.client    = @original_client
  end

  # ---------------------------------------------------------------------------
  # where_geo — radius filter against real Typesense
  # ---------------------------------------------------------------------------

  def test_where_geo_radius_returns_nearby_docs
    results = Venue.all.where_geo(:location, within_radius: RADIUS_5KM).to_a

    ids = results.map { |r| r.instance_variable_get(:@id) }
    IN_POLYGON_IDS.each { |id| assert_includes ids, id, "Expected doc #{id} within 5 km radius" }
    OUT_POLYGON_IDS.each { |id| refute_includes ids, id, "Expected doc #{id} outside 5 km radius" }
  end

  # ---------------------------------------------------------------------------
  # where_geo — polygon filter against real Typesense
  # ---------------------------------------------------------------------------

  def test_where_geo_polygon_returns_contained_docs
    results = Venue.all
                   .where_geo(:location, within_polygon: POLYGON)
                   .to_a

    ids = results.map { |r| r.instance_variable_get(:@id) }
    IN_POLYGON_IDS.each { |id| assert_includes ids, id, "Expected doc #{id} inside polygon" }
    OUT_POLYGON_IDS.each { |id| refute_includes ids, id, "Expected doc #{id} outside polygon" }
  end

  # ---------------------------------------------------------------------------
  # order_geo — distance sort against real Typesense
  # ---------------------------------------------------------------------------

  def test_order_geo_sorts_by_distance
    results = Venue.all
                   .order_geo(:location, from: { lat: CITY_CENTER[:lat], lng: CITY_CENTER[:lng] })
                   .to_a

    ids = results.map { |r| r.instance_variable_get(:@id) }
    assert_equal 5, ids.size

    in_city_ids = ids.first(3)
    IN_POLYGON_IDS.each { |id| assert_includes in_city_ids, id, "Expected #{id} in top 3 nearest" }

    assert_equal '5', ids.last, 'Kaunas (farthest) should be last'
  end

  # ---------------------------------------------------------------------------
  # order_eval — polygon boost against real Typesense
  # ---------------------------------------------------------------------------

  def test_order_eval_polygon_boost_ranks_in_polygon_first
    polygon_expr = polygon_filter_expression(:location, POLYGON)

    results = Venue.all
                   .order_eval(polygon_expr, direction: :desc)
                   .to_a

    ids = results.map { |r| r.instance_variable_get(:@id) }
    top_ids = ids.first(IN_POLYGON_IDS.size)
    bottom_ids = ids.last(OUT_POLYGON_IDS.size)

    IN_POLYGON_IDS.each { |id| assert_includes top_ids, id, "Expected #{id} in top group (in-polygon)" }
    OUT_POLYGON_IDS.each { |id| assert_includes bottom_ids, id, "Expected #{id} in bottom group (out-polygon)" }
  end

  # ---------------------------------------------------------------------------
  # Combined: order_eval + order_geo (viewport boost pattern)
  # ---------------------------------------------------------------------------

  def test_combined_viewport_boost_and_distance_sort
    polygon_expr = polygon_filter_expression(:location, POLYGON)

    results = Venue.all
                   .order_eval(polygon_expr, direction: :desc)
                   .order_geo(:location, from: { lat: CITY_CENTER[:lat], lng: CITY_CENTER[:lng] })
                   .to_a

    ids = results.map { |r| r.instance_variable_get(:@id) }
    assert_equal 5, ids.size

    top_ids = ids.first(IN_POLYGON_IDS.size)
    bottom_ids = ids.last(OUT_POLYGON_IDS.size)

    IN_POLYGON_IDS.each { |id| assert_includes top_ids, id }
    OUT_POLYGON_IDS.each { |id| assert_includes bottom_ids, id }

    assert_equal '4', bottom_ids.first, 'Trakai (closer) should come before Kaunas among out-of-polygon'
  end

  # ---------------------------------------------------------------------------
  # geo_distance_meters — present when order_geo is used
  # ---------------------------------------------------------------------------

  def test_geo_distance_meters_present_with_order_geo
    result = Venue.all
                  .order_geo(:location, from: { lat: CITY_CENTER[:lat], lng: CITY_CENTER[:lng] })
                  .execute

    result.hits.each do |hit|
      assert_respond_to hit, :geo_distance_meters,
                        "Expected geo_distance_meters on hit #{hit.instance_variable_get(:@id)}"
      geo = hit.geo_distance_meters
      assert geo.is_a?(Hash), 'geo_distance_meters should be a Hash'
      assert geo.key?('location') || geo.key?(:location), 'geo_distance_meters should have a location key'
    end

    distances = result.hits.map { |h| h.geo_distance_meters['location'] || h.geo_distance_meters[:location] }
    assert_equal distances, distances.sort, 'Distances should be in ascending order'
  end

  # ---------------------------------------------------------------------------
  # geo_distance_meters — absent when no geo sort
  # ---------------------------------------------------------------------------

  def test_geo_distance_meters_absent_without_geo_sort
    results = Venue.all.where_geo(:location, within_radius: RADIUS_5KM).to_a

    results.each do |hit|
      refute_respond_to hit, :geo_distance_meters,
                        'geo_distance_meters should not be present without geo sort'
    end
  end

  # ---------------------------------------------------------------------------
  # Chaining: where + where_geo
  # ---------------------------------------------------------------------------

  def test_where_and_where_geo_chain
    results = Venue.all
                   .where('rating:>4.5')
                   .where_geo(:location, within_radius: RADIUS_5KM)
                   .to_a

    ids = results.map { |r| r.instance_variable_get(:@id) }
    assert_includes ids, '1', 'Cathedral Square (4.8, in range) should match'
    assert_includes ids, '2', 'Gediminas Tower (4.6, in range) should match'
    refute_includes ids, '3', 'Town Hall (4.3, below threshold) should not match'
    refute_includes ids, '4', 'Trakai Castle (out of radius) should not match'
  end

  private

  def polygon_filter_expression(field, points)
    coords = points.flatten.join(',')
    "#{field}:(#{coords})"
  end
end
