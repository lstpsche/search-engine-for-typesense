# frozen_string_literal: true

require 'test_helper'

class GeoDistanceTest < Minitest::Test
  class GeoVenue < SearchEngine::Base
    collection 'geo_venues_dist'
    identify_by :id
    attribute :name, :string
    attribute :location, :geopoint
  end

  def test_geo_distance_meters_present_on_hydrated_hits
    raw = {
      'found' => 1,
      'out_of' => 1,
      'hits' => [
        {
          'document' => { 'id' => '1', 'name' => 'Venue A', 'location' => [54.69, 25.28] },
          'geo_distance_meters' => { 'location' => 1234 }
        }
      ]
    }

    result = SearchEngine::Result.new(raw, klass: GeoVenue)
    hit = result.hits.first

    assert_respond_to hit, :geo_distance_meters
    assert_equal({ 'location' => 1234 }, hit.geo_distance_meters)
  end

  def test_geo_distance_meters_with_multiple_hits
    raw = {
      'found' => 2,
      'out_of' => 2,
      'hits' => [
        {
          'document' => { 'id' => '1', 'name' => 'Venue A', 'location' => [54.69, 25.28] },
          'geo_distance_meters' => { 'location' => 500 }
        },
        {
          'document' => { 'id' => '2', 'name' => 'Venue B', 'location' => [54.70, 25.30] },
          'geo_distance_meters' => { 'location' => 2000 }
        }
      ]
    }

    result = SearchEngine::Result.new(raw, klass: GeoVenue)

    assert_equal({ 'location' => 500 }, result.hits[0].geo_distance_meters)
    assert_equal({ 'location' => 2000 }, result.hits[1].geo_distance_meters)
  end

  def test_geo_distance_meters_absent_when_not_in_response
    raw = {
      'found' => 1,
      'out_of' => 1,
      'hits' => [
        {
          'document' => { 'id' => '1', 'name' => 'Venue A', 'location' => [54.69, 25.28] }
        }
      ]
    }

    result = SearchEngine::Result.new(raw, klass: GeoVenue)
    hit = result.hits.first

    refute_respond_to hit, :geo_distance_meters
  end

  def test_geo_distance_meters_graceful_with_empty_hash
    raw = {
      'found' => 1,
      'out_of' => 1,
      'hits' => [
        {
          'document' => { 'id' => '1', 'name' => 'Venue A', 'location' => [54.69, 25.28] },
          'geo_distance_meters' => {}
        }
      ]
    }

    result = SearchEngine::Result.new(raw, klass: GeoVenue)
    hit = result.hits.first

    refute_respond_to hit, :geo_distance_meters
  end

  def test_geo_distance_meters_on_grouped_results
    raw = {
      'found' => 2,
      'out_of' => 2,
      'grouped_hits' => [
        {
          'group_key' => ['group_a'],
          'hits' => [
            {
              'document' => { 'id' => '1', 'name' => 'Venue A', 'location' => [54.69, 25.28] },
              'geo_distance_meters' => { 'location' => 750 }
            }
          ]
        },
        {
          'group_key' => ['group_b'],
          'hits' => [
            {
              'document' => { 'id' => '2', 'name' => 'Venue B', 'location' => [54.70, 25.30] },
              'geo_distance_meters' => { 'location' => 3000 }
            }
          ]
        }
      ],
      'request_params' => { 'group_by' => 'category' }
    }

    result = SearchEngine::Result.new(raw, klass: GeoVenue)

    assert result.grouped?
    groups = result.groups
    assert_equal 2, groups.size

    first_hit = groups[0].hits.first
    assert_respond_to first_hit, :geo_distance_meters
    assert_equal({ 'location' => 750 }, first_hit.geo_distance_meters)

    second_hit = groups[1].hits.first
    assert_equal({ 'location' => 3000 }, second_hit.geo_distance_meters)
  end

  def test_geo_distance_meters_without_klass_uses_openstruct
    raw = {
      'found' => 1,
      'out_of' => 1,
      'hits' => [
        {
          'document' => { 'id' => '1', 'name' => 'Venue A' },
          'geo_distance_meters' => { 'location' => 999 }
        }
      ]
    }

    result = SearchEngine::Result.new(raw)
    hit = result.hits.first

    assert_respond_to hit, :geo_distance_meters
    assert_equal({ 'location' => 999 }, hit.geo_distance_meters)
  end
end
