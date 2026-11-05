# frozen_string_literal: true

require 'test_helper'
require 'search_engine/multi'
require 'search_engine/client'

class MultiSearchMemoizationTest < Minitest::Test
  class Product < SearchEngine::Base
    collection 'products_memo'
    identify_by :id
    attribute :name, :string
  end

  class Brand < SearchEngine::Base
    collection 'brands_memo'
    identify_by :id
    attribute :name, :string
  end

  def build_relation(klass)
    klass.all.select(:id, :name).page(1).per(2)
  end

  def raw_item(found:, hits: [], collection: nil)
    item = { 'found' => found, 'out_of' => found, 'hits' => hits }
    item['collection'] = collection if collection
    item
  end

  def test_single_http_call_and_helpers_pure
    client = Minitest::Mock.new
    raw = {
      'results' => [
        raw_item(found: 2, hits: [], collection: 'products_memo'),
        raw_item(found: 1, hits: [], collection: 'brands_memo')
      ]
    }

    client.expect(:multi_search, raw) do |searches:, url_opts:|
      assert_equal 2, searches.size
      assert url_opts.key?(:use_cache)
      true
    end

    mr = nil
    SearchEngine.config.stub(:client, client) do
      mr = SearchEngine.multi_search_result(common: {}) do |m|
        m.add :products, build_relation(Product)
        m.add :brands,   build_relation(Brand)
      end
    end

    # Access multiple times; should not trigger additional HTTP calls
    assert_equal 2, mr[:products].found
    assert_equal 1, mr[:brands].found
    assert_equal %i[products brands], mr.labels

    # Pure helpers operate in-memory
    h = mr.to_h
    assert_equal %i[products brands], h.keys

    enum = mr.each_label
    assert_kind_of Enumerator, enum
    pairs = enum.map { |(l, r)| [l, r.class.name] }
    expected = [%i[products SearchEngine::Result], %i[brands SearchEngine::Result]]
    mapped_pairs = pairs.map { |l, n| [l, n.to_sym] }
    assert_equal expected, mapped_pairs

    mapped = mr.map_labels { |label, result| [label, result.found] }
    assert_equal [[:products, 2], [:brands, 1]], mapped

    # Verify the client was called exactly once
    client.verify
  end

  def test_map_labels_returns_enumerator_without_block
    mr = SearchEngine::MultiResult.new(
      labels: %i[products brands],
      raw_results: [raw_item(found: 0), raw_item(found: 0)],
      klasses: [Product, Brand]
    )

    e = mr.map_labels
    assert_kind_of Enumerator, e
    enumerated = e.map { |(l, r)| [l, r.found] }
    assert_equal [[:products, 0], [:brands, 0]], enumerated
  end

  def test_hydration_occurs_once_per_result
    labels = %i[products brands]
    raws = [raw_item(found: 1), raw_item(found: 1)]

    calls = 0
    original = SearchEngine::Result.method(:new)

    factory = lambda do |raw, klass:|
      calls += 1
      original.call(raw, klass: klass)
    end

    SearchEngine::Result.stub(:new, factory) do
      mr = SearchEngine::MultiResult.new(labels: labels, raw_results: raws, klasses: [Product, Brand])
      # Access multiple times should not rehydrate
      mr[:products]
      mr[:products].to_a
      mr[:brands]
      mr.to_h
      mr.each_label.to_a
      assert_equal 2, calls
    end
  end

  def test_order_stability_across_helpers
    labels = %i[products brands]
    raws = [raw_item(found: 0), raw_item(found: 0)]
    mr = SearchEngine::MultiResult.new(labels: labels, raw_results: raws, klasses: [Product, Brand])

    assert_equal labels, mr.labels
    assert_equal labels, mr.to_h.keys
    assert_equal labels, mr.each_label.map(&:first)
  end
end
