# frozen_string_literal: true

require 'test_helper'
require 'search_engine/multi'
require 'search_engine/client'

class MultiCurationTest < Minitest::Test
  class Product < SearchEngine::Base
    collection 'products_multi_curation'
    identify_by :id
    attribute :name, :string
  end

  class Brand < SearchEngine::Base
    collection 'brands_multi_curation'
    identify_by :id
    attribute :name, :string
  end

  def build_relation(klass)
    klass.all.select(:id, :name)
  end

  def test_mixed_curation_per_entry_payloads_shape
    rel_products = build_relation(Product).curate(pin: %w[p_1 p_2])
    rel_brands   = build_relation(Brand).curate(hide: %w[b9 b10], filter_curated_hits: true)

    m = SearchEngine::Multi.new
    m.add :products, rel_products
    m.add :brands,   rel_brands

    payloads = m.to_payloads(common: {})

    p_products = payloads[0]
    assert_equal 'products_multi_curation', p_products[:collection]
    assert_equal 'p_1,p_2', p_products[:pinned_hits]
    refute_includes p_products.keys, :hidden_hits
    refute_includes p_products.keys, :filter_curated_hits

    p_brands = payloads[1]
    assert_equal 'brands_multi_curation', p_brands[:collection]
    assert_equal 'b9,b10', p_brands[:hidden_hits]
    assert_equal true, p_brands[:filter_curated_hits]
    refute_includes p_brands.keys, :pinned_hits
  end

  def test_no_url_leakage_of_curation_keys
    rel_products = build_relation(Product).curate(pin: %w[p_1 p_2])
    rel_brands   = build_relation(Brand).curate(hide: %w[b9 b10], filter_curated_hits: true)

    client = Minitest::Mock.new
    raw = { 'results' => [{ 'found' => 0, 'hits' => [] }, { 'found' => 0, 'hits' => [] }] }
    client.expect(:multi_search, raw) do |searches:, url_opts:|
      assert_equal 2, searches.size
      # ensure curation keys live in per-entry body only
      searches.each do |s|
        if s[:collection] == 'products_multi_curation'
          assert_equal 'p_1,p_2', s[:pinned_hits]
        elsif s[:collection] == 'brands_multi_curation'
          assert_equal 'b9,b10', s[:hidden_hits]
          assert_equal true, s[:filter_curated_hits]
        end
        # none of the curation keys should appear in URL opts
        %i[pinned_hits hidden_hits curation_tags filter_curated_hits].each do |k|
          refute_includes url_opts.keys, k
        end
      end
      true
    end

    SearchEngine::Client.stub(:new, client) do
      SearchEngine.multi_search(common: {}) do |m|
        m.add :products, rel_products
        m.add :brands,   rel_brands
      end
    end

    client.verify
  end

  def test_determinism_stable_pins_and_payloads
    rel_products = build_relation(Product).curate(pin: %w[p_1 p_2 p_1]) # duplicate p_1 should be stable-deduped

    m_1 = SearchEngine::Multi.new
    m_1.add :products, rel_products
    p_1 = m_1.to_payloads(common: {})

    m_2 = SearchEngine::Multi.new
    m_2.add :products, rel_products
    p_2 = m_2.to_payloads(common: {})

    assert_equal p_1, p_2
    assert_equal 'p_1,p_2', p_1.first[:pinned_hits]
  end

  def test_hydration_round_trip_with_curation_unchanged
    rel_products = build_relation(Product).curate(pin: %w[p_1 p_2])

    client = Minitest::Mock.new
    raw = { 'results' => [{ 'found' => 0, 'hits' => [] }] }
    client.expect(:multi_search, raw) do |searches:, url_opts:|
      assert_equal 1, searches.size
      assert_equal 'p_1,p_2', searches.first[:pinned_hits]
      assert url_opts.key?(:use_cache)
      true
    end

    SearchEngine::Client.stub(:new, client) do
      res = SearchEngine.multi_search(common: {}) do |m|
        m.add :products, rel_products
      end
      assert_instance_of SearchEngine::Multi::ResultSet, res
      assert res[:products].respond_to?(:found)
    end

    client.verify
  end
end
