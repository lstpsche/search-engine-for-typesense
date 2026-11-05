# frozen_string_literal: true

require 'test_helper'
require 'search_engine/multi'
require 'search_engine/client'

class MultiTest < Minitest::Test
  class Product < SearchEngine::Base
    collection 'products_multi'
    identify_by :id
    attribute :name, :string
  end

  class Brand < SearchEngine::Base
    collection 'brands_multi'
    identify_by :id
    attribute :name, :string
  end

  def build_relation(klass)
    klass.all.select(:id, :name).page(1).per(2)
  end

  def test_add_and_labels_preserve_order
    m = SearchEngine::Multi.new
    m.add(:products, build_relation(Product))
    m.add('brands', build_relation(Brand))
    assert_equal %i[products brands], m.labels
  end

  def test_duplicate_label_raises
    m = SearchEngine::Multi.new
    m.add(:products, build_relation(Product))
    error = assert_raises(ArgumentError) { m.add('Products', build_relation(Product)) }
    assert_match(/duplicate label/i, error.message)
  end

  def test_to_payloads_common_merge_and_shape
    m = SearchEngine::Multi.new
    rel = Product.all.select(:id).per(1)
    m.add(:products, rel)

    payloads = m.to_payloads(common: { q: 'x', query_by: 'name', per_page: 99 })
    assert_equal 1, payloads.size
    p = payloads.first
    assert_equal 'products_multi', p[:collection]
    # per-search wins over common; Relation defaults q: "*"
    assert_equal '*', p[:q]
    # per-search keys win over common: Relation set per_page: 1 should override 99
    assert_equal 1, p[:per_page]
    assert_equal 'name', p[:query_by]
  end

  # Re-introduced: per-search api_key is unsupported and should raise
  def test_api_key_unsupported_raises
    m = SearchEngine::Multi.new
    rel = build_relation(Product)
    error = assert_raises(ArgumentError) { m.add(:products, rel, api_key: 'secret') }
    assert_match(/not supported/i, error.message)
  end

  def test_module_helper_executes_and_maps_results
    # Stub client.multi_search to return two empty results with counts
    client = Minitest::Mock.new
    raw = {
      'results' => [
        { 'found' => 2, 'out_of' => 2, 'hits' => [] },
        { 'found' => 1, 'out_of' => 1, 'hits' => [] }
      ]
    }
    client.expect(:multi_search, raw) do |searches:, url_opts:|
      assert_equal 2, searches.size
      assert_equal true, url_opts.key?(:use_cache)
      true
    end

    # Monkey-patch a temporary client instance into the helper path by stubbing .new
    SearchEngine::Client.stub(:new, client) do
      result_set = SearchEngine.multi_search(common: { query_by: 'name' }) do |m|
        m.add :products, build_relation(Product)
        m.add :brands,   build_relation(Brand)
      end

      assert_equal %i[products brands], result_set.labels
      assert_equal 2, result_set[:products].found
      assert_equal 1, result_set['brands'].found
      map = result_set.to_h
      assert_equal 2, map.size
      assert map.key?(:products)
    end

    client.verify
  end

  def test_to_payloads_filters_url_only_options
    m = SearchEngine::Multi.new
    rel = Product.all.select(:id).per(1)
    # Inject URL-only keys into per-search params by stubbing
    def rel.to_typesense_params
      { q: '*', per_page: 1, use_cache: true, cache_ttl: 42 }
    end
    m.add(:products, rel)

    payloads = m.to_payloads(common: { use_cache: false, cache_ttl: 5, q: 'override' })
    p = payloads.first
    refute_includes p.keys, :use_cache
    refute_includes p.keys, :cache_ttl
    # ensure other keys remain intact and per-search q wins
    assert_equal '*', p[:q]
    assert_equal 1, p[:per_page]
  end

  def test_to_payloads_preserves_entry_order
    m = SearchEngine::Multi.new
    m.add(:products, build_relation(Product))
    m.add(:brands, build_relation(Brand))
    payloads = m.to_payloads(common: {})
    assert_equal(%w[products_multi brands_multi], payloads.map { |h| h[:collection] })
  end

  def test_to_payloads_invalid_common_type_raises
    m = SearchEngine::Multi.new
    m.add(:products, build_relation(Product))
    error = assert_raises(ArgumentError) { m.to_payloads(common: 'oops') }
    assert_match(/common must be a Hash/i, error.message)
  end

  def test_to_payloads_detects_duplicate_labels_during_compile
    m = SearchEngine::Multi.new
    m.add(:products, build_relation(Product))
    m.add(:brands, build_relation(Brand))

    # Corrupt internal state to simulate external mutation causing duplicate labels
    entries = m.instance_variable_get(:@entries)
    entries[1].key = entries[0].key

    error = assert_raises(ArgumentError) { m.to_payloads(common: {}) }
    assert_match(/duplicate label/i, error.message)
  end

  def test_to_payloads_invalid_relation_raises
    m = SearchEngine::Multi.new
    rel = build_relation(Product)
    m.add(:products, rel)

    # Replace relation with an invalid duck to trigger validation during compile
    invalid = Object.new
    entries = m.instance_variable_get(:@entries)
    entries[0].relation = invalid

    error = assert_raises(ArgumentError) { m.to_payloads(common: {}) }
    assert_match(/invalid relation/i, error.message)
  end

  def test_to_payloads_omits_empty_values
    m = SearchEngine::Multi.new
    rel = Product.all
    # Stub params to include empty strings/arrays
    def rel.to_typesense_params
      { q: '*', include_fields: '', filter_by: nil, page: nil, per_page: 10, sort_by: [] }
    end
    m.add(:products, rel)

    payloads = m.to_payloads(common: {})
    p = payloads.first
    refute_includes p.keys, :include_fields
    refute_includes p.keys, :filter_by
    refute_includes p.keys, :page
    refute_includes p.keys, :sort_by
    assert_equal 10, p[:per_page]
  end

  def test_limit_enforced_before_network_call
    original = SearchEngine.config.multi_search_limit
    SearchEngine.configure { |c| c.multi_search_limit = 1 }

    # Stub to ensure client is not called
    client = Minitest::Mock.new
    SearchEngine::Client.stub(:new, client) do
      error = assert_raises(ArgumentError) do
        SearchEngine.multi_search(common: {}) do |m|
          m.add :products, build_relation(Product)
          m.add :brands,   build_relation(Brand)
        end
      end
      assert_match(/exceed limit/, error.message)
    end
  ensure
    SearchEngine.configure { |c| c.multi_search_limit = original }
  end

  def test_helper_builds_url_opts_from_config
    # Ensure cache_ttl gets passed through URL opts
    ttl = 123
    SearchEngine.configure { |c| c.cache_ttl_s = ttl }

    client = Minitest::Mock.new
    raw = { 'results' => [{ 'hits' => [], 'found' => 0 }] }
    client.expect(:multi_search, raw) do |searches:, url_opts:|
      assert_equal ttl, url_opts[:cache_ttl]
      assert_equal true, [true, false].include?(url_opts[:use_cache])
      assert searches.is_a?(Array)
      true
    end

    SearchEngine::Client.stub(:new, client) do
      SearchEngine.multi_search(common: {}) do |m|
        m.add :products, build_relation(Product)
      end
    end

    client.verify
  end

  def test_multi_search_raw_returns_raw
    client = Minitest::Mock.new
    raw = { 'results' => [{ 'found' => 0, 'hits' => [] }] }
    client.expect(:multi_search, raw) do |searches:, url_opts:|
      assert_equal 1, searches.size
      assert url_opts.key?(:use_cache)
      true
    end

    SearchEngine::Client.stub(:new, client) do
      res = SearchEngine.multi_search_raw(common: {}) do |m|
        m.add :products, build_relation(Product)
      end
      assert_equal raw, res
    end

    client.verify
  end

  def test_typed_helpers_apply_selection_and_hit_validators_without_requesting_a_preview
    stub = SearchEngine::Test::StubClient.new
    raw = { results: [{ found: 2, hits: [{ document: { id: '1' } }] }] }
    strict = Product.all.select(:name).options(selection: { strict_missing: true })
    SearchEngine.config.stub(:client, stub) do
      %i[multi_search multi_search_result].each do |helper|
        stub.enqueue_response(:multi_search, raw)
        assert_raises(SearchEngine::Errors::MissingField) do
          SearchEngine.public_send(helper) { |m| m.add(:products, strict) }
        end
        stub.enqueue_response(:multi_search, raw)
        assert_raises(SearchEngine::Errors::HitLimitExceeded) do
          SearchEngine.public_send(helper) { |m| m.add(:products, Product.all.validate_hits!(max: 1)) }
        end
      end
    end
    assert_empty stub.search_calls
    assert_equal 4, stub.multi_search_calls.size
  end

  def test_typed_helpers_reject_incomplete_responses_before_hydration_and_raw_preserves_them
    stub = SearchEngine::Test::StubClient.new
    incomplete = { results: [] }
    queries = ->(m) { m.add(:products, Product.all) }
    SearchEngine.config.stub(:client, stub) do
      %i[multi_search multi_search_result].each do |helper|
        stub.enqueue_response(:multi_search, incomplete)
        error = assert_raises(ArgumentError) { SearchEngine.public_send(helper, &queries) }
        assert_match(/does not match/, error.message)
      end
      stub.enqueue_response(:multi_search, incomplete)
      assert_same incomplete, SearchEngine.multi_search_raw(&queries)
    end
  end

  def test_cache_opt_out_applies_to_all_entries_and_a_scoped_clients_own_defaults
    scoped = SearchEngine::Config.new
    native = SearchEngine::Client.new(config: scoped)
    seen = []
    response = lambda do |searches:, url_opts:|
      seen << url_opts.fetch(:use_cache)
      assert(searches.none? { |body| body.key?(:use_cache) })
      { results: searches.map { { found: 0, hits: [] } } }
    end
    native.stub(:multi_search, response) do
      SearchEngine.config.stub(:client, native) do
        scoped.use_cache = false
        SearchEngine.multi_search { |m| m.add(:products, Product.all) }
        scoped.use_cache = true
        SearchEngine.multi_search_raw { |m| m.add(:first, Product.all).add(:second, Product.all.cache(false)) }
        SearchEngine.multi_search_result { |m| m.add(:cached, Product.all) }
      end
    end
    assert_equal [false, false, true], seen
  end

  def test_bundled_offline_clients_supply_one_empty_result_per_search
    [SearchEngine.offline_client, SearchEngine::Test::StubClient.new].each do |client|
      SearchEngine.config.stub(:client, client) do
        results = SearchEngine.multi_search { |m| m.add(:one, Product.all).add(:two, Brand.all) }
        assert_equal %i[one two], results.labels
        assert_equal [0, 0], results.to_h.values.map(&:found)
      end
    end
  end

  def test_api_error_augmentation_includes_label_when_available
    body = {
      'results' => [
        { 'status' => 200 },
        { 'status' => 422 }
      ]
    }
    error = SearchEngine::Errors::Api.new('typesense api error: 422', status: 422, body: body)

    client = Object.new
    def client.multi_search(*)
      raise @error_to_raise
    end
    client.instance_variable_set(:@error_to_raise, error)

    SearchEngine::Client.stub(:new, client) do
      raised = assert_raises(SearchEngine::Errors::Api) do
        SearchEngine.multi_search_raw(common: {}) do |m|
          m.add :products, build_relation(Product)
          m.add :brands,   build_relation(Brand)
        end
      end
      assert_match(/:brands/, raised.message)
      assert_match(/422/, raised.message)
    end
  end
end
