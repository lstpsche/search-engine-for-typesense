# frozen_string_literal: true

require 'test_helper'
require_relative 'support/typesense_integration'

class CacheIntegrationTest < Minitest::Test
  include TypesenseIntegration

  class Document < SearchEngine::Base
    identify_by :id
    attribute :name, :string
    attribute :category, :string, facet: true
    query_by :name
  end

  def setup
    setup_typesense
    @collection = fixture_collection([{ name: 'name', type: 'string' },
                                      { name: 'category', type: 'string', facet: true }]
                                    )
    Document.collection(@collection)
    import_fixture(@collection, [{ id: 'a', name: 'alpha', category: 'red' }])
  end

  def teardown
    teardown_typesense
  end

  def test_writes_remain_cached_until_clear_and_uncached_materializers_see_updates
    assert_equal 1, Document.cache(true).count
    import_fixture(@collection, [{ id: 'b', name: 'beta', category: 'blue' }])
    assert_equal 1, Document.cache(true).count
    assert_equal 2, Document.cache(false).count
    SearchEngine::Cache.clear(client: @client)
    assert_equal 2, Document.cache(true).count
  end

  def test_alias_swap_requires_explicit_cache_invalidation
    name = "#{@collection}_alias"
    target = fixture_collection([{ name: 'name', type: 'string' }])
    import_fixture(target, [{ id: 'new', name: 'new target' }, { id: 'next', name: 'next' }])
    request_json(:put, "/aliases/#{name}", { collection_name: @collection })
    @owned_aliases << name
    assert_equal 1, search(name, use_cache: true).fetch('found')
    request_json(:put, "/aliases/#{name}", { collection_name: target })
    assert_equal 1, search(name, use_cache: true).fetch('found')
    assert_equal 2, search(name, use_cache: false).fetch('found')
    SearchEngine::Cache.clear(client: @client)
    assert_equal 2, search(name, use_cache: true).fetch('found')
  end

  def test_direct_key_url_ttl_does_not_override_the_native_default
    assert_equal 1, search(@collection, use_cache: true, cache_ttl: 1).fetch('found')
    import_fixture(@collection, [{ id: 'b', name: 'beta', category: 'blue' }])
    sleep 1.2
    assert_equal 1, search(@collection, use_cache: true, cache_ttl: 1).fetch('found')
    assert_equal 2, search(@collection, use_cache: false).fetch('found')
  end

  def test_scoped_key_ttl_overrides_url
    parent = fixture_key(@collection)
    red = scoped_fixture_key(parent, filter_by: 'category:=red', cache_ttl: 1)
    assert_equal ['a'], response_ids(search(@collection, key: red, use_cache: true, cache_ttl: 30))
    import_fixture(@collection, [{ id: 'c', name: 'gamma', category: 'red' }])
    eventually { search(@collection, key: red, use_cache: true, cache_ttl: 30).fetch('found') == 2 }
  end

  # These safety assertions intentionally fail on servers that omit scoped filters from cache identity.
  def test_single_search_cache_keeps_scoped_filters_isolated
    red, blue = scoped_filter_keys
    assert_equal ['a'], response_ids(search(@collection, key: red, use_cache: false))
    assert_equal ['b'], response_ids(search(@collection, key: blue, use_cache: false))
    assert_equal ['a'], response_ids(search(@collection, key: red, use_cache: true))
    assert_equal ['b'], response_ids(search(@collection, key: blue, use_cache: true)),
                 'Cached searches must preserve scoped authorization; use cache(false) on affected servers'
  end

  def test_multi_search_cache_keeps_scoped_filters_isolated
    red, blue = scoped_filter_keys
    assert_equal ['a'], response_ids(scoped_multi_search(red, use_cache: false))
    assert_equal ['b'], response_ids(scoped_multi_search(blue, use_cache: false))
    assert_equal ['a'], response_ids(scoped_multi_search(red, use_cache: true))
    assert_equal ['b'], response_ids(scoped_multi_search(blue, use_cache: true)),
                 'Cached multi-search must preserve scoped authorization; disable caching on affected servers'
  end

  def test_cached_requests_still_authenticate_after_key_revocation
    parent = fixture_key(@collection)
    key = parent.fetch('value')
    assert_equal 1, search(@collection, key: key, use_cache: true).fetch('found')
    request_json(:delete, "/keys/#{parent.fetch('id')}")
    @owned_keys.delete(parent.fetch('id'))
    request_body(:get, "/collections/#{@collection}/documents/search",
                 params: { q: '*', query_by: 'name', use_cache: true }, key: key, expected: 401
    )
  end

  def test_single_and_multisearch_forward_cache_controls_at_request_level
    result = @client.search(collection: @collection, params: { q: '*', query_by: 'name' },
                            url_opts: { use_cache: true, cache_ttl: 1 }
    )
    assert_equal 1, result.found
    raw = @client.multi_search(searches: [{ collection: @collection, q: '*', query_by: 'name' }],
                               url_opts: { use_cache: false }
                              )
    assert_equal 1, raw.fetch(:results).first.fetch(:found)
    cached = lambda {
      @client.multi_search(searches: [{ collection: @collection, q: '*', query_by: 'name' }],
                           url_opts: { use_cache: true }
                          ).fetch(:results).first.fetch(:found)
    }
    assert_equal 1, cached.call
    import_fixture(@collection, [{ id: 'b', name: 'beta', category: 'blue' }])
    assert_equal 1, cached.call
    fresh = @client.multi_search(searches: [{ collection: @collection, q: '*', query_by: 'name' }],
                                 url_opts: { use_cache: false }
                                )
    assert_equal 2, fresh.fetch(:results).first.fetch(:found)
  end

  private

  def scoped_filter_keys
    import_fixture(@collection, [{ id: 'b', name: 'beta', category: 'blue' }])
    parent = fixture_key(@collection)
    %w[red blue].map { |color| scoped_fixture_key(parent, filter_by: "category:=#{color}", cache_ttl: 30) }
  end

  def scoped_multi_search(key, use_cache:)
    request_json(:post, '/multi_search',
                 { searches: [{ collection: @collection, q: '*', query_by: 'name' }] },
                 params: { use_cache: use_cache }, key: key
    ).fetch('results').first
  end

  def response_ids(response)
    response.fetch('hits').map { |hit| hit.fetch('document').fetch('id') }
  end

  def search(collection, key: @integration_config.api_key, **cache)
    request_json(:get, "/collections/#{collection}/documents/search",
                 params: { q: '*', query_by: 'name' }.merge(cache), key: key
    )
  end
end
