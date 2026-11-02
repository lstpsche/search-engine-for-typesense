# frozen_string_literal: true

require 'test_helper'
require 'typesense'

class ClientCacheOptionsTest < Minitest::Test
  class RecordingApi
    attr_reader :requests

    def initialize
      @requests = []
    end

    def get(path, params)
      @requests << [path, params]
      { 'found' => 0, 'hits' => [] }
    end

    def post(path, body, params)
      @requests << [path, body, params]
      { 'results' => [] }
    end
  end

  def setup
    @api = RecordingApi.new
    collection = Struct.new(:documents).new(Typesense::Documents.new('products', @api))
    sdk = Struct.new(:collections, :multi_search).new(
      { 'products' => collection }, Typesense::MultiSearch.new(@api)
    )
    @client = SearchEngine::Client.new(typesense_client: sdk)
  end

  def test_single_search_passes_cache_options_as_sdk_search_parameters
    params = { q: '*', query_by: 'name' }
    @client.search(collection: 'products', params: params, url_opts: { use_cache: false, cache_ttl: 7 })
    path, outgoing = @api.requests.fetch(0)
    assert_equal '/collections/products/documents/search', path
    assert_equal({ q: '*', query_by: 'name', use_cache: false, cache_ttl: 7 }, outgoing)
    assert_equal({ q: '*', query_by: 'name' }, params)
  end

  def test_multi_search_passes_request_options_separately_from_search_bodies
    searches = [{ collection: 'products', q: '*' }]
    @client.multi_search(searches: searches, url_opts: { use_cache: true, cache_ttl: 7 })
    path, body, outgoing = @api.requests.fetch(0)
    assert_equal '/multi_search', path
    assert_equal({ searches: searches }, body)
    assert_equal({ use_cache: true, cache_ttl: 7 }, outgoing)
  end

  def test_search_events_redact_vectors_without_changing_the_sdk_request
    vector_query = 'embedding:([1.0e-05,-2.0e-08], k:10)'
    events = SearchEngine::Test.capture_events do
      @client.search(collection: 'products', params: { q: '*', vector_query: vector_query })
    end
    payload = events.find { |event| event[:name] == 'search_engine.search' }.fetch(:payload)
    assert_equal 'embedding:([<2 dims>], k:10)', payload.fetch(:params_preview).fetch(:vector_query)
    assert_equal vector_query, @api.requests.fetch(0).last.fetch(:vector_query)
  end
end
