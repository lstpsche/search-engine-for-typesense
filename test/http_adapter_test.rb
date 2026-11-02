# frozen_string_literal: true

require 'test_helper'
require 'search_engine/client/http_adapter'
require 'search_engine/client/request_builder'
require 'typesense'
require 'minitest/mock'

class HttpAdapterTest < Minitest::Test
  def test_get_search_forwards_parameters_and_cache_controls_to_the_sdk
    params = { q: '*', query_by: 'name', use_cache: false, cache_ttl: 7 }
    api = Minitest::Mock.new
    api.expect(:get, { 'found' => 0, 'hits' => [] }, ['/collections/products/documents/search', params])
    collection = Struct.new(:documents).new(Typesense::Documents.new('products', api))
    sdk = Struct.new(:collections).new({ 'products' => collection })
    request = SearchEngine::Client::RequestBuilder.build_search(
      collection: 'products', compiled_params: SearchEngine::CompiledParams.from(q: '*', query_by: 'name'),
      url_opts: { use_cache: false, cache_ttl: 7 }
    )
    response = SearchEngine::Client::HttpAdapter.new(sdk).perform(request)
    assert_equal 200, response.status
    assert_equal 0, response.body.fetch('found')
    api.verify
  end

  def test_unsupported_path_raises_argument_error_with_method_and_path
    adapter = SearchEngine::Client::HttpAdapter.new(Object.new)
    request = SearchEngine::Client::RequestBuilder::Request.new(
      http_method: :get,
      path: '/foo',
      params: {},
      headers: {},
      body: nil,
      body_json: nil
    )

    error = assert_raises(ArgumentError) do
      adapter.perform(request)
    end

    assert_equal 'Unsupported request path for adapter: get /foo', error.message
  end
end
