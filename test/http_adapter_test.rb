# frozen_string_literal: true

require 'test_helper'
require 'search_engine/client/http_adapter'
require 'search_engine/client/request_builder'

class HttpAdapterTest < Minitest::Test
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
