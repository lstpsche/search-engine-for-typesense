# frozen_string_literal: true

require 'test_helper'
require 'timeout'

class ClientErrorMappingTest < Minitest::Test
  class FakeError
    def http_code
      500
    end

    def body
      { 'message' => 'boom' }
    end
  end

  def test_api_error_doc_link_is_stable
    client = SearchEngine::Client.new(typesense_client: Object.new)
    # Force map_and_raise by calling the private method via send
    error = assert_raises(SearchEngine::Errors::Api) do
      client.send(:map_and_raise, FakeError.new, :get, '/health', {}, SearchEngine::Instrumentation.monotonic_ms)
    end
    assert_equal 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/client#errors', error.doc
  end

  def test_timeout_error_doc_link_is_stable
    client = SearchEngine::Client.new(typesense_client: Object.new)
    dummy = ::Timeout::Error.new('timeout')

    error = assert_raises(SearchEngine::Errors::Timeout) do
      client.send(:map_and_raise, dummy, :get, '/health', {}, SearchEngine::Instrumentation.monotonic_ms)
    end
    assert_equal 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/client#errors', error.doc
  end

  def test_connection_error_doc_link_is_stable
    client = SearchEngine::Client.new(typesense_client: Object.new)
    conn_error = SocketError.new('oops')

    error = assert_raises(SearchEngine::Errors::Connection) do
      client.send(:map_and_raise, conn_error, :get, '/health', {}, SearchEngine::Instrumentation.monotonic_ms)
    end
    assert_equal 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/client#errors', error.doc
  end
end
