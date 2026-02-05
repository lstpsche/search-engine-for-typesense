# frozen_string_literal: true

require 'test_helper'

class ClientRequestInstrumentationTest < Minitest::Test
  class TimeoutCollection
    def retrieve
      raise ::Timeout::Error, 'boom'
    end
  end

  class TimeoutCollections
    def [](_name)
      TimeoutCollection.new
    end
  end

  class TimeoutTypesense
    def collections
      TimeoutCollections.new
    end
  end

  class Api404Error < StandardError
    def http_code
      404
    end

    def body
      { 'message' => 'missing' }
    end
  end

  class MissingAlias
    def retrieve
      raise Api404Error, 'not found'
    end
  end

  class MissingAliases
    def [](_name)
      MissingAlias.new
    end
  end

  class MissingAliasTypesense
    def aliases
      MissingAliases.new
    end
  end

  class FlakyMultiSearch
    def initialize
      @calls = 0
    end

    def perform(_payload, **_kwargs)
      @calls += 1
      raise ::Timeout::Error, 'transient timeout' if @calls == 1

      { 'results' => [] }
    end
  end

  class FlakyMultiSearchTypesense
    def initialize
      @multi = FlakyMultiSearch.new
    end

    def multi_search
      @multi
    end
  end

  def test_timeout_failure_emits_single_request_event_with_error_class
    client = SearchEngine::Client.new(config: SearchEngine.config, typesense_client: TimeoutTypesense.new)

    events = request_events do
      assert_raises(SearchEngine::Errors::Timeout) do
        client.retrieve_collection_schema('products')
      end
    end

    assert_equal 1, events.size
    payload = events.first[:payload]
    assert_equal :get, payload[:method]
    assert_equal '/collections/products', payload[:path]
    assert_equal SearchEngine::Errors::Timeout.name, payload[:error_class]
  end

  def test_rescued_404_emits_single_request_event_with_error_class
    client = SearchEngine::Client.new(config: SearchEngine.config, typesense_client: MissingAliasTypesense.new)

    events = request_events do
      assert_nil client.resolve_alias('missing')
    end

    assert_equal 1, events.size
    payload = events.first[:payload]
    assert_equal :get, payload[:method]
    assert_equal '/aliases/missing', payload[:path]
    assert_equal SearchEngine::Errors::Api.name, payload[:error_class]
  end

  def test_success_after_multi_search_error_still_emits_success_request_event
    client = SearchEngine::Client.new(config: SearchEngine.config, typesense_client: FlakyMultiSearchTypesense.new)
    searches = [{ collection: 'products' }]

    events = request_events do
      assert_raises(SearchEngine::Errors::Timeout) do
        client.multi_search(searches: searches)
      end

      response = client.multi_search(searches: searches)
      assert_equal [], response[:results]
    end

    multi_search_events = events.select { |event| event[:payload][:path] == '/multi_search' }
    assert_equal 2, multi_search_events.size
    assert_equal SearchEngine::Errors::Timeout.name, multi_search_events[0][:payload][:error_class]
    assert_nil multi_search_events[1][:payload][:error_class]
  end

  private

  def request_events(&block)
    events = SearchEngine::Test.capture_events(&block)
    events.select { |event| event[:name] == 'search_engine.request' }
  end
end
