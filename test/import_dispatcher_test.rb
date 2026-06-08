# frozen_string_literal: true

require 'test_helper'

class ImportDispatcherTest < Minitest::Test
  FakeClient = Struct.new(:calls, :responses, keyword_init: true) do
    def import_documents(collection:, jsonl:, action: :upsert)
      calls << { collection: collection, body: jsonl, action: action }
      resp = responses.shift
      raise resp if resp.is_a?(Exception)

      resp || "{\"success\":true}\n"
    end
  end

  def test_dry_run_emits_instrumentation_without_network
    client = FakeClient.new(calls: [], responses: [])
    retry_policy = SearchEngine::Indexer::RetryPolicy.from_config(attempts: 1, base: 0.1, max: 0.1,
                                                                  jitter_fraction: 0.0
    )

    events = SearchEngine::Test.capture_events('search_engine.indexer.batch_import') do
      stats = SearchEngine::Indexer::ImportDispatcher.import_batch(
        client: client,
        collection: 'products',
        action: :upsert,
        jsonl: "{\"id\":1}\n",
        docs_count: 1,
        bytes_sent: 10,
        batch_index: 0,
        retry_policy: retry_policy,
        dry_run: true
      )
      assert_equal 1, stats[:success_count]
      assert_equal 0, stats[:failure_count]
    end

    assert_equal 0, client.calls.size
    assert_equal 1, events.size
    p = events.first[:payload]
    assert_equal 'products', p[:into]
    assert_equal 1, p[:docs_count]
    assert_equal 200, p[:http_status]
  end

  def test_retry_on_timeout_then_success
    client = FakeClient.new(calls: [], responses: [])
    client.responses << SearchEngine::Errors::Timeout.new('boom')
    client.responses << "{\"success\":true}\n{\"success\":false,\"error\":\"bad\"}\n"

    policy = SearchEngine::Indexer::RetryPolicy.from_config(attempts: 3, base: 0.0, max: 0.0, jitter_fraction: 0.0)

    stats = SearchEngine::Indexer::ImportDispatcher.import_batch(
      client: client,
      collection: 'products',
      action: :upsert,
      jsonl: "{\"id\":1}\n{\"id\":2}\n",
      docs_count: 2,
      bytes_sent: 24,
      batch_index: 1,
      retry_policy: policy,
      dry_run: false
    )

    assert_equal 2, client.calls.size
    assert_equal 1, stats[:success_count]
    assert_equal 1, stats[:failure_count]
    assert_equal 2, stats[:attempts]
  end

  def test_unknown_import_response_shape_raises
    client = FakeClient.new(calls: [], responses: [{ ok: true }])
    policy = SearchEngine::Indexer::RetryPolicy.from_config(attempts: 1, base: 0.0, max: 0.0, jitter_fraction: 0.0)

    error = assert_raises(SearchEngine::Errors::InvalidParams) do
      SearchEngine::Indexer::ImportDispatcher.import_batch(
        client: client,
        collection: 'products',
        action: :upsert,
        jsonl: "{\"id\":1}\n",
        docs_count: 1,
        bytes_sent: 10,
        batch_index: 0,
        retry_policy: policy,
        dry_run: false
      )
    end

    assert_equal 'Unsupported Typesense import response shape: Hash', error.message
  end
end
