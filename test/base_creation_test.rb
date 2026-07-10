# frozen_string_literal: true

require 'test_helper'

class BaseCreationTest < Minitest::Test
  class Product < SearchEngine::Base
    collection 'base_creation_products'
    identify_by :id
    attribute :name, :string
  end

  class ImportClient
    attr_reader :jsonl, :calls

    def initialize(response: "{\"success\":true}\n")
      @response = response
      @calls = 0
    end

    def import_documents(collection:, jsonl:, action:)
      @calls += 1
      @jsonl = jsonl
      @collection = collection
      @action = action
      @response
    end
  end

  def test_upsert_data_emits_one_doc_updated_at_key
    client = ImportClient.new

    SearchEngine.stub(:client, client) do
      assert_equal 1, Product.upsert(data: { id: '1', name: 'a' })
    end

    line = client.jsonl.lines.first
    parsed = JSON.parse(line)
    assert_equal 1, line.scan('"doc_updated_at"').size
    assert_kind_of Integer, parsed.fetch('doc_updated_at')
  end

  def test_upsert_data_raises_for_unknown_import_response_shape
    client = ImportClient.new(response: { ok: true })

    error = assert_raises(SearchEngine::Errors::InvalidParams) do
      SearchEngine.stub(:client, client) do
        Product.upsert(data: { id: '1', name: 'a' })
      end
    end

    assert_equal 'Unsupported Typesense import response shape: Hash', error.message
  end

  def test_upsert_bulk_return_mode_returns_ordered_row_failures_without_raising
    response = <<~JSONL
      {"success":true,"status":200,"document":{"secret":"payload"}}
      {"success":false,"code":404,"error":"document was not found","document":{"secret":"payload"}}
    JSONL
    client = ImportClient.new(response: response)

    result = SearchEngine.stub(:client, client) do
      Product.upsert_bulk(
        data: [{ id: '1', name: 'a' }, { id: '2', name: 'b' }],
        on_failure: :return
      )
    end

    assert_equal 1, client.calls
    assert_equal 2, result[:docs_count]
    assert_equal 1, result[:success_count]
    assert_equal 1, result[:failure_count]
    success_values = result[:row_results].map { |row| row[:success] }
    assert_equal [true, false], success_values
    assert_equal 404, result[:row_results].last[:status]
    assert_equal ['document was not found'], result[:errors_sample]
    refute result.key?(:response)
    refute_includes result.to_s, 'payload'
  end

  def test_upsert_bulk_default_mode_raises_safe_error_for_row_failure
    response = <<~JSONL
      {"success":false,"code":422,"error":"schema mismatch","document":{"secret":"payload"}}
    JSONL
    client = ImportClient.new(response: response)

    error = assert_raises(SearchEngine::Errors::InvalidParams) do
      SearchEngine.stub(:client, client) do
        Product.upsert_bulk(data: [{ id: '1', name: 'a' }])
      end
    end

    assert_match(%r{failed for 1/1 document}, error.message)
    assert_equal 1, error.details[:failure_count]
    refute error.details.key?(:response)
    refute_includes error.details.to_s, 'payload'
  end

  def test_upsert_bulk_rejects_invalid_failure_mode_before_mapping_or_io
    client = ImportClient.new

    error = assert_raises(SearchEngine::Errors::InvalidParams) do
      SearchEngine.stub(:client, client) do
        Product.upsert_bulk(data: [{ id: '1', name: 'a' }], on_failure: 'return')
      end
    end

    assert_equal 'on_failure must be :raise or :return', error.message
    assert_equal 0, client.calls
  end

  def test_upsert_bulk_rejects_short_response_in_return_mode
    client = ImportClient.new(response: "{\"success\":true}\n")

    error = assert_raises(SearchEngine::Errors::InvalidParams) do
      SearchEngine.stub(:client, client) do
        Product.upsert_bulk(
          data: [{ id: '1', name: 'a' }, { id: '2', name: 'b' }],
          on_failure: :return
        )
      end
    end

    assert_equal 'Typesense import response row count mismatch: expected 2, got 1', error.message
    assert_equal({ collection: 'base_creation_products', submitted_count: 2, response_count: 1 }, error.details)
  end

  def test_upsert_bulk_rejects_long_response_in_return_mode
    client = ImportClient.new(response: "{\"success\":true}\n{\"success\":true}\n")

    error = assert_raises(SearchEngine::Errors::InvalidParams) do
      SearchEngine.stub(:client, client) do
        Product.upsert_bulk(data: [{ id: '1', name: 'a' }], on_failure: :return)
      end
    end

    assert_equal 'Typesense import response row count mismatch: expected 1, got 2', error.message
  end

  def test_upsert_bulk_empty_input_returns_safe_empty_stats_without_io
    client = ImportClient.new

    result = SearchEngine.stub(:client, client) do
      Product.upsert_bulk(data: [], on_failure: :return)
    end

    assert_equal 0, client.calls
    assert_equal 0, result[:docs_count]
    assert_equal [], result[:row_results]
    assert_predicate result[:row_results], :frozen?
    refute result.key?(:response)
  end
end
