# frozen_string_literal: true

require 'test_helper'

class BaseCreationTest < Minitest::Test
  class Product < SearchEngine::Base
    collection 'base_creation_products'
    identify_by :id
    attribute :name, :string
  end

  class ImportClient
    attr_reader :jsonl

    def initialize(response: "{\"success\":true}\n")
      @response = response
    end

    def import_documents(collection:, jsonl:, action:)
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
end
