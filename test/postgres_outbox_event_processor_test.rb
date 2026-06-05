# frozen_string_literal: true

require 'test_helper'

class PostgresOutboxEventProcessorTest < Minitest::Test
  Product = Struct.new(:id, keyword_init: true)

  class SourceProduct
    class << self
      attr_accessor :record

      def find_by(id:)
        record if record&.id.to_s == id.to_s
      end
    end
  end

  class FakeSearchModel
    class << self
      attr_accessor :upserted

      def upsert(record:)
        self.upserted = record
        1
      end
    end
  end

  class FakeClient
    attr_reader :deletes

    def initialize(alias_target: nil, delete_response: nil)
      @alias_target = alias_target
      @delete_response = delete_response
      @deletes = []
    end

    def resolve_alias(collection)
      @resolved = collection
      @alias_target
    end

    def delete_document(collection:, id:)
      deletes << [collection, id]
      @delete_response
    end
  end

  def setup
    SourceProduct.record = nil
    FakeSearchModel.upserted = nil
  end

  def test_delete_nil_response_is_success
    client = FakeClient.new(alias_target: 'products_20260605', delete_response: nil)
    event = event(operation: 'delete', document_id: 'sku-1')

    result = SearchEngine::PostgresOutbox::EventProcessor.call(events: [event], context: { client: client })

    assert result.success?
    assert_equal [[1]], [result.processed_event_ids]
    assert_equal [%w[products_20260605 sku-1]], client.deletes
  end

  def test_upsert_reloads_source_and_uses_search_engine_model
    record = Product.new(id: 10)
    SourceProduct.record = record

    SearchEngine::CollectionResolver.stub(:model_for_logical, FakeSearchModel) do
      result = SearchEngine::PostgresOutbox::EventProcessor.call(events: [event(record_id: '10')])

      assert result.success?
      assert_same record, FakeSearchModel.upserted
    end
  end

  def test_missing_source_row_deletes_document
    client = FakeClient.new
    event = event(record_id: 'missing', document_id: 'sku-missing')

    result = SearchEngine::PostgresOutbox::EventProcessor.call(events: [event], context: { client: client })

    assert result.success?
    assert_equal [%w[products sku-missing]], client.deletes
  end

  def test_missing_search_engine_model_is_failure
    SourceProduct.record = Product.new(id: 10)

    SearchEngine::CollectionResolver.stub(:model_for_logical, nil) do
      result = SearchEngine::PostgresOutbox::EventProcessor.call(events: [event(record_id: '10')])

      refute result.success?
      assert_equal [1], result.failed_event_ids
      assert_kind_of NameError, result.error
    end
  end

  private

  def event(operation: 'upsert', record_id: '1', document_id: '1')
    SearchEngine::PostgresOutbox::Event.new(
      id: 1,
      source_table: 'products',
      source_model_name: "#{self.class.name}::SourceProduct",
      collection: 'products',
      record_id: record_id,
      document_id: document_id,
      operation: operation,
      attempts: 0,
      payload: {}
    )
  end
end
