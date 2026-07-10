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

  class BatchSourceProduct
    class << self
      attr_accessor :records

      def find_by(id:)
        records.find { |record| record.id.to_s == id.to_s }
      end
    end
  end

  class SelectiveSearchModel
    class << self
      attr_accessor :failing_id, :upserted_ids

      def upsert(record:)
        raise StandardError, "mapping failed for record #{record.id}" if record.id == failing_id

        upserted_ids << record.id
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
    BatchSourceProduct.records = []
    SelectiveSearchModel.failing_id = nil
    SelectiveSearchModel.upserted_ids = []
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
      assert_kind_of SearchEngine::PostgresOutbox::EventProcessor::EventProcessingError, result.error
      assert_kind_of NameError, result.error.original_error
    end
  end

  def test_one_bad_event_does_not_fail_its_99_good_siblings
    BatchSourceProduct.records = (1..100).map { |id| Product.new(id: id) }
    SelectiveSearchModel.failing_id = 50
    events = (1..100).map do |id|
      event(
        id: id,
        source_model_name: "#{self.class.name}::BatchSourceProduct",
        record_id: id.to_s,
        document_id: "sku-#{id}",
        payload: { 'private_data' => 'must-not-appear-in-errors' }
      )
    end

    SearchEngine::CollectionResolver.stub(:model_for_logical, SelectiveSearchModel) do
      result = SearchEngine::PostgresOutbox::EventProcessor.call(events: events)

      assert_equal((1..100).to_a - [50], result.processed_event_ids)
      assert_equal [50], result.failed_event_ids
      assert_same result, result.validate_for!((1..100).to_a)
      assert_equal((1..100).to_a - [50], SelectiveSearchModel.upserted_ids)

      error = result.error_for(50)
      assert_same error, result.error
      assert_kind_of SearchEngine::PostgresOutbox::EventProcessor::EventProcessingError, error
      assert_equal 50, error.event_id
      assert_equal 'products', error.collection
      assert_equal :upsert, error.operation
      assert_equal 'sku-50', error.document_id
      assert_kind_of StandardError, error.original_error
      assert_includes error.message, 'collection=products event_id=50 operation=upsert document_id=sku-50'
      assert_includes error.message, 'mapping failed for record 50'
      refute_includes error.message, 'must-not-appear-in-errors'
      assert_operator error.message.length, :<, 1_000
    end
  end

  def test_failure_context_is_bounded_and_valid_utf8_for_malformed_error_messages
    malformed_message = "\xFF".b + ('x' * 2_000)
    original_error = StandardError.new(malformed_message)

    error = SearchEngine::PostgresOutbox::EventProcessor::EventProcessingError.new(event, original_error)

    assert_predicate error.message, :valid_encoding?
    assert_operator error.message.length, :<, 1_000
    assert_same original_error, error.original_error
  end

  private

  def event(
    id: 1,
    operation: 'upsert',
    source_model_name: "#{self.class.name}::SourceProduct",
    record_id: '1',
    document_id: '1',
    payload: {}
  )
    SearchEngine::PostgresOutbox::Event.new(
      id: id,
      source_table: 'products',
      source_model_name: source_model_name,
      collection: 'products',
      record_id: record_id,
      document_id: document_id,
      operation: operation,
      attempts: 0,
      payload: payload
    )
  end
end
