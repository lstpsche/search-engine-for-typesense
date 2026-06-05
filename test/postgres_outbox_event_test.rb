# frozen_string_literal: true

require 'test_helper'

class PostgresOutboxEventTest < Minitest::Test
  def test_normalizes_string_and_symbol_keys
    event = SearchEngine::PostgresOutbox::Event.new(
      'id' => 7,
      source_table: :products,
      'source_model_name' => 'Product',
      collection: :products,
      record_id: 42,
      document_id: 'sku-42',
      operation: 'UPSERT',
      attempts: 2,
      payload: { 'trigger_operation' => 'UPDATE' },
      created_at: 'now'
    )

    assert_equal 7, event.id
    assert_equal 'products', event.source_table
    assert_equal 'Product', event.source_model_name
    assert_equal 'products', event.collection
    assert_equal '42', event.record_id
    assert_equal 'sku-42', event.document_id
    assert_equal :upsert, event.operation
    assert_equal 2, event.attempts
    assert_equal %w[products sku-42], event.coalesce_key
  end

  def test_rejects_invalid_operation
    assert_raises(ArgumentError) do
      SearchEngine::PostgresOutbox::Event.new(operation: 'truncate')
    end
  end
end
