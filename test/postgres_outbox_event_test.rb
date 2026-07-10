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
      created_at: 'now',
      delivery_id: 99,
      target_key: :target_1,
      delivery_lease_owner: 'worker-1:lease-1'
    )

    assert_equal 7, event.id
    assert_equal 'products', event.source_table
    assert_equal 'Product', event.source_model_name
    assert_equal 'products', event.collection
    assert_equal '42', event.record_id
    assert_equal 'sku-42', event.document_id
    assert_equal :upsert, event.operation
    assert_equal 2, event.attempts
    assert_equal 99, event.delivery_id
    assert_equal 'target_1', event.target_key
    assert_equal 'worker-1:lease-1', event.delivery_lease_owner
    assert_equal %w[products sku-42], event.coalesce_key
  end

  def test_delivery_metadata_is_optional
    event = SearchEngine::PostgresOutbox::Event.new(operation: 'upsert')

    assert_nil event.delivery_id
    assert_nil event.target_key
    assert_nil event.delivery_lease_owner
  end

  def test_delivery_attempts_override_parent_event_attempts
    event = SearchEngine::PostgresOutbox::Event.new(
      operation: 'upsert',
      attempts: 1,
      delivery_attempts: 3
    )

    assert_equal 3, event.attempts
  end

  def test_rejects_invalid_operation
    assert_raises(ArgumentError) do
      SearchEngine::PostgresOutbox::Event.new(operation: 'truncate')
    end
  end
end
