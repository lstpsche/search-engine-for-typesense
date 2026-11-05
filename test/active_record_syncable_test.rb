# frozen_string_literal: true

require 'test_helper'

class ActiveRecordSyncableTest < Minitest::Test
  # Mock AR class that records callback registrations without a database.
  def build_mock_ar_class
    Class.new do
      include SearchEngine::ActiveRecordSyncable

      @registered_callbacks = []

      class << self
        attr_reader :registered_callbacks

        %i[after_commit after_create after_update after_destroy].each do |callback|
          define_method(callback) do |filter, **options|
            @registered_callbacks << [callback, filter, options[:on]]
          end
        end
      end
    end
  end

  def test_macro_uses_callbacks_strategy_by_default
    model = build_mock_ar_class
    model.search_engine_syncable(collection: :products)

    config = model._se_syncable_config
    assert_equal :callbacks, config[:sync_strategy]
    assert_equal 'products', config[:logical]
    assert_equal %i[create update destroy], config[:actions]
    assert_equal [
      %i[after_commit __se_syncable_create! create],
      %i[after_commit __se_syncable_upsert! update],
      %i[after_commit __se_syncable_delete! destroy]
    ], model.registered_callbacks
  end

  def test_postgres_outbox_strategy_stores_config_without_callbacks
    model = build_mock_ar_class
    model.search_engine_syncable(collection: :products, on: %i[create destroy], sync_strategy: 'postgres_outbox')

    config = model._se_syncable_config
    assert_equal :postgres_outbox, config[:sync_strategy]
    assert_equal 'products', config[:logical]
    assert_equal %i[create destroy], config[:actions]
    assert_empty model.registered_callbacks
  end

  def test_invalid_sync_strategy_raises_clear_error
    model = build_mock_ar_class

    error = assert_raises(ArgumentError) do
      model.search_engine_syncable(collection: :products, sync_strategy: :sidekiq)
    end

    assert_includes error.message, 'search_engine_syncable: sync_strategy must be within'
    assert_includes error.message, ':callbacks'
    assert_includes error.message, ':postgres_outbox'
  end

  def test_registers_after_save_callbacks_with_legacy_config
    model = build_mock_ar_class

    SearchEngine.config.stub(:syncable_callback_timing, :after_save) do
      model.search_engine_syncable(collection: :products)
    end

    assert_equal [
      [:after_create, :__se_syncable_create!, nil],
      [:after_update, :__se_syncable_upsert!, nil],
      [:after_destroy, :__se_syncable_delete!, nil]
    ], model.registered_callbacks
  end

  def test_repeated_declarations_register_each_requested_action_once
    model = build_mock_ar_class
    2.times { model.search_engine_syncable(collection: :products, on: :create) }

    assert_equal [%i[after_commit __se_syncable_create! create]], model.registered_callbacks
  end

  def test_unmapped_physical_name_preserves_strict_and_optional_registry_lookups
    assert_nil SearchEngine.collection_for('bare-products') { nil }
    error = assert_raises(ArgumentError) { SearchEngine.collection_for('bare-products') }
    assert_includes error.message, "Unregistered collection: 'bare-products'"
  end
end
