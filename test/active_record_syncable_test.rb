# frozen_string_literal: true

require 'test_helper'

class ActiveRecordSyncableTest < Minitest::Test
  # Mock AR class that records callback registrations without a database.
  def build_mock_ar_class
    Class.new do
      include SearchEngine::ActiveRecordSyncable

      @__registered_callbacks__ = []

      class << self
        attr_reader :__registered_callbacks__

        %i[
          after_create after_update after_destroy
          after_create_commit after_update_commit after_destroy_commit
        ].each do |cb|
          define_method(cb) do |method_name|
            @__registered_callbacks__ << [cb, method_name]
          end
        end
      end
    end
  end

  def test_registers_after_commit_callbacks_by_default
    mock_klass = build_mock_ar_class
    cfg = { actions: %i[create update destroy] }

    SearchEngine::ActiveRecordSyncable.__register_callbacks_for(mock_klass, cfg)

    callbacks = mock_klass.__registered_callbacks__
    assert_includes callbacks, %i[after_create_commit __se_syncable_upsert!]
    assert_includes callbacks, %i[after_update_commit __se_syncable_upsert!]
    assert_includes callbacks, %i[after_destroy_commit __se_syncable_delete!]

    refute(callbacks.any? { |cb, _| cb == :after_create })
    refute(callbacks.any? { |cb, _| cb == :after_update })
    refute(callbacks.any? { |cb, _| cb == :after_destroy })
  end

  def test_macro_uses_callbacks_strategy_by_default
    mock_klass = build_mock_ar_class

    mock_klass.search_engine_syncable(collection: :products)

    cfg = mock_klass.instance_variable_get(:@__se_syncable_cfg__)
    assert_equal :callbacks, cfg[:sync_strategy]
    assert_equal 'products', cfg[:logical]
    assert_equal %i[create update destroy], cfg[:actions]

    callbacks = mock_klass.__registered_callbacks__
    assert_includes callbacks, %i[after_create_commit __se_syncable_upsert!]
    assert_includes callbacks, %i[after_update_commit __se_syncable_upsert!]
    assert_includes callbacks, %i[after_destroy_commit __se_syncable_delete!]
  end

  def test_postgres_outbox_strategy_stores_config_without_callbacks
    mock_klass = build_mock_ar_class

    mock_klass.search_engine_syncable(collection: :products, on: %i[create destroy], sync_strategy: :postgres_outbox)

    cfg = mock_klass.instance_variable_get(:@__se_syncable_cfg__)
    assert_equal :postgres_outbox, cfg[:sync_strategy]
    assert_equal 'products', cfg[:logical]
    assert_equal %i[create destroy], cfg[:actions]
    assert_empty mock_klass.__registered_callbacks__
    refute mock_klass.instance_variable_defined?(:@__se_syncable_callbacks_installed__)
  end

  def test_string_sync_strategy_normalizes_to_symbol
    mock_klass = build_mock_ar_class

    mock_klass.search_engine_syncable(collection: :products, sync_strategy: 'postgres_outbox')

    cfg = mock_klass.instance_variable_get(:@__se_syncable_cfg__)
    assert_equal :postgres_outbox, cfg[:sync_strategy]
    assert_empty mock_klass.__registered_callbacks__
  end

  def test_invalid_sync_strategy_raises_clear_error
    mock_klass = build_mock_ar_class

    error = assert_raises(ArgumentError) do
      mock_klass.search_engine_syncable(collection: :products, sync_strategy: :sidekiq)
    end

    assert_includes error.message, 'search_engine_syncable: sync_strategy must be within'
    assert_includes error.message, ':callbacks'
    assert_includes error.message, ':postgres_outbox'
  end

  def test_registers_after_save_callbacks_with_legacy_config
    original_timing = SearchEngine.config.syncable_callback_timing
    SearchEngine.config.syncable_callback_timing = :after_save

    mock_klass = build_mock_ar_class
    cfg = { actions: %i[create update destroy] }

    SearchEngine::ActiveRecordSyncable.__register_callbacks_for(mock_klass, cfg)

    callbacks = mock_klass.__registered_callbacks__
    assert_includes callbacks, %i[after_create __se_syncable_upsert!]
    assert_includes callbacks, %i[after_update __se_syncable_upsert!]
    assert_includes callbacks, %i[after_destroy __se_syncable_delete!]

    refute(callbacks.any? { |cb, _| cb == :after_create_commit })
    refute(callbacks.any? { |cb, _| cb == :after_update_commit })
    refute(callbacks.any? { |cb, _| cb == :after_destroy_commit })
  ensure
    SearchEngine.config.syncable_callback_timing = original_timing
  end

  def test_registers_only_requested_actions
    mock_klass = build_mock_ar_class
    cfg = { actions: %i[create] }

    SearchEngine::ActiveRecordSyncable.__register_callbacks_for(mock_klass, cfg)

    callbacks = mock_klass.__registered_callbacks__
    assert_equal 1, callbacks.size
    assert_includes callbacks, %i[after_create_commit __se_syncable_upsert!]
  end

  def test_guard_against_double_install
    mock_klass = build_mock_ar_class
    cfg = { actions: %i[create update destroy] }

    SearchEngine::ActiveRecordSyncable.__register_callbacks_for(mock_klass, cfg)
    first_count = mock_klass.__registered_callbacks__.size

    SearchEngine::ActiveRecordSyncable.__register_callbacks_for(mock_klass, cfg)
    second_count = mock_klass.__registered_callbacks__.size

    assert_equal first_count, second_count, 'Callbacks should not be registered twice'
  end
end
