# frozen_string_literal: true

require 'test_helper'

class ConfigDefaultsTest < Minitest::Test
  def test_defaults
    cfg = SearchEngine::Config.new

    h = cfg.to_h

    # Typesense transport defaults
    assert_equal 'localhost', h[:host]
    assert_equal 8108, h[:port]
    assert_equal 'http', h[:protocol]
    assert_equal 3_600_000, h[:timeout_ms]
    assert_equal 1_000, h[:open_timeout_ms]
    assert_equal({ attempts: 2, backoff: (10.0..60.0) }, h[:retries])

    # Core defaults
    assert_nil h[:default_query_by]
    assert_equal 'fallback', h[:default_infix]
    assert_equal true, h[:use_cache]
    assert_equal 60, h[:cache_ttl_s]
    assert_equal true, h[:strict_fields]
    assert_equal 50, h[:multi_search_limit]

    # Selection defaults
    assert_equal false, h.dig(:selection, :strict_missing)

    # Presets defaults
    assert_equal true, h.dig(:presets, :enabled)
    assert_nil h.dig(:presets, :namespace)
    assert_equal %i[filter_by sort_by include_fields exclude_fields], h.dig(:presets, :locked_domains)

    # Async partition indexing defaults
    assert_equal :inline, h.dig(:indexer, :partition_execution)
    assert_nil h.dig(:indexer, :partition_queue_name)
    assert_equal 2, h.dig(:indexer, :partition_poll_interval_s)
    assert_nil h.dig(:indexer, :partition_timeout_s)
    assert_equal 86_400, h.dig(:indexer, :partition_run_ttl_s)
    assert_nil h.dig(:indexer, :partition_run_store)

    # Syncable callback timing
    assert_equal :after_commit, h[:syncable_callback_timing]
  end

  def test_postgres_outbox_config_defaults
    h = SearchEngine::Config.new.to_h.fetch(:postgres_outbox)

    assert_equal false, h[:enabled]
    assert_equal 'search_engine_outbox_events', h[:table_name]
    assert_equal 'search_engine_outbox', h[:channel]
    assert_equal 'search_engine', h[:queue_name]
    assert_equal 1000, h[:batch_size]
    assert_equal 10, h[:max_attempts]
    assert_equal 5, h[:poll_interval_s]
    assert_equal 30, h[:listener_wait_timeout_s]
    assert_equal 900, h[:processing_timeout_s]
    assert_equal 604_800, h[:retention_s]
    assert_equal false, h[:advisory_lock]
    assert_nil h[:advisory_lock_key]
    assert_equal false, h[:listener_enabled].call
    assert_equal({}, h[:collection_processors])
    assert_operator h[:retry_backoff].call(1), :>, 0
  end

  def test_postgres_outbox_config_mutation
    cfg = SearchEngine::Config.new
    processor = ->(event) { event }

    cfg.postgres_outbox.enabled = true
    cfg.postgres_outbox.queue_name = 'critical_search'
    cfg.postgres_outbox.collection_processors[:products] = processor
    cfg.postgres_outbox.listener_enabled = -> { true }

    h = cfg.to_h.fetch(:postgres_outbox)

    assert_equal true, h[:enabled]
    assert_equal 'critical_search', h[:queue_name]
    assert_same processor, h.dig(:collection_processors, :products)
    assert_equal true, h[:listener_enabled].call
  end

  def test_configure_yields_and_returns_config
    original = SearchEngine.config

    returned = SearchEngine.configure do |c|
      # Do not change behavior; set to same value to avoid side effects
      c.default_infix = 'fallback'
    end

    assert_instance_of SearchEngine::Config, returned
    assert_same original, returned
    assert_equal 'fallback', SearchEngine.config.to_h[:default_infix]
  end
end
