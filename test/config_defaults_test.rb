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
