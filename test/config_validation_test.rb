# frozen_string_literal: true

require 'test_helper'

class ConfigValidationTest < Minitest::Test
  def test_validate_bare_minimum_success
    cfg = SearchEngine::Config.new
    # Defaults are valid
    assert_equal true, cfg.validate!
  end

  def test_validate_protocol_error
    cfg = SearchEngine::Config.new
    cfg.protocol = 'ftp'

    error = assert_raises(ArgumentError) { cfg.validate! }
    assert_equal 'protocol must be "http" or "https"', error.message
  end

  def test_validate_host_error
    cfg = SearchEngine::Config.new
    cfg.host = ''

    error = assert_raises(ArgumentError) { cfg.validate! }
    assert_equal 'host must be present', error.message
  end

  def test_validate_port_error
    cfg = SearchEngine::Config.new
    cfg.port = 0

    error = assert_raises(ArgumentError) { cfg.validate! }
    assert_equal 'port must be a positive Integer', error.message
  end

  def test_validate_multiple_errors_joined
    cfg = SearchEngine::Config.new
    cfg.protocol = 'ftp'
    cfg.host = ''
    cfg.port = -1

    error = assert_raises(ArgumentError) { cfg.validate! }
    # Order matches validate! appends
    assert_equal 'protocol must be "http" or "https", host must be present, port must be a positive Integer',
                 error.message
  end

  def test_validate_rejects_unknown_dispatch_modes
    cfg = SearchEngine::Config.new
    cfg.indexer.dispatch = :sidekiq
    cfg.indexer.partition_execution = :background

    error = assert_raises(ArgumentError) { cfg.validate! }

    assert_equal(
      'indexer.dispatch must be :active_job or :inline, ' \
      'indexer.partition_execution must be :active_job or :inline',
      error.message
    )
  end

  def test_validate_accepts_exact_string_dispatch_modes
    cfg = SearchEngine::Config.new
    cfg.indexer.dispatch = 'active_job'
    cfg.indexer.partition_execution = 'inline'

    assert_equal true, cfg.validate!
  end

  def test_validate_accepts_nil_or_callable_schema_rebuild_guard
    cfg = SearchEngine::Config.new
    assert_equal true, cfg.validate!

    cfg.schema.around_rebuild = ->(**, &block) { block.call }
    assert_equal true, cfg.validate!
  end

  def test_validate_rejects_non_callable_schema_rebuild_guard
    cfg = SearchEngine::Config.new
    cfg.schema.around_rebuild = :pause_claims

    error = assert_raises(ArgumentError) { cfg.validate! }

    assert_equal 'schema.around_rebuild must be callable or nil', error.message
  end

  def test_configure_runs_validation
    # Force invalid and ensure configure triggers validation
    assert_raises(ArgumentError) do
      SearchEngine.configure do |c|
        c.protocol = 'ftp'
      end
    end
  ensure
    # restore valid protocol to avoid side effects
    SearchEngine.configure { |c| c.protocol = 'http' }
  end
end
