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
