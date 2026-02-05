# frozen_string_literal: true

require 'test_helper'
require 'search_engine/otel'
require 'ostruct'

class OTelAdapterTest < Minitest::Test
  def setup
    # Ensure clean adapter state
    SearchEngine::Otel.stop! if defined?(SearchEngine::Otel)
  rescue StandardError
    nil
  end

  def test_installed_flag_reflects_sdk_presence
    return if defined?(OpenTelemetry::SDK)

    refute SearchEngine::Otel.installed?
  end

  def test_disabled_path_no_error
    SearchEngine.configure do |c|
      c.opentelemetry = OpenStruct.new(enabled: false, service_name: 'search_engine')
    end

    handle = SearchEngine::Otel.start!
    assert_nil handle

    # Emit a test event; should be a no-op for OTel
    result = SearchEngine::Instrumentation.instrument('search_engine.test.disabled', {}) { :ok }
    assert_equal :ok, result
  end
end
