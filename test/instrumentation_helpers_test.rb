# frozen_string_literal: true

require 'test_helper'
require 'active_support/notifications'

class InstrumentationHelpersTest < Minitest::Test
  def test_instrument_yields_ctx_and_sets_status_and_duration
    received = []
    sub = ActiveSupport::Notifications.subscribe('search_engine.test.event') do |*args|
      ev = ActiveSupport::Notifications::Event.new(*args)
      received << ev
    end

    result = SearchEngine::Instrumentation.instrument('search_engine.test.event', collection: 'X') do |ctx|
      ctx[:params_preview] = SearchEngine::Instrumentation.redact({ q: 'milk' })
      :ok_value
    end

    ActiveSupport::Notifications.unsubscribe(sub)

    assert_equal :ok_value, result
    refute_empty received
    payload = received.first.payload
    assert_equal 'X', payload[:collection]
    assert payload[:duration_ms].to_f >= 0.0
    assert_equal :ok, payload[:status]
    assert_kind_of Hash, payload[:params_preview]
    assert payload[:correlation_id]
  end

  def test_instrument_captures_error_and_reraises
    received = []
    sub = ActiveSupport::Notifications.subscribe('search_engine.test.error') do |*args|
      ev = ActiveSupport::Notifications::Event.new(*args)
      received << ev
    end

    assert_raises RuntimeError do
      SearchEngine::Instrumentation.instrument('search_engine.test.error', {}) do |_ctx|
        raise 'boom'
      end
    end

    ActiveSupport::Notifications.unsubscribe(sub)

    refute_empty received
    payload = received.first.payload
    assert_equal :error, payload[:status]
    assert_equal 'RuntimeError', payload[:error_class]
    assert payload[:error_message]
    assert payload[:duration_ms].to_f >= 0.0
  end

  def test_with_correlation_id_scopes_and_restores
    prev = SearchEngine::Instrumentation.current_correlation_id
    SearchEngine::Instrumentation.with_correlation_id('abc') do
      assert_equal 'abc', SearchEngine::Instrumentation.current_correlation_id
      SearchEngine::Instrumentation.with_correlation_id do
        # generated but set
        assert SearchEngine::Instrumentation.current_correlation_id
      end
      # outer still present
      assert_equal 'abc', SearchEngine::Instrumentation.current_correlation_id
    end
    # restore previous (may be nil)
    assert_equal prev, SearchEngine::Instrumentation.current_correlation_id
  end

  def test_filter_redaction_masks_complete_list_members_and_backtick_literals
    params = { filter_by: 'id:=[red, blue] && label:=[`private, x] || token:=secret`]' }
    assert_equal 'id:=*** && label:=***', SearchEngine::Instrumentation.redact(params)[:filter_by]
  end

  def test_redact_returns_compact_hash
    red = SearchEngine::Instrumentation.redact({ q: 'a' * 200, filter_by: "id:=123 && title:='T'" })
    assert_kind_of Hash, red
    assert red[:q].length <= 131 # allowing trailing dots
    assert_kind_of String, red[:filter_by]
    assert_includes red[:filter_by], '***'
  end
end
