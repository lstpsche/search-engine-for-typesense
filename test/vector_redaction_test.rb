# frozen_string_literal: true

require 'test_helper'

class VectorRedactionTest < Minitest::Test
  def setup
    @original = SearchEngine.config.observability.redact_vectors
    SearchEngine.config.observability.redact_vectors = true
  end

  def teardown
    SearchEngine.config.observability.redact_vectors = @original
  end

  def test_redacts_scientific_notation_signs_and_whitespace
    query = 'embedding:([1.0e-05, -2E+10, +.3, 4.], k:10, alpha:0.8)'
    assert_equal 'embedding:([<4 dims>], k:10, alpha:0.8)', redact(query)
  end

  def test_preserves_history_and_masks_malformed_vector_data
    history = 'embedding:([], queries:[phone, tablet], query_weights:[0.5,0.5])'
    assert_equal history, redact(history)
    refute_includes redact('embedding:([sensitive-vector], k:10)'), 'sensitive-vector'
    assert_equal '[REDACTED]', redact('embedding:([0.12345')
    assert_equal '[REDACTED]', redact('embedding:([[sensitive], still-sensitive], k:10)')
    assert_equal '[REDACTED]', redact('embedding:([sensitive]still-sensitive)')
  end

  def test_explicit_redaction_opt_out_is_preserved
    SearchEngine.config.observability.redact_vectors = false
    query = 'embedding:([1.0e-05], k:10)'
    assert_equal query, redact(query)
  end

  def test_request_event_preview_uses_central_redaction
    events = SearchEngine::Test.capture_events do
      SearchEngine::Instrumentation.instrument('search_engine.search',
                                               params_preview: SearchEngine::Observability.redact(
                                                 vector_query: 'embedding:([1.0e-05,2.0e-05])'
                                               )
                                              ) {}
    end
    assert_equal 'embedding:([<2 dims>])', events.first[:payload][:params_preview][:vector_query]
  end

  private

  def redact(query)
    SearchEngine::Observability.redact(vector_query: query)[:vector_query]
  end
end
