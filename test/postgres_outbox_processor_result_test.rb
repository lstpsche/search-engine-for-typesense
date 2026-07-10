# frozen_string_literal: true

require 'test_helper'

class PostgresOutboxProcessorResultTest < Minitest::Test
  def test_validates_exhaustive_disjoint_classification
    error = StandardError.new('bad row')
    result = SearchEngine::PostgresOutbox::ProcessorResult.new(
      processed_event_ids: [1, '2'],
      failed_event_ids: [3],
      errors_by_event_id: { '3' => error }
    )

    assert_same result, result.validate_for!(%w[1 2 3])
    assert_same error, result.error_for(3)
    refute result.success?
  end

  def test_rejects_missing_classification
    result = SearchEngine::PostgresOutbox::ProcessorResult.success([1])

    error = assert_raises(validation_error) { result.validate_for!([1, 2]) }

    assert_includes error.message, 'missing ids: 2'
  end

  def test_rejects_duplicate_classification
    result = SearchEngine::PostgresOutbox::ProcessorResult.new(
      processed_event_ids: [1, '1'],
      failed_event_ids: [2],
      error: 'failed'
    )

    error = assert_raises(validation_error) { result.validate_for!([1, 2]) }

    assert_includes error.message, 'duplicate processed ids: 1'
  end

  def test_rejects_overlapping_classification
    result = SearchEngine::PostgresOutbox::ProcessorResult.new(
      processed_event_ids: [1],
      failed_event_ids: [1, 2],
      error: 'failed'
    )

    error = assert_raises(validation_error) { result.validate_for!([1, 2]) }

    assert_includes error.message, 'overlapping ids: 1'
  end

  def test_rejects_unknown_classification
    result = SearchEngine::PostgresOutbox::ProcessorResult.new(
      processed_event_ids: [1, 99],
      failed_event_ids: [2],
      error: 'failed'
    )

    error = assert_raises(validation_error) { result.validate_for!([1, 2]) }

    assert_includes error.message, 'unknown ids: 99'
  end

  def test_rejects_failed_ids_without_any_error
    result = SearchEngine::PostgresOutbox::ProcessorResult.new(
      processed_event_ids: [1],
      failed_event_ids: [2]
    )

    error = assert_raises(validation_error) { result.validate_for!([1, 2]) }

    assert_includes error.message, 'failed ids without errors: 2'
  end

  def test_rejects_error_mapping_for_non_failed_id
    result = SearchEngine::PostgresOutbox::ProcessorResult.new(
      processed_event_ids: [1],
      failed_event_ids: [2],
      error: 'failed',
      errors_by_event_id: { 1 => 'wrong event' }
    )

    error = assert_raises(validation_error) { result.validate_for!([1, 2]) }

    assert_includes error.message, 'errors for non-failed ids: 1'
    refute result.success?
  end

  def test_rejects_global_error_without_failed_ids
    result = SearchEngine::PostgresOutbox::ProcessorResult.new(
      processed_event_ids: [1],
      error: 'unexpected error'
    )

    error = assert_raises(validation_error) { result.validate_for!([1]) }

    assert_includes error.message, 'error present without failed ids'
  end

  def test_validation_error_bounds_reported_ids
    oversized_id = 'sensitive-looking-id-' * 1_000
    result = SearchEngine::PostgresOutbox::ProcessorResult.success([*(100..130), oversized_id])

    error = assert_raises(validation_error) { result.validate_for!([1]) }

    assert_includes error.message, '32 total'
    refute_includes error.message, oversized_id
    assert_operator error.message.length, :<, 3_000
  end

  private

  def validation_error
    SearchEngine::PostgresOutbox::ProcessorResult::ValidationError
  end
end
