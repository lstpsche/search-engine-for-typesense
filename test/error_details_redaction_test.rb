# frozen_string_literal: true

require 'test_helper'

class ErrorDetailsRedactionTest < Minitest::Test
  def test_safe_structured_metadata_survives_error_sanitization
    error = SearchEngine::Errors::InvalidParams.new(
      'invalid document',
      details: {
        missing_required: %w[id name],
        field: 'price',
        expected: 'float',
        got: 'String',
        partition_key: 'p-123'
      }
    )

    assert_equal %w[id name], error.details[:missing_required]
    assert_equal 'price', error.details[:field]
    assert_equal 'float', error.details[:expected]
    assert_equal 'String', error.details[:got]
    assert_equal 'p-123', error.details[:partition_key]
  end

  def test_sensitive_and_payload_metadata_is_redacted_recursively
    error = SearchEngine::Errors::InvalidParams.new(
      'invalid document',
      details: {
        api_key: 'secret-key',
        nested: {
          password: 'secret-password',
          payload: { customer_email: 'private@example.test' }
        },
        response: { document: { customer_email: 'private@example.test' } }
      }
    )

    assert_equal '[REDACTED]', error.details[:api_key]
    assert_equal '[REDACTED]', error.details.dig(:nested, :password)
    assert_equal '[REDACTED]', error.details.dig(:nested, :payload)
    assert_equal '[REDACTED]', error.details[:response]
    refute_includes error.details.to_s, 'private@example.test'
    refute_includes error.details.to_s, 'secret-password'
  end

  def test_detail_strings_and_collections_are_bounded
    error = SearchEngine::Errors::InvalidParams.new(
      'invalid document',
      details: {
        sample_error: "line one\n#{'x' * 600}",
        missing_required: (1..80).map { |index| "field_#{index}" }
      }
    )

    assert_operator error.details[:sample_error].length, :<=, 500
    refute_includes error.details[:sample_error], "\n"
    assert_equal 50, error.details[:missing_required].length
  end
end
