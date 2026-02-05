# frozen_string_literal: true

require 'test_helper'

class ImportResponseParserTest < Minitest::Test
  def test_parse_jsonl_string_response
    raw = <<~JSONL
      {"success":true}
      {"success":false,"error":"bad-row"}
      {invalid-json}
    JSONL

    success, failure, samples = SearchEngine::Indexer::ImportResponseParser.parse(raw)

    assert_equal 1, success
    assert_equal 2, failure
    assert_equal %w[bad-row invalid-json-line], samples
  end

  def test_parse_array_response
    raw = [
      { 'success' => true },
      { 'success' => false, 'message' => 'm1' },
      { success: false, error: 'm2' },
      'not-a-hash'
    ]

    success, failure, samples = SearchEngine::Indexer::ImportResponseParser.parse(raw)

    assert_equal 1, success
    assert_equal 3, failure
    assert_equal %w[m1 m2], samples
  end

  def test_parse_unknown_shape_returns_zeroes
    assert_equal [0, 0, []], SearchEngine::Indexer::ImportResponseParser.parse({ ok: true })
  end
end
