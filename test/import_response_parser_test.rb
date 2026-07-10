# frozen_string_literal: true

require 'test_helper'

class ImportResponseParserTest < Minitest::Test
  def test_parse_rows_preserves_order_and_normalizes_status
    raw = <<~JSONL
      {"success":true,"status":201,"document":{"secret":"ignored"}}
      {"success":false,"code":"404","error":"not found"}
    JSONL

    rows = SearchEngine::Indexer::ImportResponseParser.parse_rows(raw)

    assert_equal(
      [
        { index: 0, success: true, status: 201, error: nil },
        { index: 1, success: false, status: 404, error: 'not found' }
      ],
      rows
    )
    refute_includes rows.to_s, 'secret'
  end

  def test_parse_legacy_tuple_is_derived_from_strict_rows
    raw = [
      { success: true },
      { success: false, message: 'm1' },
      { 'success' => false, 'error' => 'm2' }
    ]

    assert_equal [1, 2, %w[m1 m2]], SearchEngine::Indexer::ImportResponseParser.parse(raw)
  end

  def test_parse_rows_returns_deeply_immutable_scalar_rows
    rows = SearchEngine::Indexer::ImportResponseParser.parse_rows(
      [{ success: false, error: 'bad row' }]
    )

    assert_predicate rows, :frozen?
    assert_predicate rows.first, :frozen?
    assert_predicate rows.first[:error], :frozen?
    assert_raises(FrozenError) { rows << {} }
    assert_raises(FrozenError) { rows.first[:success] = true }
    assert_raises(FrozenError) { rows.first[:error].replace('changed') }
  end

  def test_error_is_bounded_and_complex_payload_values_are_ignored
    rows = SearchEngine::Indexer::ImportResponseParser.parse_rows(
      [
        { success: false, error: "x\n#{'y' * 300}", document: { secret: 'value' } },
        { success: false, error: { document: { secret: 'value' } } }
      ]
    )

    assert_equal 200, rows.first[:error].length
    refute_includes rows.first[:error], "\n"
    assert_nil rows.last[:error]
    refute_includes rows.to_s, 'secret'
  end

  def test_blank_jsonl_lines_do_not_create_response_rows
    rows = SearchEngine::Indexer::ImportResponseParser.parse_rows("\n  \n{\"success\":true}\n")

    assert_equal [{ index: 0, success: true, status: nil, error: nil }], rows
  end

  def test_nil_and_unknown_top_level_shapes_raise
    nil_error = assert_raises(SearchEngine::Errors::InvalidParams) do
      SearchEngine::Indexer::ImportResponseParser.parse_rows(nil)
    end
    shape_error = assert_raises(SearchEngine::Errors::InvalidParams) do
      SearchEngine::Indexer::ImportResponseParser.parse_rows({ ok: true })
    end

    assert_equal 'Typesense import response is nil', nil_error.message
    assert_equal 'Unsupported Typesense import response shape: Hash', shape_error.message
  end

  def test_invalid_json_and_non_hash_rows_raise_without_echoing_content
    json_error = assert_raises(SearchEngine::Errors::InvalidParams) do
      SearchEngine::Indexer::ImportResponseParser.parse_rows('{"secret":"do-not-echo"')
    end
    row_error = assert_raises(SearchEngine::Errors::InvalidParams) do
      SearchEngine::Indexer::ImportResponseParser.parse_rows(['secret-row'])
    end

    assert_equal 'Invalid JSON in Typesense import response row 0', json_error.message
    assert_equal 'Typesense import response row 0 must be a Hash (got String)', row_error.message
    refute_includes json_error.message, 'secret'
    refute_includes row_error.message, 'secret-row'
  end

  def test_missing_non_boolean_and_conflicting_success_values_raise
    missing = assert_raises(SearchEngine::Errors::InvalidParams) do
      SearchEngine::Indexer::ImportResponseParser.parse_rows([{ code: 400 }])
    end
    non_boolean = assert_raises(SearchEngine::Errors::InvalidParams) do
      SearchEngine::Indexer::ImportResponseParser.parse_rows([{ success: 'true' }])
    end
    conflicting = assert_raises(SearchEngine::Errors::InvalidParams) do
      SearchEngine::Indexer::ImportResponseParser.parse_rows([{ 'success' => true, success: false }])
    end

    assert_match(/missing success/, missing.message)
    assert_match(/non-boolean success/, non_boolean.message)
    assert_match(/conflicting success/, conflicting.message)
  end

  def test_invalid_and_conflicting_status_values_raise
    invalid = assert_raises(SearchEngine::Errors::InvalidParams) do
      SearchEngine::Indexer::ImportResponseParser.parse_rows([{ success: false, status: 400.5 }])
    end
    conflicting = assert_raises(SearchEngine::Errors::InvalidParams) do
      SearchEngine::Indexer::ImportResponseParser.parse_rows(
        [{ success: false, status: 400, code: 404 }]
      )
    end

    assert_match(/non-integer status/, invalid.message)
    assert_match(/conflicting status/, conflicting.message)
  end
end
