# frozen_string_literal: true

require 'test_helper'
require_relative 'support/vector_models'

class VectorDistanceTest < Minitest::Test
  def test_preserves_zero_distance_and_raw_payload
    raw = { 'hits' => [{ 'document' => { 'id' => 'a' }, 'vector_distance' => 0 }] }
    result = SearchEngine::Result.new(raw, klass: VectorModels::Article)
    assert_equal 0, result.hits.first.vector_distance
    assert_same raw, result.raw
  end

  def test_grouped_and_multisearch_results_attach_symbol_keyed_metadata
    raw = { grouped_hits: [{ group_key: ['books'], hits: [{ document: { id: 'a' }, vector_distance: 0.25 }] }] }
    multi = SearchEngine::MultiResult.new(labels: [:articles], raw_results: [raw], klasses: [VectorModels::Article])
    assert_equal 0.25, multi[:articles].groups.first.hits.first.vector_distance
    assert_equal 0.25, multi[:articles].hits.first.vector_distance
  end

  def test_does_not_fabricate_missing_metadata
    result = SearchEngine::Result.new({ hits: [{ document: { id: 'a' } }] }, klass: VectorModels::Article)
    refute_respond_to result.hits.first, :vector_distance
  end

  def test_metadata_reader_takes_precedence_over_a_same_named_document_field
    raw = { hits: [{ document: { vector_distance: 99 }, vector_distance: 0.5 }] }
    result = SearchEngine::Result.new(raw)
    assert_equal 0.5, result.hits.first.vector_distance
    assert_equal 99, result.raw[:hits].first[:document][:vector_distance]
  end
end
