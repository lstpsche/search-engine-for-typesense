# frozen_string_literal: true

require 'test_helper'
require_relative 'support/vector_models'

class VectorDslTest < Minitest::Test
  def test_rejects_fractional_and_nonfinite_integer_options
    %i[k ef flat_search_cutoff].each do |key|
      [1.5, Float::NAN, Float::INFINITY, 0, -1].each do |value|
        assert_raises(SearchEngine::Errors::InvalidVectorQuery) do
          VectorModels::Article.vector_search(query: [1, 0, 0], **{ key => value })
        end
      end
    end
    assert_match(/k:10/, VectorModels::Article.vector_search(query: [1, 0, 0], k: '10').to_h[:vector_query])
  end

  def test_rejects_nonfinite_and_nonreal_vectors_without_disclosing_the_array
    [Float::NAN, Float::INFINITY, Complex(1, 2), '1'].each do |value|
      error = assert_raises(SearchEngine::Errors::InvalidVectorQuery) do
        VectorModels::Article.vector_search(query: [value, 0, 0])
      end
      refute_includes error.message, value.inspect
    end
  end

  def test_requires_vector_field_and_declared_dimension
    assert_raises(SearchEngine::Errors::InvalidVectorQuery) do
      VectorModels::Article.vector_search(:title, query: [1, 0, 0])
    end
    assert_raises(SearchEngine::Errors::InvalidVectorQuery) do
      VectorModels::Article.vector_search(:embedding, query: [1, 0])
    end
    assert_match(/raw_vector:/, VectorModels::Article.vector_search(:raw_vector, query: [1]).to_h[:vector_query])
  end

  def test_rejects_ambiguous_and_undeclared_fields
    klass = Class.new(SearchEngine::Base)
    assert_raises(SearchEngine::Errors::InvalidVectorQuery) { klass.vector_search(query: [1]) }
    assert_raises(SearchEngine::Errors::InvalidVectorQuery) { klass.vector_search(:unknown, query: [1]) }
    klass.embedding :first, num_dim: 3
    klass.embedding :second, num_dim: 3
    assert_raises(SearchEngine::Errors::InvalidVectorQuery) { klass.vector_search(query: [1, 0, 0]) }
  end

  def test_rejects_mutually_exclusive_modes
    assert_raises(SearchEngine::Errors::InvalidVectorQuery) do
      VectorModels::Article.vector_search(query: [1, 0, 0], id: 'a')
    end
  end

  def test_rejects_unrepresentable_history_and_ids
    ['a:b', "a\nb", ' a', "a#{96.chr}b"].each do |value|
      assert_raises(SearchEngine::Errors::InvalidVectorQuery) do
        VectorModels::SemanticArticle.vector_search(queries: [value])
      end
    end
    ['', 'a,b', 'a:b', 'a(b)', ' a'].each do |value|
      assert_raises(SearchEngine::Errors::InvalidVectorQuery) { VectorModels::Article.find_similar(value) }
    end
  end

  def test_preserves_representable_scalar_ids
    ['a[b]', 'a`b', 'a b', false].each do |value|
      assert_includes VectorModels::Article.find_similar(value).to_h[:vector_query], "id:#{value}"
    end
  end

  def test_preserves_caller_arrays_and_relation_immutability
    vector = [1, 0, 0]
    original = VectorModels::Article.all
    relation = original.vector_search(query: vector)
    vector[0] = 0
    assert_nil original.to_h[:vector_query]
    assert_match(/1.0,0.0,0.0/, relation.to_h[:vector_query])
    assert_match(/0.0,1.0,0.0/, relation.vector_search(query: [0, 1, 0]).to_h[:vector_query])
  end

  def test_rejects_nonfinite_weights
    assert_raises(SearchEngine::Errors::InvalidVectorQuery) do
      VectorModels::SemanticArticle.vector_search(queries: ['a'], weights: [Float::NAN])
    end
  end
end
