# frozen_string_literal: true

require 'test_helper'
require_relative 'support/vector_models'

class VectorCompilerTest < Minitest::Test
  def test_external_hybrid_preserves_alpha_without_requesting_auto_embedding
    params = VectorModels::Article.search('chair').vector_search(query: [1, 0, 0], alpha: 0.8).to_h
    assert_includes params[:vector_query], 'alpha:0.8'
    assert_equal 'title', params[:query_by]
  end

  def test_auto_embedded_hybrid_includes_embedding_once
    params = VectorModels::SemanticArticle.search('chair').vector_search(alpha: 0.7).to_h
    assert_equal 'title,embedding', params[:query_by]
    assert_includes params[:vector_query], 'alpha:0.7'
    explicit = VectorModels::SemanticArticle.search('chair').options(query_by: 'title,embedding').vector_search.to_h
    assert_equal 'title,embedding', explicit[:query_by]
  end

  def test_history_strings_with_commas_keep_their_boundaries
    params = VectorModels::SemanticArticle.vector_search(queries: ['phone, case', 'tablet'], weights: [0.5, 0.5]).to_h
    tick = 96.chr
    assert_includes params[:vector_query], "queries:[#{tick}phone, case#{tick}, #{tick}tablet#{tick}]"
    assert_includes params[:vector_query], 'query_weights:[0.5, 0.5]'
  end

  def test_vector_distance_sort_reuses_the_active_vector
    params = VectorModels::Article.vector_search(query: [1, 0, 0]).order(vector_distance: :asc).to_h
    assert_equal '_vector_distance:asc', params[:sort_by]
    assert_raises(SearchEngine::Errors::InvalidVectorQuery) { VectorModels::Article.order(vector_distance: :asc).to_h }
  end

  def test_selection_filters_and_multisearch_preserve_vector_parameters
    relation = VectorModels::Article.where(category: 'books').vector_search(query: [1, 0, 0]).per(2)
    assert_equal 'embedding', relation.to_h[:exclude_fields]
    assert_nil relation.select(:embedding).to_h[:exclude_fields]
    multi = SearchEngine::Multi.new.add(:articles, relation).to_payloads.first
    assert_equal relation.to_h[:vector_query], multi[:vector_query]
    assert_equal 2, multi[:per_page]
    assert_includes multi[:filter_by], 'books'
  end
end
