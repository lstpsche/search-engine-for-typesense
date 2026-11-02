# frozen_string_literal: true

require 'test_helper'
require_relative 'support/vector_models'
require_relative 'support/typesense_integration'

class VectorIntegrationTest < Minitest::Test
  include TypesenseIntegration

  class Article < VectorModels::Article; end
  class SemanticArticle < VectorModels::SemanticArticle; end

  DOCUMENTS = [
    { id: 'a', title: 'chair', category: 'red', embedding: [1, 0, 0] },
    { id: 'b', title: 'chair', category: 'blue', embedding: [0, 1, 0] },
    { id: 'c', title: 'table', category: 'red', embedding: [0.9, 0.1, 0] }
  ].freeze

  def setup
    setup_typesense
    @collection = fixture_collection([
                                       { name: 'title', type: 'string' },
                                       { name: 'category', type: 'string', facet: true },
                                       { name: 'embedding', type: 'float[]', num_dim: 3 }
                                     ]
                                    )
    Article.collection(@collection)
    import_fixture(@collection, DOCUMENTS)
  end

  def teardown
    teardown_typesense
  end

  def test_external_neighbors_distance_sort_threshold_and_selection
    result = Article.vector_search(query: [1, 0, 0], k: 3).order(vector_distance: :asc).execute
    assert_equal %w[a c b], ids(result)
    assert_in_delta 0, result.hits.first.vector_distance, 0.00001
    refute result.raw.fetch('hits').first.fetch('document').key?('embedding')
    nearby = Article.vector_search(query: [1, 0, 0], distance_threshold: 0.01).execute
    assert_equal %w[a c], ids(nearby)
  end

  def test_hybrid_alpha_changes_the_order_without_auto_embedding
    keyword = Article.search('chair').vector_search(query: [1, 0, 0], alpha: 0.01, k: 3).execute
    vector = Article.search('chair').vector_search(query: [1, 0, 0], alpha: 0.99, k: 3).execute
    assert_operator ids(keyword).index('b'), :<, ids(keyword).index('c')
    assert_operator ids(vector).index('c'), :<, ids(vector).index('b')
  end

  def test_filters_and_explicit_vector_selection
    result = Article.where(category: 'red').vector_search(query: [1, 0, 0], k: 3)
                    .select(:id, :embedding).order(vector_distance: :asc).execute
    assert_equal %w[a c], ids(result)
    assert_equal [1.0, 0.0, 0.0], result.raw.fetch('hits').first.fetch('document').fetch('embedding')
    assert_kind_of Numeric, result.hits.first.vector_distance
  end

  def test_similarity_ids_preserve_supported_delimiters
    ['item[1]', 'item`2', 'item 3'].each do |id|
      import_fixture(@collection, [{ id: id, title: 'reference', category: 'red', embedding: [1, 0, 0] }])
      result = Article.find_similar(id, k: 3).execute
      refute_includes ids(result), id
      assert_operator result.found, :>, 0
    end
  end

  def test_find_similar_pagination_and_grouped_multisearch_metadata
    similar = Article.find_similar('a', k: 3).execute
    assert_equal 'c', ids(similar).first
    page = Article.vector_search(query: [1, 0, 0], k: 3).per(1).page(2).execute
    assert_equal ['c'], ids(page)
    multi = SearchEngine.multi_search_result do |builder|
      builder.add :articles, Article.vector_search(query: [1, 0, 0], k: 3).group_by(:category, limit: 2)
    end
    assert multi[:articles].grouped?
    multi[:articles].groups.each { |group| group.hits.each { |hit| assert_kind_of Numeric, hit.vector_distance } }
  end

  def test_auto_embedding_hybrid_and_history_queries
    setup_semantic_collection
    hybrid = SemanticArticle.search('chair').vector_search(alpha: 0.8, k: 3)
                            .options(rerank_hybrid_matches: true).execute
    assert_operator hybrid.found, :>, 0
    assert_kind_of Numeric, hybrid.hits.first.vector_distance
    ['phone, case', 'phone(case)', 'phone[case]'].each do |query|
      result = SemanticArticle.vector_search(queries: [query, 'tablet'], weights: [0.5, 0.5]).execute
      assert_kind_of Numeric, result.found
    end
  end

  def test_server_rejects_weight_sum_outside_exact_parser_contract
    setup_semantic_collection
    relation = SemanticArticle.vector_search(queries: %w[phone tablet], weights: [0.5, 0.495])
    error = assert_raises(SearchEngine::Errors::Api) { relation.execute }
    assert_equal 400, error.status
    assert_match(/sum to 1.0/, error.body.to_s)
  end

  private

  def ids(result)
    result.hits.map { |hit| hit.instance_variable_get(:@id) }
  end

  def setup_semantic_collection
    skip 'Set TYPESENSE_EMBEDDING_INTEGRATION=1 to allow the built-in model download' unless
      ENV['TYPESENSE_EMBEDDING_INTEGRATION'] == '1'

    name = fixture_collection([
                                { name: 'title', type: 'string' },
                                { name: 'embedding', type: 'float[]',
                                  embed: { from: ['title'], model_config: { model_name: 'ts/all-MiniLM-L12-v2' } } }
                              ]
                             )
    SemanticArticle.collection(name)
    import_fixture(name, DOCUMENTS.map { |document| document.slice(:id, :title) })
  end
end
