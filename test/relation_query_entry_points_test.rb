# frozen_string_literal: true

require 'test_helper'
require 'minitest/mock'
require_relative 'support/vector_models'

class RelationQueryEntryPointsTest < Minitest::Test
  def test_public_entry_points_delegate_keyword_arguments_without_executing
    calls = {
      where_geo: [:location, { within_radius: { lat: 54.69, lng: 25.28, radius: '10 km' } }],
      order_geo: [:location, { from: { lat: 54.69, lng: 25.28 } }],
      order_eval: ['category:=books', { direction: :desc }],
      vector_search: [:embedding, { query: [1, 0, 0], k: 3 }],
      find_similar: ['article-1', { field: :embedding, k: 3 }]
    }

    SearchEngine.stub(:client, -> { flunk 'Query construction must not access the client' }) do
      calls.each do |method, (argument, keywords)|
        relation = VectorModels::Article.public_send(method, argument, **keywords)
        assert_instance_of SearchEngine::Relation, relation
        expected = VectorModels::Article.all.public_send(method, argument, **keywords).to_h
        assert_equal expected, relation.to_h
      end
    end
  end

  def test_model_entry_point_can_resolve_the_only_embedding
    relation = VectorModels::Article.vector_search(query: [1, 0, 0])
    assert_match(/embedding:/, relation.to_h[:vector_query])
  end
end
