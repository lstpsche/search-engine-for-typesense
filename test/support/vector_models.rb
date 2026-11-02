# frozen_string_literal: true

module VectorModels
  class Article < SearchEngine::Base
    collection 'query_articles'
    identify_by :id
    attribute :title, :string
    attribute :category, :string, facet: true
    attribute :location, :geopoint
    attribute :raw_vector, [:float]
    embedding :embedding, suffix: false, num_dim: 3
    query_by :title
  end

  class SemanticArticle < SearchEngine::Base
    collection 'semantic_query_articles'
    identify_by :id
    attribute :title, :string
    embedding :embedding, suffix: false, from: [:title], model: 'ts/all-MiniLM-L12-v2'
    query_by :title
  end
end
