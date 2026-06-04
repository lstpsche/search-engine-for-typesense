# frozen_string_literal: true

require 'test_helper'

class BulkTest < Minitest::Test
  class PrimaryProduct < SearchEngine::Base
    collection 'bulk_primary_products'
    identify_by :id
    attribute :name, :string
  end

  class DependentProduct < SearchEngine::Base
    collection 'bulk_dependent_products'
    identify_by :id
    attribute :name, :string
  end

  def test_index_collections_force_rebuilds_primary_and_cascade_collections
    calls = []
    reverse_graph = {
      'bulk_primary_products' => [{ referrer: 'bulk_dependent_products' }]
    }

    primary_indexer = lambda do |**kwargs|
      calls << [:primary, kwargs]
      { collection: 'bulk_primary_products', status: :ok }
    end
    dependent_indexer = lambda do |**kwargs|
      calls << [:dependent, kwargs]
      { collection: 'bulk_dependent_products', status: :ok }
    end

    SearchEngine::Cascade.stub(:build_reverse_graph, reverse_graph) do
      PrimaryProduct.stub(:index_collection, primary_indexer) do
        DependentProduct.stub(:index_collection, dependent_indexer) do
          SearchEngine::Bulk.index_collections(:bulk_primary_products, silent: true)
        end
      end
    end

    assert_equal(
      [
        [:primary, { force_rebuild: true }],
        [:dependent, { force_rebuild: true, pre: :ensure }]
      ],
      calls
    )
  end
end
