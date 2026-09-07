# frozen_string_literal: true

require 'test_helper'

class MaterializersExecutionTest < Minitest::Test
  class Product < SearchEngine::Base
    collection 'materializer_products'
    identify_by :id
    attribute :name, :string
    has_many :authors, collection: 'authors', local_key: :id, foreign_key: :id
    query_by :name
  end

  class Client
    attr_reader :calls
    attr_accessor :error

    def initialize
      @calls = []
    end

    def search(**args)
      @calls << args
      raise error if error

      SearchEngine::Result.new({ 'hits' => [], 'found' => 0 }, klass: Product)
    end

    def retrieve_document(**)
      raise 'An ID filter must not bypass search constraints'
    end
  end

  def test_first_keeps_search_and_selection_constraints
    client = Client.new
    relation = Product.all.where(id: '123').search('missing').select(:name)
    relation.instance_variable_set(:@__client, client)

    assert_nil relation.first
    assert_equal 1, client.calls.size
    params = client.calls.first[:params].to_h
    assert_equal 'missing', params[:q]
    assert_includes params[:filter_by], '123'
    assert_includes params[:include_fields], 'name'
  end

  def test_reverse_join_is_sent_to_typesense_and_memoized
    client = Client.new
    relation = Product.all.joins(:authors).where(authors: { first_name: 'Ada' })
    relation.instance_variable_set(:@__client, client)

    2.times { assert_empty relation.to_a }
    assert_equal 1, client.calls.size
    assert_includes client.calls.first[:params].to_h[:filter_by], '$authors'
  end

  def test_configuration_errors_propagate_from_all_materializers
    %i[to_a count exists? preview].each do |operation|
      client = Client.new
      client.error = SearchEngine::Errors::Api.new('Could not find field in infix index', status: 400)
      relation = Product.all
      relation.instance_variable_set(:@__client, client)
      error = assert_raises(SearchEngine::Errors::Api) do
        if operation == :preview
          SearchEngine.stub(:client, client) { SearchEngine::Hydration::Materializers.preview(relation, 2) }
        else
          relation.public_send(operation)
        end
      end
      assert_same client.error, error
      assert_equal 1, client.calls.size
    end
  end
end
