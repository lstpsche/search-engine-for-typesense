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

  def test_legacy_client_hydrated_hits_are_reused_with_facet_and_selection_context
    raw = {
      hits: [{ document: { id: '1', name: 'alpha' } }],
      found: 1,
      facet_counts: [{ field_name: 'name', counts: [{ value: 'alpha', count: 1 }] }]
    }
    eager = SearchEngine::Result.new(raw, klass: Product)
    client = Object.new
    calls = []
    client.define_singleton_method(:search) do |collection:, params:, url_opts:|
      calls << [collection, params[:q], url_opts]
      eager
    end
    relation = Product.all.select(:name).options(selection: { strict_missing: true })
                      .facet_query(:name, 'alpha', label: 'First')
    SearchEngine.config.stub(:client, client) do
      result = relation.execute
      assert_same eager.first, result.first
      assert_equal 'First', result.facet_values(:name).first.fetch(:label)
      assert_nil eager.facet_values(:name).first.fetch(:label)
      assert_equal ['alpha'], relation.pluck(:name)
      assert_same eager.first, SearchEngine::Hydration::Materializers.preview(relation.per(1), 1).first
    end
    assert_equal [[Product.collection, '*', {}]] * 2, calls
  end

  def test_strict_pluck_checks_requested_fields_in_symbol_keyed_hits
    client = SearchEngine::Test::StubClient.new
    client.enqueue_response(:search, { hits: [{ document: { id: '1' } }], found: 1 })
    SearchEngine.config.stub(:client, client) do
      relation = Product.all.options(selection: { strict_missing: true })
      assert_equal ['1'], relation.ids
      error = assert_raises(SearchEngine::Errors::MissingField) { relation.pluck(:name) }
      assert_equal ['name'], error.details.fetch(:requested)
    end
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
