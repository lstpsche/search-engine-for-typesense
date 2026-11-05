# frozen_string_literal: true

require 'test_helper'
require_relative 'support/typesense_integration'

class ScopedResultsIntegrationTest < Minitest::Test
  include TypesenseIntegration

  class Document < SearchEngine::Base
    identify_by :id
    attribute :name, :string
    attribute :tenant, :string
    query_by :name
  end

  class MissingCollection < SearchEngine::Base
    attribute :name, :string
    query_by :name
  end

  def setup
    setup_typesense
    fields = [{ name: 'name', type: 'string' }, { name: 'tenant', type: 'string', facet: true }]
    @collection = fixture_collection(fields)
    Document.collection(@collection)
  end

  def teardown
    teardown_typesense
  end

  def test_scoped_client_governs_every_typed_raw_and_paginated_fetch
    fixtures = (1..251).map { |n| { id: n.to_s, tenant: 'alpha', name: 'allowed' } }
    fixtures << { id: 'secret', tenant: 'beta', name: 'private' }
    import_fixture(@collection, fixtures)
    bulk = Document.all.select(:id, :tenant)

    # The configured client enforces the tenant restriction; ambient credentials can read both tenants.
    SearchEngine.stub(:config, @integration_config) do
      @integration_config.client = scoped_client_for('alpha')
      assert_equal 251, bulk.count
      %i[multi_search multi_search_result].each do |helper|
        result = SearchEngine.public_send(helper) { |m| m.add(:docs, Document.all) }[:docs]
        assert_equal 251, result.found
        assert_equal ['alpha'], result.map(&:tenant).uniq
      end
      raw = SearchEngine.multi_search_raw { |m| m.add(:docs, Document.all) }
      assert_equal 251, raw.fetch(:results).first.fetch(:found)
      assert_equal (1..251).map(&:to_s).sort, bulk.all!.map(&:id).sort

      @integration_config.client = @client
      assert_equal 252, Document.all.count
      assert_equal (1..251).map(&:to_s).sort, bulk.all!.map(&:id).sort
    end
  end

  def test_http_success_with_an_embedded_error_keeps_its_label_and_status
    MissingCollection.collection("#{@collection}_missing")
    searches = ->(multi) { multi.add(:available, Document.all).add(:missing, MissingCollection.all) }
    %i[multi_search multi_search_result].each do |helper|
      error = assert_raises(SearchEngine::Errors::Api) { SearchEngine.public_send(helper, &searches) }
      assert_equal 404, error.status
      assert_includes error.message, ':missing'
      assert_match(/not found|could not find/i, error.body.fetch(:error))
    end
    results = SearchEngine.multi_search_raw(&searches).fetch(:results)
    assert_equal 0, results.first.fetch(:found)
    assert_equal 404, results.last.fetch(:code)
  end

  def test_hydrates_each_selected_and_faceted_hit_once_for_every_entry_point
    import_fixture(@collection, (1..5).map { |n| { id: n.to_s, name: 'row', tenant: 'alpha' } })
    relation = Document.all.per(5).select(:id, :tenant).facet_by(:tenant)
                       .facet_query(:tenant, 'alpha', label: 'Allowed')
    with_hydration_calls do |calls|
      result = relation.execute
      assert_equal 5, calls.size
      assert_equal 5, result.to_a.size
      assert_equal 'Allowed', result.facet_values(:tenant).first.fetch(:label)
      assert_equal 5, relation.to_a.size
      assert_equal 5, calls.size

      preview = relation.per(5)
      assert_equal 2, SearchEngine::Hydration::Materializers.preview(preview, 2).size
      assert_equal 10, calls.size
      assert_equal 3, SearchEngine::Hydration::Materializers.preview(preview, 3).size
      assert_equal 10, calls.size

      typed = SearchEngine.multi_search { |m| m.add(:documents, relation) }[:documents]
      assert_equal 'Allowed', typed.facet_values(:tenant).first.fetch(:label)
      assert_equal 15, calls.size

      direct = @client.search(collection: @collection, params: { q: '*', query_by: 'name' })
      assert_equal 20, calls.size
      assert_instance_of Document, direct.first
    end
  end

  private

  def scoped_client_for(tenant)
    key = scoped_fixture_key(fixture_key(@collection), filter_by: "tenant:=#{tenant}")
    node = { host: @integration_config.host, port: @integration_config.port, protocol: @integration_config.protocol }
    sdk = Typesense::Client.new(api_key: key, nodes: [node])
    SearchEngine::Client.new(config: @integration_config, typesense_client: sdk)
  end

  def with_hydration_calls
    calls = []
    original = Document.method(:from_document)
    hydrate = lambda do |doc|
      calls << doc.fetch('id') { doc.fetch(:id) }
      original.call(doc)
    end

    Document.stub(:from_document, hydrate) { yield calls }
  end
end
