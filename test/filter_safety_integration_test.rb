# frozen_string_literal: true

require 'test_helper'
require_relative 'support/typesense_integration'

class FilterSafetyIntegrationTest < Minitest::Test
  include TypesenseIntegration

  class Document < SearchEngine::Base
    identify_by(&:id)
    attribute :tenant, :string
    attribute :label, :string
    attribute :position, :integer
    attribute :published_at, :datetime
    attribute :marker, :string
    query_by :label
  end

  class Reference < SearchEngine::Base
    identify_by(&:id)
    attribute :label, :string
    query_by :label
  end

  OPERATOR_TEXT = 'absent || tenant:=beta || label:=absent'
  COMMA_TEXT = 'Books, Heavy'

  def setup
    setup_typesense
    fields = [
      { name: 'tenant', type: 'string' },
      { name: 'label', type: 'string', symbols_to_index: ['`', ','] },
      { name: 'position', type: 'int32' },
      { name: 'published_at', type: 'int64' },
      { name: 'marker', type: 'string' }
    ]
    @collection = fixture_collection(fields)
    Document.collection(@collection)
    documents = [
      { id: '001', tenant: 'alpha', label: OPERATOR_TEXT, position: 1 },
      { id: '1', tenant: 'alpha', label: COMMA_TEXT, position: 2 },
      { id: '2', tenant: 'beta', label: 'Books', position: 2 },
      { id: '4', tenant: 'beta', label: 'Heavy', position: 3 },
      { id: 'id,5', tenant: 'beta', label: 'More Books, Heavy', position: 4 }
    ]
    import_fixture(@collection, documents.map { |row| row.merge(marker: 'original', published_at: row[:position]) })
  end

  def teardown
    teardown_typesense
  end

  def test_literals_remain_single_values_in_search_and_bulk_writes
    tenant = Document.where(tenant: :alpha)
    assert_equal ['001'], tenant.where(label: OPERATOR_TEXT).ids
    assert_equal ['001'], tenant.where('label = ?', OPERATOR_TEXT).ids
    assert_equal ['1'], Document.where(label: [COMMA_TEXT]).ids
    assert_equal ['1'], Document.where('label IN ?', [COMMA_TEXT]).ids
    assert_equal %w[001 id,5], Document.where(id: ['id,5', '001']).ids.sort

    assert_equal 1, tenant.where('label = ?', OPERATOR_TEXT).update_all(marker: 'changed')
    assert_equal ['001'], Document.where(marker: 'changed').ids
    assert_equal 1, Document.delete_by({ tenant: :alpha, label: OPERATOR_TEXT })
    assert_equal 1, Document.where(label: [COMMA_TEXT]).delete_all
    assert_equal %w[2 4 id,5], Document.where(marker: 'original').ids.sort
  end

  def test_legacy_filters_keep_fixed_predicates_when_values_contain_operators
    expression = '(tenant:=alpha && label:=?) || (tenant:=beta && label:=absent)'
    legacy = SearchEngine::Relation.new(Document, { filters: [expression, OPERATOR_TEXT] })
    assert_equal ['001'], legacy.ids
    assert_equal ['001'], Document.where(tenant: :alpha).where('label = ?', OPERATOR_TEXT).ids

    expression = 'label:=[`absent || tenant:=beta || label:=absent`] && published_at:=?'
    date_filter = SearchEngine::Relation.new(Document, { filters: [expression, Time.at(1).utc] })
    assert_equal 1, date_filter.update_all(marker: 'changed')
    assert_equal ['001'], Document.where(marker: 'changed').ids
  end

  def test_native_join_binds_use_the_referenced_collection_type
    fields = [{ name: 'label', type: 'string' },
              { name: 'document_id', type: 'string', reference: "#{@collection}.id" }]
    collection = fixture_collection(fields)
    Reference.collection(collection)
    references = [{ id: 'alpha-ref', label: 'ref', document_id: '001' },
                  { id: 'beta-ref', label: 'ref', document_id: '2' }]
    import_fixture(collection, references)
    expression = "label:=ref && $#{@collection}((published_at:=?) && tenant:=alpha)"
    relation = SearchEngine::Relation.new(Reference, { filters: [expression, Time.at(1).utc] })
    assert_equal ['alpha-ref'], relation.ids
  end

  def test_an_id_wildcard_cannot_become_a_bulk_mutation
    assert_raises(ArgumentError) { Document.where(id: '*').update_all(marker: 'changed') }
    assert_raises(ArgumentError) { Document.delete_by(id: ['2', '*']) }
    assert_raises(ArgumentError) { Document.delete_by({ tenant: 'alpha' }, label: COMMA_TEXT) }
    assert_equal 5, Document.where(marker: 'original').count
  end

  def test_datetime_filters_use_the_same_epoch_representation_for_reads_and_writes
    instant = DateTime.iso8601('1970-01-01T00:00:02Z')
    assert_equal %w[1 2], Document.where(published_at: [instant]).ids.sort
    assert_equal %w[4 id,5], Document.where('published_at > ?', instant).ids.sort
    assert_equal 2, SearchEngine::Update.update_by(klass: Document, hash: { published_at: instant },
                                                   attributes: { marker: 'published' }
    )
    assert_equal %w[1 2], Document.where(marker: 'published').ids.sort
    assert_equal 2, Document.delete_by(published_at: instant)
    assert_equal %w[001 4 id,5], Document.all.ids.sort
  end

  def test_negated_comparisons_at_the_boundary
    {
      '>' => %w[001 1 2], '>=' => %w[001], '<' => %w[1 2 4 id,5], '<=' => %w[4 id,5],
      '!=' => %w[1 2], '=' => %w[001 4 id,5]
    }.each do |operator, expected|
      assert_equal expected, Document.where.not("position #{operator} ?", 2).ids.sort, operator
    end
    assert_equal %w[1 2], Document.where.not('position NOT IN ?', [2]).ids.sort
    assert_equal 2, Document.where.not('position <= ?', 2).update_all(marker: 'after')
    assert_equal %w[4 id,5], Document.where(marker: 'after').ids.sort
    assert_raises(SearchEngine::Errors::InvalidOperator) { Document.where.not('position:<=2').delete_all }
    assert_equal 5, Document.all.count
  end
end
