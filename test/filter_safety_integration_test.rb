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
    attribute :marker, :string
    query_by :label
  end

  OPERATOR_TEXT = 'absent || tenant:=beta || label:=absent'
  COMMA_TEXT = 'Books, Heavy'

  def setup
    setup_typesense
    @collection = fixture_collection([
                                       { name: 'tenant', type: 'string' },
                                       { name: 'label', type: 'string', symbols_to_index: ['`', ','] },
                                       { name: 'position', type: 'int32' },
                                       { name: 'marker', type: 'string' }
                                     ]
                                    )
    Document.collection(@collection)
    import_fixture(@collection, [
                     { id: '001', tenant: 'alpha', label: OPERATOR_TEXT, position: 1, marker: 'original' },
                     { id: '1', tenant: 'alpha', label: COMMA_TEXT, position: 2, marker: 'original' },
                     { id: '2', tenant: 'beta', label: 'Books', position: 2, marker: 'original' },
                     { id: '4', tenant: 'beta', label: 'Heavy', position: 3, marker: 'original' },
                     { id: 'id,5', tenant: 'beta', label: 'More Books, Heavy', position: 4, marker: 'original' }
                   ]
    )
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

  def test_an_id_wildcard_cannot_become_a_bulk_mutation
    assert_raises(ArgumentError) { Document.where(id: '*').update_all(marker: 'changed') }
    assert_raises(ArgumentError) { Document.delete_by(id: ['2', '*']) }
    assert_raises(ArgumentError) { Document.delete_by({ tenant: 'alpha' }, label: COMMA_TEXT) }
    assert_equal 5, Document.where(marker: 'original').count
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
