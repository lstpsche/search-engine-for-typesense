# frozen_string_literal: true

require 'test_helper'
require_relative 'support/typesense_integration'

class PaginationIntegrationTest < Minitest::Test
  include TypesenseIntegration

  class Document < SearchEngine::Base
    identify_by :id
    attribute :position, :integer
    attribute :name, :string
    query_by :name
  end

  def setup
    setup_typesense
    collection = fixture_collection([{ name: 'position', type: 'int32' }, { name: 'name', type: 'string' }])
    Document.collection(collection)
    import_fixture(collection, (1..5).map { |n| { id: n.to_s, position: n, name: 'row' } })
    @ordered = Document.order(position: :asc)
  end

  def teardown
    teardown_typesense
  end

  def test_offset_can_start_between_pages_and_without_a_limit
    assert_equal %w[2 3], @ordered.limit(2).offset(1).ids
    assert_equal %w[4 5], @ordered.offset(3).ids
    assert_empty @ordered.offset(10).ids
    assert_equal %w[3 4], @ordered.limit(2).offset(1).page(2).per(2).ids
  end

  def test_all_counts_pages_using_the_same_page_size_as_the_server
    query = @ordered.limit_hits(2)
    assert_equal %w[1 2 3 4 5], query.all!.map(&:id)
    assert_equal 3, query.per(4).pages_count
    assert_equal 1, @ordered.offset(100).tap(&:to_a).pages_count
  end

  def test_materializers_use_the_requested_window_and_count_all_matches
    window = @ordered.limit(2).offset(1)
    assert_equal '2', window.pick(:id)
    assert_equal '2', window.find_by(name: 'row').id
    assert_equal %w[2 3], SearchEngine::Hydration::Materializers.preview(window, 2).map(&:id)
    assert_equal 5, window.count
    assert_equal 3, window.pages_count
    assert_equal %w[2 3], window.ids
    assert_equal 3, window.pages_count
  end
end
