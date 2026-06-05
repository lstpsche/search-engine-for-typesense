# frozen_string_literal: true

require 'test_helper'

class DependencyPlannerTest < Minitest::Test
  Event = Struct.new(:collection, :id, keyword_init: true)

  GRAPH = {
    'products' => [
      { referrer: 'product_barcodes', local_key: 'product_id', foreign_key: 'id' }
    ],
    'calculated_products' => [
      { 'referrer' => 'product_balances', 'local_key' => 'calculated_product_id', 'foreign_key' => 'id' }
    ],
    'product_barcodes' => [
      { referrer: 'barcode_snapshots', local_key: 'barcode_id', foreign_key: 'id' }
    ]
  }.freeze

  def test_reverse_graph_dispatches_to_registry_source
    registry_graph = { products: [{ 'referrer' => :product_barcodes }] }

    SearchEngine::Cascade.stub(:build_from_registry, registry_graph) do
      graph = SearchEngine::DependencyPlanner.reverse_graph(source: :registry)

      assert_equal ['products'], graph.keys
      assert_equal 'product_barcodes', graph['products'].first[:referrer]
    end
  end

  def test_reverse_graph_dispatches_to_typesense_source
    client = Object.new
    typesense_graph = { 'products' => [{ referrer: 'product_barcodes' }] }
    called_with = nil

    build_from_typesense = lambda do |arg|
      called_with = arg
      typesense_graph
    end

    SearchEngine::Cascade.stub(:build_from_typesense, build_from_typesense) do
      graph = SearchEngine::DependencyPlanner.reverse_graph(source: :typesense, client: client)

      assert_same client, called_with
      assert_equal 'product_barcodes', graph['products'].first[:referrer]
    end
  end

  def test_reverse_graph_dispatches_to_auto_source
    client = Object.new
    auto_graph = { 'products' => [{ referrer: 'product_barcodes' }] }
    called_with = nil

    build_reverse_graph = lambda do |client:|
      called_with = client
      auto_graph
    end

    SearchEngine::Cascade.stub(:build_reverse_graph, build_reverse_graph) do
      graph = SearchEngine::DependencyPlanner.reverse_graph(source: :auto, client: client)

      assert_same client, called_with
      assert_equal 'product_barcodes', graph['products'].first[:referrer]
    end
  end

  def test_reverse_graph_rejects_unknown_source
    assert_raises(ArgumentError) do
      SearchEngine::DependencyPlanner.reverse_graph(source: :unknown)
    end
  end

  def test_order_collections_places_dependencies_before_referrers
    ordered = SearchEngine::DependencyPlanner.order_collections(
      %i[product_barcodes products product_balances calculated_products],
      reverse_graph: GRAPH
    )

    assert_equal %w[calculated_products product_balances products product_barcodes], ordered
  end

  def test_order_collections_collapses_duplicates_and_keeps_unknown_collections
    ordered = SearchEngine::DependencyPlanner.order_collections(
      %w[product_barcodes products product_barcodes unknown],
      reverse_graph: GRAPH
    )

    assert_equal %w[products product_barcodes unknown], ordered
  end

  def test_order_collections_handles_cycles_deterministically
    graph = {
      'alpha' => [{ referrer: 'beta' }],
      'beta' => [{ referrer: 'alpha' }]
    }

    events = SearchEngine::Test.capture_events('search_engine.dependency_planner.cycle') do
      assert_equal(
        %w[alpha beta],
        SearchEngine::DependencyPlanner.order_collections(%w[beta alpha], reverse_graph: graph)
      )
    end

    assert_equal 1, events.size
    assert_equal %w[alpha beta], events.first[:payload][:collections]
  end

  def test_order_events_orders_by_collection_and_preserves_same_collection_order
    events = [
      Event.new(collection: 'product_barcodes', id: 1),
      Event.new(collection: 'products', id: 2),
      Event.new(collection: 'product_barcodes', id: 3),
      Event.new(collection: nil, id: 4)
    ]

    ordered = SearchEngine::DependencyPlanner.order_events(events, reverse_graph: GRAPH)

    assert_equal [2, 1, 3, 4], ordered.map(&:id)
  end

  def test_order_events_reads_hash_collection_keys
    events = [
      { 'collection' => 'product_barcodes', id: 1 },
      { collection: 'products', id: 2 }
    ]

    ordered = SearchEngine::DependencyPlanner.order_events(events, reverse_graph: GRAPH)

    ids = ordered.map { |event| event[:id] }
    assert_equal [2, 1], ids
  end

  def test_referencers_for_returns_direct_referrers
    assert_equal(
      %w[product_barcodes],
      SearchEngine::DependencyPlanner.referencers_for(:products, reverse_graph: GRAPH)
    )
  end

  def test_dependencies_for_returns_direct_targets_for_referrer
    assert_equal(
      %w[products],
      SearchEngine::DependencyPlanner.dependencies_for(:product_barcodes, reverse_graph: GRAPH)
    )
  end

  def test_bulk_stages_preserve_primary_and_cascade_split
    stages = SearchEngine::DependencyPlanner.bulk_stages(%w[products product_barcodes], reverse_graph: GRAPH)

    assert_equal %w[products], stages[:stage_1]
    assert_equal %w[product_barcodes barcode_snapshots], stages[:cascade]
  end
end
