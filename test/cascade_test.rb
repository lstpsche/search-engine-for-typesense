# frozen_string_literal: true

require 'test_helper'

class CascadeTest < Minitest::Test
  def test_cascade_reindex_emits_single_outcome_when_partial_fails_then_falls_back
    source_klass, referrer_klass = build_collection_classes
    reverse_graph = {
      'source_items' => [
        { referrer: 'ref_items', local_key: 'source_id', foreign_key: 'id' }
      ]
    }

    with_stubbed_cascade(
      reverse_graph: reverse_graph,
      source_klass: source_klass,
      referrer_klass: referrer_klass,
      full_reindex: true,
      runner: lambda do
        failing_partial = lambda do |*_args, **_kwargs|
          raise 'partial failed'
        end
        SearchEngine::Indexer.stub(:rebuild_partition!, failing_partial) do
          result = SearchEngine::Cascade.cascade_reindex!(
            source: source_klass,
            ids: [42],
            context: :update,
            client: Object.new
          )

          outcomes = result[:outcomes]
          assert_equal 1, outcomes.size

          outcome = outcomes.first
          assert_equal 'ref_items', outcome[:collection]
          assert_equal :full, outcome[:mode]
          assert_equal 'RuntimeError', outcome[:error_class]
          assert_equal 'partial failed', outcome[:message]
        end
      end
    )
  end

  def test_cascade_reindex_deduplicates_outcomes_for_duplicate_referencer_edges
    source_klass, referrer_klass = build_collection_classes
    reverse_graph = {
      'source_items' => [
        { referrer: 'ref_items', local_key: 'source_id', foreign_key: 'id' },
        { referrer: 'ref_items', local_key: 'alt_source_id', foreign_key: 'id' }
      ]
    }

    partial_calls = 0
    full_calls = 0
    partial_reindex = lambda do |*_args, **_kwargs|
      partial_calls += 1
      true
    end
    full_reindex = lambda do |*_args, **_kwargs|
      full_calls += 1
      true
    end

    with_stubbed_cascade(
      reverse_graph: reverse_graph,
      source_klass: source_klass,
      referrer_klass: referrer_klass,
      full_reindex: full_reindex,
      runner: lambda do
        SearchEngine::Indexer.stub(:rebuild_partition!, partial_reindex) do
          result = SearchEngine::Cascade.cascade_reindex!(
            source: source_klass,
            ids: [42],
            context: :update,
            client: Object.new
          )

          assert_equal 2, partial_calls
          assert_equal 0, full_calls

          outcomes = result[:outcomes]
          assert_equal 1, outcomes.size
          assert_equal 'ref_items', outcomes.first[:collection]
          assert_equal :partial, outcomes.first[:mode]
        end
      end
    )
  end

  def test_cascade_reindex_prefers_full_mode_when_same_referencer_has_partial_and_fallback
    source_klass, referrer_klass = build_collection_classes
    reverse_graph = {
      'source_items' => [
        { referrer: 'ref_items', local_key: 'source_id', foreign_key: 'id' },
        { referrer: 'ref_items', local_key: 'alt_source_id', foreign_key: 'id' }
      ]
    }

    partial_calls = 0
    full_calls = 0
    partial_reindex = lambda do |*_args, **_kwargs|
      partial_calls += 1
      raise 'partial failed' if partial_calls == 1

      true
    end
    full_reindex = lambda do |*_args, **_kwargs|
      full_calls += 1
      true
    end

    with_stubbed_cascade(
      reverse_graph: reverse_graph,
      source_klass: source_klass,
      referrer_klass: referrer_klass,
      full_reindex: full_reindex,
      runner: lambda do
        SearchEngine::Indexer.stub(:rebuild_partition!, partial_reindex) do
          result = SearchEngine::Cascade.cascade_reindex!(
            source: source_klass,
            ids: [42],
            context: :update,
            client: Object.new
          )

          assert_equal 2, partial_calls
          assert_equal 1, full_calls

          outcomes = result[:outcomes]
          assert_equal 1, outcomes.size
          outcome = outcomes.first
          assert_equal 'ref_items', outcome[:collection]
          assert_equal :full, outcome[:mode]
          assert_equal 'RuntimeError', outcome[:error_class]
          assert_equal 'partial failed', outcome[:message]
        end
      end
    )
  end

  def test_cascade_reindex_propagates_safe_full_rebuild_guard_failure
    source_klass, referrer_klass = build_collection_classes
    reverse_graph = {
      'source_items' => [
        { referrer: 'ref_items', local_key: 'source_id', foreign_key: 'id' }
      ]
    }
    guard_error = SearchEngine::Errors::Timeout.new('cutover lock unavailable')

    with_stubbed_cascade(
      reverse_graph: reverse_graph,
      source_klass: source_klass,
      referrer_klass: referrer_klass,
      full_reindex: ->(*, **) { raise guard_error },
      runner: lambda do
        SearchEngine::Indexer.stub(:rebuild_partition!, ->(*, **) { raise 'partial failed' }) do
          raised = assert_raises(SearchEngine::Errors::Timeout) do
            SearchEngine::Cascade.cascade_reindex!(
              source: source_klass,
              ids: [42],
              context: :update,
              client: Object.new
            )
          end

          assert_same guard_error, raised
        end
      end
    )
  end

  def test_full_reindex_never_downgrades_safe_rebuild_failure_to_live_partition_import
    guard_error = SearchEngine::Errors::Timeout.new('cutover lock unavailable')
    referrer_klass = Class.new do
      def self.collection = 'ref_items'

      define_singleton_method(:index_collection) do |**|
        raise guard_error
      end
    end
    client = Object.new
    client.define_singleton_method(:resolve_alias) { |_logical| 'ref_items_20260711_000000_001' }
    live_import = ->(*, **) { raise 'live partition fallback must not run' }

    SearchEngine::Indexer.stub(:rebuild_partition!, live_import) do
      raised = assert_raises(SearchEngine::Errors::Timeout) do
        SearchEngine::Cascade.send(
          :__se_full_reindex_for_referrer,
          referrer_klass,
          client: client,
          alias_cache: {}
        )
      end

      assert_same guard_error, raised
    end
  end

  private

  def build_collection_classes
    source_klass = Class.new do
      def self.collection = 'source_items'
    end
    referrer_klass = Class.new
    [source_klass, referrer_klass]
  end

  def with_stubbed_cascade(reverse_graph:, source_klass:, referrer_klass:, full_reindex:, runner:)
    class_lookup = lambda do |name|
      case name
      when 'source_items' then source_klass
      when 'ref_items' then referrer_klass
      end
    end

    SearchEngine::Cascade.stub(:build_reverse_graph, reverse_graph) do
      SearchEngine::Cascade.stub(:detect_immediate_cycles, []) do
        SearchEngine::Cascade.stub(:ensure_source_reference_fields!, nil) do
          SearchEngine::Cascade.stub(:safe_collection_class, class_lookup) do
            SearchEngine::Cascade.stub(:can_partial_reindex?, true) do
              SearchEngine::Cascade.stub(:__se_full_reindex_for_referrer, full_reindex) do
                runner.call
              end
            end
          end
        end
      end
    end
  end
end
