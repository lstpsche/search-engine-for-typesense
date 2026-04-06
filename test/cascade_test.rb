# frozen_string_literal: true

require 'test_helper'

class CascadeTest < Minitest::Test
  def test_cascade_reindex_emits_single_outcome_when_partial_fails_then_falls_back
    source_klass = Class.new do
      def self.collection = 'source_items'
    end
    referrer_klass = Class.new

    reverse_graph = {
      'source_items' => [
        { referrer: 'ref_items', local_key: 'source_id', foreign_key: 'id' }
      ]
    }
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
              SearchEngine::Cascade.stub(:__se_full_reindex_for_referrer, true) do
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
            end
          end
        end
      end
    end
  end
end
