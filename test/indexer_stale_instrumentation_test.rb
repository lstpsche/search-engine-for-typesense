# frozen_string_literal: true

require 'test_helper'

class IndexerStaleInstrumentationTest < Minitest::Test
  class StaleModel < SearchEngine::Base; end

  def setup
    StaleModel.collection 'stale_models'
    StaleModel.index do
      stale filter: 'status:=stale'
      map { |_| { id: 'stub' } }
    end
  end

  def test_stale_delete_emits_started_and_finished
    SearchEngine.configure do |c|
      c.stale_deletes.enabled = true
      c.stale_deletes.strict_mode = false
    end

    # Stub internal delete to avoid network and force predictable count
    sc = SearchEngine::Indexer.singleton_class
    sc.class_eval do
      alias_method :__orig_pdc_for_test, :perform_delete_and_count
      define_method(:perform_delete_and_count) { |_into, _filter, _timeout| 3 }
    end

    events = SearchEngine::Test.capture_events(/search_engine\.(stale_deletes\.|indexer\.delete_stale)/) do
      SearchEngine::Indexer.delete_stale!(StaleModel, into: 'stale_models')
    end

    names = events.map { |e| e[:name] }
    assert_includes names, 'search_engine.stale_deletes.started'
    assert_includes names, 'search_engine.stale_deletes.finished'
    assert_includes names, 'search_engine.indexer.delete_stale'
    started_idx = names.index('search_engine.stale_deletes.started')
    finished_idx = names.index('search_engine.stale_deletes.finished')
    assert_operator finished_idx, :>, started_idx

    payload = events.find { |e| e[:name] == 'search_engine.indexer.delete_stale' }[:payload]
    assert_equal 'stale_models', payload[:into]
    assert_equal 'ok', payload[:status]
    assert payload.key?(:partition_hash)
  ensure
    # Restore
    sc.class_eval do
      alias_method :perform_delete_and_count, :__orig_pdc_for_test
      remove_method :__orig_pdc_for_test
    end
  end
end
