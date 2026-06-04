# frozen_string_literal: true

require 'test_helper'

class BulkImportStageMetricsTest < Minitest::Test
  class StageMetricProduct < SearchEngine::Base
    collection 'stage_metric_products'
    identify_by :id
    attribute :name, :string
  end

  FakeClient = Struct.new(:calls, keyword_init: true) do
    def import_documents(collection:, jsonl:, action: :upsert)
      calls << { collection: collection, jsonl: jsonl, action: action }
      jsonl.each_line.map { '{"success":true}' }.join("\n")
    end
  end

  def test_summary_includes_stage_duration_totals
    client = FakeClient.new(calls: [])
    batch = [{ id: '1', name: 'one' }, { id: '2', name: 'two' }]
    batch.instance_variable_set(
      :@__search_engine_stage_metrics__,
      source_duration_ms: 12.3,
      map_duration_ms: 45.6,
      source_rows_count: 2
    )

    summary = SearchEngine.stub(:client, client) do
      SearchEngine::Indexer::BulkImport.call(
        klass: StageMetricProduct,
        into: 'stage_metric_products_20260604_000001_001',
        enum: [batch],
        batch_size: nil,
        log_batches: false
      )
    end

    assert_equal 12.3, summary.source_duration_ms_total
    assert_equal 45.6, summary.map_duration_ms_total
    assert_operator summary.jsonl_duration_ms_total, :>=, 0
    assert_operator summary.import_duration_ms_total, :>=, 0
  end
end
