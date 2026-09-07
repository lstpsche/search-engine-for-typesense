# frozen_string_literal: true

require 'test_helper'
require 'timeout'

class BulkImportParallelTest < Minitest::Test
  class Product < SearchEngine::Base
    collection 'parallel_products'
    identify_by :id
    attribute :name, :string
  end

  class Client
    def import_documents(jsonl:, **)
      jsonl.each_line.map { '{"success":true}' }.join("\n")
    end
  end

  def import(callback = nil)
    SearchEngine.stub(:client, Client.new) do
      Timeout.timeout(5) do
        SearchEngine::Indexer::BulkImport.call(
          klass: Product, into: 'parallel_products', enum: Array.new(30) { |id| [{ id: id.to_s, name: 'a' }] },
          batch_size: 1, action: :upsert, log_batches: false, max_parallel: 2, on_batch: callback
        )
      end
    end
  end

  def test_worker_failure_unblocks_producer_and_reaches_caller
    failure = RuntimeError.new('progress callback failed')
    error = assert_raises(RuntimeError) { import(->(**) { raise failure }) }
    assert_same failure, error
  end

  def test_parallel_import_consumes_all_batches
    summary = import
    assert_equal 30, summary.docs_total
    assert_equal 30, summary.success_total
    assert_equal 0, summary.failed_total
  end
end
