# frozen_string_literal: true

require 'test_helper'
require 'active_job'
require_relative '../app/search_engine/search_engine/index_partition_job'

class IndexPartitionJobTest < Minitest::Test
  def test_perform_preserves_original_error_when_constantize_fails
    job = SearchEngine::IndexPartitionJob.new

    events = SearchEngine::Test.capture_events('search_engine.dispatcher.job_error') do
      error = assert_raises(ArgumentError) do
        job.perform('DefinitelyMissingSearchEngineClass', { shard: 1 }, metadata: nil)
      end
      assert_match(/unknown collection class/i, error.message)
    end

    assert_equal 1, events.length
    payload = events.first[:payload]
    assert_equal 'ArgumentError', payload[:error_class]
    assert_equal({}, payload[:metadata])
    assert_match(/unknown collection class/i, payload[:message_truncated])
  end
end
