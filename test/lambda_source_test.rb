# frozen_string_literal: true

require 'test_helper'
require_relative '../lib/search_engine/sources/base'
require_relative '../lib/search_engine/sources/lambda_source'

class LambdaSourceTest < Minitest::Test
  def test_each_batch_consumes_enumerator_lazily
    produced = []
    enum = Enumerator.new do |y|
      produced << :first_batch_emitted
      y << [1]
      produced << :second_batch_emitted
      y << [2]
    end

    source = SearchEngine::Sources::LambdaSource.new(->(**_kwargs) { enum })

    rows = [source.each_batch.first]

    assert_equal [[1]], rows
    assert_equal [:first_batch_emitted], produced
  end

  def test_each_batch_keeps_returned_array_behavior
    source = SearchEngine::Sources::LambdaSource.new(
      ->(**_kwargs) { [[1, 2], [3]] }
    )

    rows = []
    source.each_batch { |batch_rows| rows << batch_rows }

    assert_equal [[1, 2], [3]], rows
  end

  def test_each_batch_consumes_yielded_batches_when_callable_returns_nil
    source = SearchEngine::Sources::LambdaSource.new(
      lambda do |cursor:, partition:, &emit|
        assert_equal :cursor_value, cursor
        assert_equal :partition_value, partition
        emit.call([1])
        emit.call([2, 3])
        nil
      end
    )

    rows = []
    source.each_batch(partition: :partition_value, cursor: :cursor_value) { |batch_rows| rows << batch_rows }

    assert_equal [[1], [2, 3]], rows
  end

  def test_each_batch_raises_when_callable_yields_and_returns_batch_enumerable
    source = SearchEngine::Sources::LambdaSource.new(
      lambda do |**_kwargs, &emit|
        emit.call([1])
        [[2]]
      end
    )

    error = assert_raises(SearchEngine::Errors::InvalidParams) do
      source.each_batch { |_batch_rows| nil }
    end

    assert_match(/must either yield batches or return an Enumerable of batches, not both/i, error.message)
  end

  def test_each_batch_allows_non_batch_return_when_callable_yields
    source = SearchEngine::Sources::LambdaSource.new(
      lambda do |**_kwargs, &emit|
        emit.call([1])
        :ok
      end
    )

    rows = []
    source.each_batch { |batch_rows| rows << batch_rows }

    assert_equal [[1]], rows
  end
end
