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
end
