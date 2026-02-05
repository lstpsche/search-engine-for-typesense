# frozen_string_literal: true

require 'test_helper'

# See tmp/refactor/logic_review.md, row ID R-002
class ReproStrictMissingSelectionTest < Minitest::Test
  class Product < SearchEngine::Base
    collection 'products_repro_strict_missing'
    identify_by :id
    attribute :name, :string
  end

  def setup
    @orig_client = SearchEngine.config.client
    @stub = SearchEngine::Test::StubClient.new
    SearchEngine.configure { |c| c.client = @stub }
  end

  def teardown
    SearchEngine.configure { |c| c.client = @orig_client }
  end

  def test_strict_missing_raises_when_requested_root_absent
    # Enqueue hit missing the requested root field :name
    @stub.enqueue_response(:search, {
                             'hits' => [
                               { 'document' => { 'id' => 1 } }
                             ],
      'found' => 1,
      'out_of' => 1
                           }
    )

    rel = Product.all
                 .select(:name)
                 .options(selection: { strict_missing: true })

    # Expect MissingField once strictness is propagated to Result
    assert_raises(SearchEngine::Errors::MissingField) { rel.to_a }
  end

  def test_missing_strict_selection
    rel = Product.all
    error = assert_raises(SearchEngine::Errors::UnknownField) do
      rel.select(:id, :unknown)
    end
    assert_match(/UnknownField/i, error.message)
  end
end
