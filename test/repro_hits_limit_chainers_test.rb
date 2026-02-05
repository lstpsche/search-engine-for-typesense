# frozen_string_literal: true

require 'test_helper'

# See tmp/refactor/logic_review.md, row ID R-003
class ReproHitsLimitChainersTest < Minitest::Test
  class Product < SearchEngine::Base
    collection 'products_repro_hits_limit'
    identify_by :id
  end

  def test_documented_hits_limit_chainers_exist
    # This is documentation-driven: DSL should exist
    rel = Product.all
    assert_respond_to rel, :limit_hits
    assert_respond_to rel, :validate_hits!

    preview = rel.limit_hits(1000).validate_hits!(max: 10_000).dry_run!
    body = JSON.parse(preview[:body])
    # Internal _hits preview should reflect both early_limit and max
    assert_equal 1000, body.dig('_hits', 'early_limit')
    assert_equal 10_000, body.dig('_hits', 'max')
  end
end
