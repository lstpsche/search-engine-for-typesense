# frozen_string_literal: true

require 'test_helper'
require 'active_job'
require 'search_engine/cli'

class CliDispatchTest < Minitest::Test
  def test_resolve_dispatch_mode_accepts_only_exact_supported_values
    assert_equal :inline, SearchEngine::Cli.resolve_dispatch_mode('inline')
    assert_equal :active_job, SearchEngine::Cli.resolve_dispatch_mode('active_job')

    error = assert_raises(ArgumentError) { SearchEngine::Cli.resolve_dispatch_mode('sidekiq') }
    alias_error = assert_raises(ArgumentError) { SearchEngine::Cli.resolve_dispatch_mode('aj') }

    assert_equal 'dispatch mode must be :active_job or :inline', error.message
    assert_equal 'dispatch mode must be :active_job or :inline', alias_error.message
  end
end
