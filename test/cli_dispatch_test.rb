# frozen_string_literal: true

require 'test_helper'
require 'active_job'
require 'search_engine/cli'

class CliDispatchTest < Minitest::Test
  def setup
    @previous_around_rebuild = SearchEngine.config.schema.around_rebuild
    @previous_live_maintenance_override = ENV.fetch('ALLOW_LIVE_INDEX_MAINTENANCE', :missing)
  end

  def teardown
    SearchEngine.config.schema.around_rebuild = @previous_around_rebuild
    if @previous_live_maintenance_override == :missing
      ENV.delete('ALLOW_LIVE_INDEX_MAINTENANCE')
    else
      ENV['ALLOW_LIVE_INDEX_MAINTENANCE'] = @previous_live_maintenance_override
    end
  end

  def test_resolve_dispatch_mode_accepts_only_exact_supported_values
    assert_equal :inline, SearchEngine::Cli.resolve_dispatch_mode('inline')
    assert_equal :active_job, SearchEngine::Cli.resolve_dispatch_mode('active_job')

    error = assert_raises(ArgumentError) { SearchEngine::Cli.resolve_dispatch_mode('sidekiq') }
    alias_error = assert_raises(ArgumentError) { SearchEngine::Cli.resolve_dispatch_mode('aj') }

    assert_equal 'dispatch mode must be :active_job or :inline', error.message
    assert_equal 'dispatch mode must be :active_job or :inline', alias_error.message
  end

  def test_live_index_maintenance_remains_allowed_without_schema_guard
    SearchEngine.config.schema.around_rebuild = nil
    ENV.delete('ALLOW_LIVE_INDEX_MAINTENANCE')

    assert SearchEngine::Cli.ensure_live_index_maintenance_allowed!
  end

  def test_live_index_maintenance_is_refused_with_schema_guard_without_explicit_override
    SearchEngine.config.schema.around_rebuild = ->(**, &block) { block.call }
    ENV.delete('ALLOW_LIVE_INDEX_MAINTENANCE')

    error = assert_raises(ArgumentError) { SearchEngine::Cli.ensure_live_index_maintenance_allowed! }

    assert_match(/ALLOW_LIVE_INDEX_MAINTENANCE=true/, error.message)
  end

  def test_live_index_maintenance_override_must_be_truthy
    SearchEngine.config.schema.around_rebuild = ->(**, &block) { block.call }
    ENV['ALLOW_LIVE_INDEX_MAINTENANCE'] = 'false'
    assert_raises(ArgumentError) { SearchEngine::Cli.ensure_live_index_maintenance_allowed! }

    ENV['ALLOW_LIVE_INDEX_MAINTENANCE'] = 'true'
    assert SearchEngine::Cli.ensure_live_index_maintenance_allowed!
  end
end
