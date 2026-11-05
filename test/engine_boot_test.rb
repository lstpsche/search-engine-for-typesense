# frozen_string_literal: true

require 'test_helper'
require 'fileutils'
require 'open3'
require 'tmpdir'

class EngineBootTest < Minitest::Test
  def test_inline_host_can_eager_load_without_the_active_job_railtie
    Dir.mktmpdir do |root|
      run_host(root, eager: true, checks: <<~'RUBY_CODE')
        abort 'unexpected job framework' if defined?(::ActiveJob::Base)
        abort 'unexpected dispatch' unless SearchEngine.config.indexer.dispatch == :inline
      RUBY_CODE
    end
  end

  def test_lazy_host_registers_nonconventionally_named_rails_models_before_callbacks_and_after_reload
    Dir.mktmpdir do |root|
      models = File.join(root, 'app/models/search_engine/catalog')
      FileUtils.mkdir_p(models)
      File.write(File.join(models, 'listing.rb'), <<~'RUBY_CODE')
        class SearchEngine::Catalog::Listing < SearchEngine::Base
          collection 'catalog_entries'
          identify_by { |id| "sku_#{id}" }
        end
      RUBY_CODE
      run_host(root, eager: false, checks: <<~'RUBY_CODE')
        initial = SearchEngine::Registry.mapping.fetch('catalog_entries')
        abort 'wrong first identity' unless initial.compute_document_id(1) == 'sku_1'
        Rails.application.reloader.reload!
        current = SearchEngine::Registry.mapping.fetch('catalog_entries')
        abort 'stale mapping' if current.equal?(initial)
        abort 'wrong reloaded identity' unless current.compute_document_id(1) == 'sku_1'
      RUBY_CODE
    end
  end

  private

  def run_host(root, eager:, checks:)
    boot = <<~'RUBY_CODE'
      require 'action_controller/railtie'
      require 'search_engine'
      class InlineApp < Rails::Application
        config.root = ARGV.fetch(0)
        config.eager_load = ARGV.fetch(1) == 'true'
        config.cache_classes = config.eager_load
        config.secret_key_base = 'test' * 20
        config.logger = Logger.new(IO::NULL)
      end
      InlineApp.initialize!
    RUBY_CODE
    output, status = Open3.capture2e(
      { 'RAILS_ENV' => 'test' }, RbConfig.ruby, '-I', File.expand_path('../lib', __dir__),
      '-e', boot + checks, root, eager.to_s
    )
    assert status.success?, output
  end
end
