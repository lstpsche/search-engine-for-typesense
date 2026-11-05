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
        abort 'missing engine entrypoint' unless SearchEngine::AppInfo.identifier == 'search_engine/app_info'
        require 'active_job'
        abort 'missing partition job' unless SearchEngine::IndexPartitionJob < ActiveJob::Base
        abort 'missing drain job' unless SearchEngine::PostgresOutbox::DrainJob < ActiveJob::Base
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
        require 'active_job'
        payloads = [SearchEngine::IndexPartitionJob.new.serialize, SearchEngine::PostgresOutbox::DrainJob.new.serialize]
        Rails.application.reloader.reload!
        current = SearchEngine::Registry.mapping.fetch('catalog_entries')
        abort 'stale mapping' if current.equal?(initial)
        abort 'wrong reloaded identity' unless current.compute_document_id(1) == 'sku_1'
        jobs = payloads.map { |payload| ActiveJob::Base.deserialize(payload).class }
        expected = [SearchEngine::IndexPartitionJob, SearchEngine::PostgresOutbox::DrainJob]
        abort 'wrong job class after reload' unless jobs == expected
      RUBY_CODE
    end
  end

  private

  def run_host(root, eager:, checks:)
    boot = <<~'RUBY_CODE'
      require 'logger'
      require 'action_controller/railtie'
      require 'search_engine'
      class InlineApp < Rails::Application
        config.root = ARGV.fetch(0)
        config.eager_load = ARGV.fetch(1) == 'true'
        config.cache_classes = config.eager_load
        config.autoloader = ARGV.fetch(2).to_sym if Rails::VERSION::MAJOR == 6
        config.secret_key_base = 'test' * 20
        config.logger = Logger.new(IO::NULL)
      end
      InlineApp.initialize!
    RUBY_CODE
    modes = Rails::VERSION::MAJOR == 6 ? %w[zeitwerk classic] : %w[zeitwerk]
    modes.each do |mode|
      output, status = Open3.capture2e(
        { 'RAILS_ENV' => 'test' }, RbConfig.ruby, '-I', File.expand_path('../lib', __dir__),
        '-e', boot + checks, root, eager.to_s, mode
      )
      assert status.success?, "#{mode} host failed to boot:\n#{output}"
    end
  end
end
