# frozen_string_literal: true

require 'test_helper'
require 'open3'
require 'tmpdir'

class OptionalJobsBootTest < Minitest::Test
  def test_inline_host_can_eager_load_without_the_active_job_railtie
    Dir.mktmpdir do |root|
      program = <<~'RUBY_CODE'
        require 'action_controller/railtie'
        require 'search_engine'
        class InlineApp < Rails::Application
          config.root = ARGV.fetch(0)
          config.eager_load = true
          config.secret_key_base = 'test' * 20
          config.logger = Logger.new(IO::NULL)
        end
        InlineApp.initialize!
        abort 'unexpected job framework' if defined?(::ActiveJob::Base)
        abort 'unexpected dispatch' unless SearchEngine.config.indexer.dispatch == :inline
      RUBY_CODE
      output, status = Open3.capture2e(
        { 'RAILS_ENV' => 'test' }, RbConfig.ruby, '-I', File.expand_path('../lib', __dir__), '-e', program, root
      )
      assert status.success?, output
    end
  end
end
