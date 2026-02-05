# frozen_string_literal: true

require 'json'
require 'search_engine'
require 'search_engine/cli/doctor'

namespace :search_engine do
  desc 'Run diagnostics checks. Usage: rails search_engine:doctor (FORMAT=table|json)'
  task doctor: :environment do
    exit_code = SearchEngine::Cli::Doctor.run
    Kernel.exit(exit_code)
  rescue StandardError => error
    warn("doctor failed: #{error.message}")
    Kernel.exit(1)
  end
end
