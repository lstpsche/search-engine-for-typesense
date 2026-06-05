# frozen_string_literal: true

require 'rails/generators'
require 'rails/generators/active_record'

module SearchEngine
  module Generators
    module PostgresOutbox
      # Install generator for PostgreSQL outbox migration helpers.
      #
      # @example
      #   rails g search_engine:postgres_outbox:install
      class InstallGenerator < Rails::Generators::Base
        include ActiveRecord::Generators::Migration

        source_root File.expand_path('templates', __dir__)

        def create_outbox_table_migration
          migration_template 'create_outbox_events.rb.tt',
                             'db/migrate/create_search_engine_outbox_events.rb'
        end

        def create_trigger_examples_migration
          migration_template 'add_outbox_triggers.rb.tt',
                             'db/migrate/add_search_engine_outbox_triggers.rb'
        end

        def self.next_migration_number(_dirname)
          sleep 1
          Time.now.utc.strftime('%Y%m%d%H%M%S')
        end
      end
    end
  end
end
