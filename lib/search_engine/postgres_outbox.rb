# frozen_string_literal: true

require 'search_engine/postgres_outbox/event'
require 'search_engine/postgres_outbox/processor_result'
require 'search_engine/postgres_outbox/repository'
require 'search_engine/postgres_outbox/event_processor'
require 'search_engine/postgres_outbox/drainer'
require 'search_engine/postgres_outbox/listener'
require 'search_engine/postgres_outbox/migration_helpers'

module SearchEngine
  # PostgreSQL-backed outbox integration points.
  module PostgresOutbox
  end
end
