# frozen_string_literal: true

require 'test_helper'
require 'active_record'

class ActiveRecordSyncableIntegrationTest < Minitest::Test
  class Database < ActiveRecord::Base
    self.abstract_class = true
  end

  class Record < Database
    self.table_name = 'syncable_records'
    include SearchEngine::ActiveRecordSyncable

    class_attribute :events, default: []

    def __se_syncable_upsert!
      self.class.events << [:upsert, name]
    end

    def __se_syncable_delete!
      self.class.events << [:delete, name]
    end
  end

  def setup
    Database.establish_connection(adapter: 'sqlite3', database: ':memory:')
    Database.connection.create_table(:syncable_records) { |table| table.string :name }
    Record.events = []
    Record.search_engine_syncable(collection: :syncable_records)
  end

  def teardown
    Database.remove_connection
  end

  def test_create_and_update_both_run_after_commit
    record = Record.create!(name: 'created')
    assert_equal [[:upsert, 'created']], Record.events

    record.update!(name: 'updated')
    assert_equal [[:upsert, 'created'], [:upsert, 'updated']], Record.events

    record.destroy!
    assert_equal [:delete, 'updated'], Record.events.last
  end

  def test_rolled_back_create_does_not_sync
    Record.transaction do
      Record.create!(name: 'rolled back')
      assert_empty Record.events
      raise ActiveRecord::Rollback
    end
    assert_empty Record.events
    assert_equal 0, Record.count
  end
end
