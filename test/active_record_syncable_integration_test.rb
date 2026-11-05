# frozen_string_literal: true

require 'test_helper'
require 'active_record'
require 'tmpdir'

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

  class MappedRecord < Database
    self.table_name = 'syncable_records'
    include SearchEngine::ActiveRecordSyncable
  end

  class StiRecord < MappedRecord
  end

  class ScopedRecord < MappedRecord
  end

  class OutboxRecord < MappedRecord
  end

  class DocumentClient < SearchEngine::Test::OfflineClient
    attr_reader :deletes

    def initialize
      super
      @deletes = []
    end

    def delete_document(collection:, id:)
      deletes << [collection, id]
    end
  end

  def setup
    @client = DocumentClient.new
    @registry = SearchEngine::Registry.mapping
    Database.establish_connection(adapter: 'sqlite3', database: ':memory:')
    Database.connection.create_table(:syncable_records) { |table| table.string :name, :type }
    Record.events = []
    Record.search_engine_syncable(collection: :syncable_records)
    MappedRecord.search_engine_syncable(collection: :syncable_documents)
  end

  def teardown
    SearchEngine::Registry.replace!(@registry)
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

  def test_sti_inherits_mapping_and_uses_it_for_callbacks_and_manual_helpers
    writes = []
    model = Class.new(SearchEngine::Base) { identify_by :name }
    SearchEngine.register_collection!(:syncable_documents, model)
    upsert = lambda do |record:|
      writes << model.compute_document_id(record)
      1
    end

    model.stub(:upsert, upsert) do
      MappedRecord.create!(name: 'parent')
      record = StiRecord.create!(name: 'child')
      assert_instance_of StiRecord, MappedRecord.find(record.id)
      record.update!(name: 'updated')
      assert_equal 1, record.sync_search_engine_record
      model.stub(:find, ->(id) { "found-#{id}" }) do
        assert_equal 'found-updated', record.search_engine_record
      end
    end
    assert_equal %w[parent child updated updated], writes
  end

  def test_child_can_override_actions_collection_and_strategy_without_changing_siblings
    ScopedRecord.search_engine_syncable(collection: :scoped_documents, on: :update)
    OutboxRecord.search_engine_syncable(collection: :syncable_documents, sync_strategy: :postgres_outbox)
    writes = []
    parent = Class.new(SearchEngine::Base)
    scoped = Class.new(SearchEngine::Base)
    SearchEngine.register_collection!(:syncable_documents, parent)
    SearchEngine.register_collection!(:scoped_documents, scoped)
    SearchEngine.stub(:client, @client) do
      parent.stub(:upsert, ->(record:) { writes << [:parent, record.name] }) do
        scoped.stub(:upsert, ->(record:) { writes << [:scoped, record.name] }) do
          record = ScopedRecord.create!(name: 'created')
          record.update!(name: 'updated')
          record.destroy!
          OutboxRecord.create!(name: 'outbox').destroy!
          StiRecord.create!(name: 'inherited')
        end
      end
    end
    assert_equal [[:scoped, 'updated'], [:parent, 'inherited']], writes
    assert_empty @client.deletes
  end

  def test_destroy_first_after_model_registration_preserves_unrelated_primary_key_document
    MappedRecord.insert_all!([{ name: 'existing' }])
    record = MappedRecord.find_by!(name: 'existing')
    mapped_id = "sku_#{record.id}"
    documents = [record.id.to_s, mapped_id]
    model = Class.new(SearchEngine::Base) { identify_by { |row| "sku_#{row.id}" } }
    SearchEngine.register_collection!(:syncable_documents, model)

    delete = lambda do |collection:, id:|
      assert_equal 'syncable_documents', collection
      documents.delete(id)
    end
    @client.stub(:delete_document, delete) { SearchEngine.stub(:client, @client) { record.destroy! } }

    assert_equal [record.id.to_s], documents
  end

  def test_destroy_without_registered_model_uses_primary_key
    MappedRecord.insert_all!([{ name: 'bare collection' }])
    record = MappedRecord.first!
    SearchEngine.stub(:client, @client) { record.destroy! }
    assert_equal [['syncable_documents', record.id.to_s]], @client.deletes
  end

  def test_replaced_model_supplies_identity_for_existing_record
    initial, reloaded = %w[old new].map do |prefix|
      Class.new(SearchEngine::Base) do
        define_singleton_method(:name) { 'ReloadedDocument' }
        identify_by { |record| "#{prefix}-#{record.id}" }
      end
    end
    SearchEngine.register_collection!(:syncable_documents, initial)
    MappedRecord.insert_all!([{ name: 'existing' }])
    record = MappedRecord.first!
    initial.stub(:find, ->(id) { id }) { assert_equal "old-#{record.id}", record.search_engine_record }

    SearchEngine.register_collection!(:syncable_documents, reloaded)
    reloaded.stub(:find, ->(id) { id }) { assert_equal "new-#{record.id}", record.search_engine_record }
    SearchEngine.stub(:client, @client) { record.destroy! }
    assert_equal [['syncable_documents', "new-#{record.id}"]], @client.deletes
  end

  def test_failing_model_autoload_cannot_delete_by_primary_key
    # Load a real Ruby autoload to distinguish a missing dependency from an absent mapping.
    Dir.mktmpdir do |directory|
      path = File.join(directory, 'syncable_document.rb')
      File.write(path, <<~RUBY)
        module SearchEngine
          class SyncableDocument < SearchEngine::Base
            collection :syncable_documents
            identify_by MissingSyncableIdentity
          end
        end
      RUBY
      SearchEngine.autoload(:SyncableDocument, path)
      MappedRecord.insert_all!([{ name: 'first' }, { name: 'second' }])
      errors = StringIO.new
      SearchEngine.config.stub(:logger, Logger.new(errors)) do
        SearchEngine.stub(:client, @client) { MappedRecord.order(:id).each(&:destroy!) }
      end
      assert_empty @client.deletes
      assert_equal 2, errors.string.lines.grep(/delete failed: .*MissingSyncableIdentity/).size
    ensure
      SearchEngine.send(:remove_const, :SyncableDocument) if SearchEngine.const_defined?(:SyncableDocument, false)
    end
  end

  def test_failing_identity_cannot_lookup_or_delete_by_primary_key
    model = Class.new(SearchEngine::Base) { identify_by { raise 'missing identity' } }
    SearchEngine.register_collection!(:syncable_documents, model)
    MappedRecord.insert_all!([{ name: 'existing' }])
    record = MappedRecord.first!
    errors = StringIO.new
    SearchEngine.config.stub(:logger, Logger.new(errors)) do
      model.stub(:find, ->(_) { flunk 'an identity failure must stop lookup' }) do
        assert_nil record.search_engine_record
      end
      SearchEngine.stub(:client, @client) { record.destroy! }
    end
    assert_empty @client.deletes
    assert_includes errors.string, 'missing identity'
  end

  def test_child_after_save_timing_runs_in_the_transaction_without_an_inherited_commit_callback
    SearchEngine.config.stub(:syncable_callback_timing, :after_save) do
      ScopedRecord.search_engine_syncable(collection: :syncable_documents, on: :create)
    end
    writes = []
    model = Class.new(SearchEngine::Base)
    SearchEngine.register_collection!(:syncable_documents, model)
    model.stub(:upsert, ->(record:) { writes << record.name }) do
      ScopedRecord.transaction do
        ScopedRecord.create!(name: 'rolled back')
        assert_equal ['rolled back'], writes
        raise ActiveRecord::Rollback
      end
      ScopedRecord.create!(name: 'committed').update!(name: 'excluded update')
    end
    assert_equal ['rolled back', 'committed'], writes
  end
end
