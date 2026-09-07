# frozen_string_literal: true

require 'test_helper'

class MapperValidationTest < Minitest::Test
  class Product < SearchEngine::Base
    collection 'mapper_validation_products'
    identify_by :id
    attribute :name, :string
    attribute :active, :boolean
    embedding from: [:name], model: { model_name: 'ts/all-MiniLM-L12-v2' }

    index do
      map { |row| { name: row.name, active: row.active } }
    end
  end

  class BrokenIdentity < SearchEngine::Base
    collection 'broken_identity_products'
    attribute :name, :string
    identify_by { |row| row.category.code }

    index do
      map { |row| { name: row.name } }
    end
  end

  Row = Struct.new(:id, :name, :active, :category)

  class Client
    attr_reader :documents

    def import_documents(jsonl:, **)
      @documents = jsonl.lines.map { |line| JSON.parse(line) }
      jsonl.lines.map { '{"success":true}' }.join("\n")
    end
  end

  def test_custom_identity_errors_are_not_replaced_with_record_id
    error = assert_raises(NoMethodError) { BrokenIdentity.mapped_data_for(Row.new(123, 'a')) }
    assert_equal :code, error.name
  end

  def test_false_coercions_survive_mapping_and_direct_upsert
    original = SearchEngine.config.mapper.coercions
    SearchEngine.config.mapper.coercions = { enabled: true }
    SearchEngine::Mapper.send(:cache).delete(Product)
    client = Client.new

    ['false', '0', 0, 'true', '1', 1].each do |value|
      expected = %w[true 1].include?(value.to_s)
      assert_equal expected, Product.mapped_data_for(Row.new(1, 'a', value))[:active]
      SearchEngine.stub(:client, client) do
        Product.upsert(data: { id: '1', name: 'a', active: value })
      end
      assert_equal expected, client.documents.first['active']
    end
  ensure
    SearchEngine.config.mapper.coercions = original
    SearchEngine::Mapper.send(:cache).delete(Product)
  end

  def test_record_upsert_leaves_auto_embedding_to_server
    client = Client.new
    SearchEngine.stub(:client, client) { Product.upsert(record: Row.new(1, 'a', false)) }
    assert_equal '1', client.documents.first['id']
    refute client.documents.first.key?('product_embedding')
  end
end
