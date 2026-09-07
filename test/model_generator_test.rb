# frozen_string_literal: true

require 'test_helper'
require 'tmpdir'
require 'generators/search_engine/model/model_generator'

class ModelGeneratorTest < Minitest::Test
  def test_reserved_id_generates_identity_and_model_loads
    Dir.mktmpdir do |directory|
      SearchEngine::Generators::ModelGenerator.start(
        ['GeneratedProduct', '--collection', 'generated_products', '--attrs', 'id:integer name:string'],
        destination_root: directory
      )
      file = File.join(directory, 'app/search_engine/generated_product.rb')
      source = File.read(file)
      assert_includes source, 'identify_by :id'
      refute_includes source, 'attribute :id'
      load file
      assert_equal '123', SearchEngine::GeneratedProduct.compute_document_id(Struct.new(:id).new(123))
      assert_equal 'generated_products', SearchEngine::GeneratedProduct.collection
    end
  ensure
    SearchEngine.send(:remove_const, :GeneratedProduct) if SearchEngine.const_defined?(:GeneratedProduct, false)
  end
end
