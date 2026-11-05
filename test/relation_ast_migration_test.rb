# frozen_string_literal: true

require 'test_helper'

class RelationASTMigrationTest < Minitest::Test
  class Author < SearchEngine::Base
    collection 'authors_relation_migration'
    attribute :registered_at, :time
  end

  class Product < SearchEngine::Base
    collection 'products_relation_migration'
    identify_by :id
    attribute :active, :boolean
    attribute :price, :float
    attribute :brand_id, :integer
    attribute :registered_at, :time_string
    attribute :author_id, :string
    join :authors, collection: 'authors_relation_migration', local_key: :author_id, foreign_key: :id
  end

  def test_legacy_filters_migrate_to_ast_raw
    r = SearchEngine::Relation.new(Product, { filters: ['active:=true', 'brand_id:=[1,2]'] })

    assert_equal 2, r.ast.length
    filter_by = r.to_typesense_params[:filter_by]
    assert_match(/active:=true/, filter_by)
    assert_match(/brand_id:=\[1,\s*2\]/, filter_by)
  end

  def test_legacy_templates_bind_only_complete_operands_in_their_own_collection
    template = '((active:=true) && $authors((registered_at:=?) || name:=[`left || right`])) && registered_at:=?'
    instant = Time.at(2).utc
    relation = SearchEngine::Relation.new(Product, { joins: [:authors], filters: [template, instant, instant] })

    assert_equal '((active:=true) && $authors_relation_migration((registered_at:=2) || name:=[`left || right`])) ' \
                 '&& registered_at:=[`1970-01-01T00:00:02Z`]', relation.filter_params
    state = { joins: [:authors], filters: ['$authors(name:=?)', 'left || right'] }
    relation = SearchEngine::Relation.new(Product, state)
    assert_equal '$authors_relation_migration(name:=[`left || right`])', relation.filter_params
  end

  def test_an_expanded_question_mark_stays_data_when_the_relation_is_chained
    relation = SearchEngine::Relation.new(Product, { filters: ['active:=true && name:=?', 'Which?'] })

    assert_equal 'active:=true && name:=[`Which?`] && brand_id:=4', relation.where(brand_id: 4).filter_params
    assert_equal 'active:=false', relation.rewhere(active: false).filter_params
    assert_nil relation.unscope(:where).filter_params
    assert_equal 'active:=true && name:=[`Which?`]', relation.filter_params
    state = { filters: ['brand_id:=?', 4], ast: [SearchEngine::AST.eq(:active, false)] }
    assert_equal 'active:=false', SearchEngine::Relation.new(Product, state).filter_params
  end

  def test_escaped_question_marks_remain_verbatim_and_quoted_or_partial_binds_raise
    template = 'name:=[`fixed \? || punctuation`] && id:=?'
    assert_equal template.sub(/=\?\z/, '=1'),
                 SearchEngine::Relation.new(Product, { filters: [template, 1] }).filter_params
    sanitizer = SearchEngine::Filters::Sanitizer
    assert_equal([0, 1, 0], (1..3).map { |count| sanitizer.count_placeholders("#{'\\' * count}?") })
    %w[name:=`?` name:='?' name:="?" name:=prefix? name:=?suffix name:=[?]].each do |fragment|
      assert_raises(ArgumentError) { SearchEngine::Relation.new(Product, { filters: [fragment, 'a || b'] }) }
    end
    assert_raises(ArgumentError) { sanitizer.apply_placeholders('$authors(name:=?)', ['x) || (other']) }
  end

  def test_compilation_prefers_ast
    r = Product.all.where(id: 1).where(['price > ?', 100])
    compiled = SearchEngine::Compiler.compile(r.ast, klass: Product)
    assert_equal compiled, r.to_typesense_params[:filter_by]
  end
end
