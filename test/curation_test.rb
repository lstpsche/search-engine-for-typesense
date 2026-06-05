# frozen_string_literal: true

require 'test_helper'

class CurationTest < Minitest::Test
  class Product < SearchEngine::Base
    collection 'products_curation'
    identify_by :id
  end

  def test_pin_and_hide_chainers_are_immutable_and_preserve_semantics
    r_1 = Product.all
    r_2 = r_1.pin('p_12', 'p_34').hide('p_99')

    refute_equal r_1.object_id, r_2.object_id

    state = r_2.instance_variable_get(:@state)
    cur = state[:curation]
    assert_equal %w[p_12 p_34], cur[:pinned]
    assert_equal %w[p_99], cur[:hidden]

    r_3 = r_2.pin('p_12') # duplicate, should not re-add
    cur_3 = r_3.instance_variable_get(:@state)[:curation]
    assert_equal %w[p_12 p_34], cur_3[:pinned]

    r_4 = r_3.hide('p_99') # duplicate hidden
    cur_4 = r_4.instance_variable_get(:@state)[:curation]
    assert_equal %w[p_99], cur_4[:hidden]
  end

  def test_curate_replaces_provided_keys_and_clear_works
    r =
      Product
      .all
      .pin('x')
      .curate(pin: %w[a b], hide: %w[x], override_tags: %w[tag1], filter_curated_hits: false)

    cur = r.instance_variable_get(:@state)[:curation]
    assert_equal %w[a b], cur[:pinned]
    assert_equal %w[x], cur[:hidden]
    assert_equal %w[tag1], cur[:override_tags]
    assert_equal false, cur[:filter_curated_hits]

    r_2 = r.clear_curation
    assert_nil r_2.instance_variable_get(:@state)[:curation]
  end

  def test_curate_methods
    rel = Product.all.curate(filter_curated_hits: true)
    params = rel.to_typesense_params
    assert_equal({ filter_curated_hits: true }, params[:_curation])
  end

  def test_result_hydrates_curated_hit_metadata
    hits = result_from_hits(
      { 'curated' => true, 'document' => { 'id' => 'p_1' } },
      { 'curated' => false, 'document' => { 'id' => 'p_2' } },
      { 'document' => { 'id' => 'p_3' } }
    ).to_a

    assert_equal true, hits[0].curated_hit?
    assert_equal false, hits[1].curated_hit?
    refute_respond_to hits[2], :curated_hit?
  end

  def test_grouped_result_hydrates_curated_hit_metadata
    result = SearchEngine::Result.new(
      {
        'found' => 2,
        'out_of' => 2,
        'request_params' => { 'group_by' => 'brand_id' },
        'grouped_hits' => [
          {
            'group_key' => ['10'],
            'hits' => [
              { 'curated' => true, 'document' => { 'id' => 'p_1', 'brand_id' => 10 } }
            ]
          },
          {
            'group_key' => ['20'],
            'hits' => [
              { 'curated' => false, 'document' => { 'id' => 'p_2', 'brand_id' => 20 } }
            ]
          }
        ]
      },
      klass: Product
    )

    assert_equal true, result.groups.first.hits.first.curated_hit?
    assert_equal true, result.to_a.first.curated_hit?
    assert_equal false, result.groups.last.hits.first.curated_hit?
  end

  def test_filter_curated_hits_count_includes_visible_curated_hits
    client = SearchEngine::Test::StubClient.new
    client.enqueue_response(
      :search,
      {
        'found' => 40,
        'found_docs' => 40,
        'out_of' => 42,
        'hits' => [
          { 'curated' => true, 'document' => { 'id' => 'p_1' } },
          { 'curated' => false, 'document' => { 'id' => 'p_2' } },
          { 'document' => { 'id' => 'p_3' } },
          { 'curated' => true, 'document' => { 'id' => 'p_4' } }
        ]
      }
    )

    rel = Product.all.curate(filter_curated_hits: true).per(10).page(1)
    rel.instance_variable_set(:@__client, client)

    assert_equal 42, rel.count
    assert_equal 4, rel.to_a.size
    assert_equal 1, client.search_calls.size
  end

  def test_filter_curated_hits_count_uses_found_as_base_when_found_docs_is_absent
    client = SearchEngine::Test::StubClient.new
    client.enqueue_response(
      :search,
      {
        'found' => 42,
        'out_of' => 42,
        'hits' => [
          { 'curated' => true, 'document' => { 'id' => 'p_1' } },
          { 'curated' => false, 'document' => { 'id' => 'p_2' } }
        ]
      }
    )

    rel = Product.all.curate(filter_curated_hits: true).per(10).page(1)
    rel.instance_variable_set(:@__client, client)

    assert_equal 43, rel.count
    assert_equal 1, client.search_calls.size
  end

  def test_filter_curated_hits_exists_uses_visible_curated_hits
    client = SearchEngine::Test::StubClient.new
    client.enqueue_response(
      :search,
      {
        'found' => 0,
        'found_docs' => 0,
        'out_of' => 1,
        'hits' => [
          { 'curated' => true, 'document' => { 'id' => 'p_1' } }
        ]
      }
    )

    rel = Product.all.curate(filter_curated_hits: true).per(10).page(1)
    rel.instance_variable_set(:@__client, client)

    assert rel.exists?
    assert_equal 1, client.search_calls.size
  end

  private

  def result_from_hits(*hits)
    SearchEngine::Result.new(
      { 'found' => hits.size, 'out_of' => hits.size, 'hits' => hits },
      klass: Product
    )
  end
end
