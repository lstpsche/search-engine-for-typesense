# frozen_string_literal: true

require 'test_helper'
require 'search_engine/client/request_builder'

class RequestBuilderTest < Minitest::Test
  def test_build_search_basic
    compiled = SearchEngine::CompiledParams.from({ q: '*', query_by: 'name', per_page: 10 })
    url_opts = { use_cache: true, cache_ttl: 30 }

    req = SearchEngine::Client::RequestBuilder.build_search(
      collection: 'products',
      compiled_params: compiled,
      url_opts: url_opts
    )

    assert_equal :post, req.http_method
    assert_equal '/collections/products/documents/search', req.path
    assert_equal url_opts, req.params
    assert_equal 'application/json', req.headers['Content-Type']

    # body is a Hash; body_json is deterministic JSON
    assert_equal({ q: '*', query_by: 'name', per_page: 10 }, req.body)
    expected_json = SearchEngine::CompiledParams.from(req.body).to_json
    assert_equal expected_json, req.body_json
  end

  def test_build_search_sanitizes_internal_keys
    compiled = SearchEngine::CompiledParams.from(
      {
        q: '*',
        _join: {},
        _selection: { include_count: 1 },
        _hits: [1],
        query_by: 'name'
      }
    )
    req = SearchEngine::Client::RequestBuilder.build_search(
      collection: 'brands',
      compiled_params: compiled,
      url_opts: {}
    )

    refute_includes req.body.keys, :_join
    refute_includes req.body.keys, :_selection
    refute_includes req.body.keys, :_hits
    assert_equal({ q: '*', query_by: 'name' }, req.body)
  end

  def test_body_json_is_deterministic
    # Intentionally provide keys out of order; CompiledParams guarantees ordering
    raw = { per_page: 5, query_by: 'name,brand', q: 'milk' }
    compiled = SearchEngine::CompiledParams.from(raw)
    req = SearchEngine::Client::RequestBuilder.build_search(
      collection: 'products',
      compiled_params: compiled,
      url_opts: {}
    )

    assert_equal(
      SearchEngine::CompiledParams.from(req.body).to_json,
      req.body_json
    )
  end

  def test_constants_are_frozen_and_values
    rb = SearchEngine::Client::RequestBuilder
    assert_equal '/collections/', rb::COLLECTIONS_PREFIX
    assert_equal '/documents/search', rb::DOCUMENTS_SEARCH_SUFFIX
    assert_equal '/documents', rb::DOCUMENTS_SUFFIX
    assert_equal '/documents/import', rb::DOCUMENTS_IMPORT_SUFFIX
    assert_equal '/collections', rb::COLLECTIONS_ROOT
    assert_equal '/aliases/', rb::ALIASES_PREFIX
    assert_equal '/synonyms', rb::SYNONYMS_SUFFIX
    assert_equal '/synonyms/', rb::SYNONYMS_PREFIX
    assert_equal '/stopwords', rb::STOPWORDS_SUFFIX
    assert_equal '/stopwords/', rb::STOPWORDS_PREFIX
    assert_equal '/health', rb::HEALTH_PATH
    expected_doc = 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/' \
                   'client#errors'
    assert_equal expected_doc, rb::DOC_CLIENT_ERRORS

    assert rb::DEFAULT_HEADERS_JSON.frozen?
    assert rb::INTERNAL_ONLY_KEYS.frozen?
  end
end
