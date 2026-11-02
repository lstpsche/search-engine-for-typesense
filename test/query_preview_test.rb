# frozen_string_literal: true

require 'test_helper'
require 'shellwords'

class QueryPreviewTest < Minitest::Test
  def test_curl_uses_get_and_quotes_each_query_parameter_without_shell_expansion
    query = "phone's $(printf should-not-run)"
    params = { q: query, query_by: 'title', _hits: { early_limit: 10 } }
    command = SearchEngine::Relation::Dx::DryRun.curl(
      'http://localhost:8108/collections/articles/documents/search', params,
      url_opts: { use_cache: false, cache_ttl: 7 }
    )
    arguments = Shellwords.split(command)
    assert_equal %w[curl --get], arguments.first(2)
    pairs = arguments.each_cons(2).filter_map { |option, value| value if option == '--data-urlencode' }
    assert_includes pairs, "q=#{query}"
    assert_includes pairs, 'use_cache=false'
    assert_includes pairs, 'cache_ttl=7'
    refute(pairs.any? { |pair| pair.start_with?('_hits=') })
    assert_includes arguments, 'X-TYPESENSE-API-KEY: ***'
  end
end
