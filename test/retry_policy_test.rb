# frozen_string_literal: true

require 'test_helper'

class RetryPolicyTest < Minitest::Test
  def build_policy(cfg = { attempts: 3, base: 0.5, max: 2.0, jitter_fraction: 0.0 })
    SearchEngine::Indexer::RetryPolicy.from_config(cfg)
  end

  def test_retryable_on_timeout_and_connection
    policy = build_policy
    assert policy.retry?(1, SearchEngine::Errors::Timeout.new('t'))
    assert policy.retry?(2, SearchEngine::Errors::Connection.new('c'))
  end

  def test_retryable_on_transient_api_status
    policy = build_policy
    error_429 = SearchEngine::Errors::Api.new('429', status: 429)
    error_500 = SearchEngine::Errors::Api.new('500', status: 500)
    error_599 = SearchEngine::Errors::Api.new('599', status: 599)

    assert policy.retry?(1, error_429)
    assert policy.retry?(1, error_500)
    assert policy.retry?(1, error_599)
  end

  def test_not_retryable_on_client_api_error
    policy = build_policy
    error_400 = SearchEngine::Errors::Api.new('400', status: 400)

    refute policy.retry?(1, error_400)
  end

  def test_retryable_on_404
    policy = build_policy
    error_404 = SearchEngine::Errors::Api.new('404', status: 404)

    assert policy.retry?(1, error_404)
  end

  def test_attempts_cap
    policy = build_policy(attempts: 2, base: 0.1, max: 0.2, jitter_fraction: 0.0)
    assert policy.retry?(1, SearchEngine::Errors::Timeout.new('t'))
    refute policy.retry?(2, SearchEngine::Errors::Timeout.new('t'))
  end

  def test_next_delay_exponential_without_jitter
    policy = build_policy(base: 0.5, max: 2.0, jitter_fraction: 0.0)
    assert_in_delta 0.5, policy.next_delay(1, StandardError.new), 1e-6
    assert_in_delta 1.0, policy.next_delay(2, StandardError.new), 1e-6
    assert_in_delta 2.0, policy.next_delay(3, StandardError.new), 1e-6
    assert_in_delta 2.0, policy.next_delay(4, StandardError.new), 1e-6
  end
end
