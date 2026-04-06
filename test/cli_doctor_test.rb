# frozen_string_literal: true

require 'test_helper'
require 'search_engine/cli/doctor'

class CliDoctorTest < Minitest::Test
  Runner = SearchEngine::Cli::Doctor::Runner
  FakeClient = Struct.new(:health)

  def test_health_check_fails_when_ok_is_false
    result = run_health_check({ ok: false })

    assert_equal 'health_check', result[:name]
    assert_equal false, result[:ok]
    assert_equal :error, result[:severity]
    refute_nil result[:hint]
  end

  def test_health_check_passes_when_ok_is_true
    result = run_health_check({ ok: true })

    assert_equal 'health_check', result[:name]
    assert_equal true, result[:ok]
    assert_equal :info, result[:severity]
    assert_nil result[:hint]
  end

  def test_health_check_fails_when_ok_is_missing
    result = run_health_check({})

    assert_equal 'health_check', result[:name]
    assert_equal false, result[:ok]
    assert_equal :error, result[:severity]
  end

  def test_health_check_accepts_string_key_ok
    result = run_health_check({ 'ok' => true })

    assert_equal 'health_check', result[:name]
    assert_equal true, result[:ok]
    assert_equal :info, result[:severity]
  end

  private

  def run_health_check(payload)
    runner = Runner.new
    client = FakeClient.new(payload)

    runner.stub(:client_with_overrides, client) do
      runner.check_connectivity_health
    end
  end
end
