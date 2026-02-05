# frozen_string_literal: true

require 'test_helper'
require 'search_engine/cli/support'

class CliSupportTest < Minitest::Test
  Support = SearchEngine::Cli::Support

  def with_env(temp)
    backup = {}
    temp.each do |k, v|
      backup[k] = ENV.key?(k) ? ENV[k] : :__absent__
      if v.nil?
        ENV.delete(k)
      else
        ENV[k] = v
      end
    end
    yield
  ensure
    backup.each do |k, v|
      if v == :__absent__
        ENV.delete(k)
      else
        ENV[k] = v
      end
    end
  end

  def test_json_string
    assert_equal true, Support.json_string?('{}')
    assert_equal true, Support.json_string?('  [1, 2, 3]  ')
    assert_equal false, Support.json_string?(nil)
    assert_equal false, Support.json_string?('not json')
  end

  def test_parse_json_safe
    assert_equal({ 'a' => 1 }, Support.parse_json_safe('{"a":1}'))
    assert_nil Support.parse_json_safe('not json')
    assert_nil Support.parse_json_safe('')
  end

  def test_parse_json_or_string
    assert_equal({ 'a' => 1 }, Support.parse_json_or_string('{"a":1}'))
    assert_equal 'x', Support.parse_json_or_string('x')
  end

  def test_json_output_env_toggle
    with_env('FORMAT' => 'json') { assert_equal true, Support.json_output? }
    with_env('FORMAT' => 'table') { assert_equal false, Support.json_output? }
    with_env('FORMAT' => nil) { assert_equal false, Support.json_output? }
  end

  def test_boolean_env
    with_env('X' => '1') { assert_equal true, Support.boolean_env?('X') }
    with_env('X' => 'true') { assert_equal true, Support.boolean_env?('X') }
    with_env('X' => 'yes') { assert_equal true, Support.boolean_env?('X') }
    with_env('X' => 'on') { assert_equal true, Support.boolean_env?('X') }
    with_env('X' => '0') { assert_equal false, Support.boolean_env?('X') }
    with_env('X' => 'false') { assert_equal false, Support.boolean_env?('X') }
    with_env('X' => nil) { assert_equal false, Support.boolean_env?('X') }
  end

  def test_console_formatting_plain_without_color_emoji
    with_env('NO_COLOR' => '1', 'NO_EMOJI' => '1') do
      assert_equal 'Title', Support.fmt_heading('Title')
      assert_equal '- item', Support.fmt_bullet('item')
      assert_equal 'key: val', Support.fmt_kv('key', 'val')
      assert_equal 'OK done', Support.fmt_ok('done')
      assert_equal 'WARN check', Support.fmt_warn('check')
      assert_equal 'ERROR fail', Support.fmt_err('fail')
    end
  end

  def test_wrap_and_indent
    text = 'abcdefghij'
    assert_equal "abc\ndef\nghi\nj", Support.wrap(text, width: 3)
    assert_equal "  a\n  b", Support.indent("a\nb", level: 1, spaces: 2)
  end

  def test_paths_helpers
    home = Dir.respond_to?(:home) ? Dir.home : ENV['HOME']
    assert_equal home, Support.expand('~')

    base = Dir.mktmpdir
    begin
      abs = File.join(base, 'foo', 'bar.txt')
      FileUtils.mkdir_p(File.dirname(abs))
      File.write(abs, 'data')

      rel = Support.rel(abs, base: base)
      assert_equal File.join('foo', 'bar.txt'), rel

      assert_equal 'data', Support.safe_read(abs)
      assert_nil Support.safe_read(File.join(base, 'missing.txt'))

      tmp = Support.tmp_path(prefix: 'x')
      assert_includes tmp, Dir.tmpdir
      assert_includes tmp, 'search_engine-x-'
    ensure
      FileUtils.rm_rf(base)
    end
  end
end
