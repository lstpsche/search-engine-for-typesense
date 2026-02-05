# frozen_string_literal: true

require 'json'
require 'tmpdir'
require 'pathname'

module SearchEngine
  module Cli
    # Small, pure helpers shared by CLI tasks/commands.
    #
    # Side‑effect free and safe to require in task context.
    # All helpers return strings/values; callers are responsible for printing.
    module Support
      # Common UI constants (kept minimal to avoid behavior drift)
      DOCS_ROOT = 'docs/'
      SEP       = ' - '
      BULLET    = '-'
      CHECK     = '✓'
      WARN      = '⚠'
      CROSS     = '✖'

      module_function

      # --- JSON helpers ------------------------------------------------------
      # @param str [Object]
      # @return [Boolean]
      def json_string?(str)
        s = str.to_s.strip
        return false if s.empty?

        JSON.parse(s)
        true
      rescue StandardError
        false
      end

      # Parse JSON safely.
      # @param str [Object]
      # @return [Object, nil] parsed JSON or nil when invalid
      def parse_json_safe(str)
        s = str.to_s
        return nil if s.strip.empty?

        JSON.parse(s)
      rescue StandardError
        nil
      end

      # Parse JSON or return original string, without raising.
      # @param str [Object]
      # @return [Object] parsed value or the original string
      def parse_json_or_string(str)
        parsed = parse_json_safe(str)
        return parsed unless parsed.nil?

        str.to_s
      end

      # Whether JSON output is requested via FORMAT=json.
      # @return [Boolean]
      def json_output?
        (ENV['FORMAT'] || '').to_s.strip.downcase == 'json'
      end

      # Returns true when ENV[name] is a truthy flag (1/true/yes/on).
      # @param name [String]
      # @return [Boolean]
      def boolean_env?(name)
        truthy_env?(ENV[name])
      end

      # --- Console formatting ------------------------------------------------
      # Respect NO_COLOR and TTY.
      # @return [Boolean]
      def color?
        no_color = ENV['NO_COLOR']
        return false if truthy_env?(no_color)

        $stdout.respond_to?(:tty?) ? $stdout.tty? : false
      end

      # Whether emoji are enabled (disabled via NO_EMOJI=1).
      # @return [Boolean]
      def emoji?
        return false if truthy_env?(ENV['NO_EMOJI'])

        true
      end

      # Heading formatter (no decoration by default to preserve existing output).
      # @param text [String]
      # @return [String]
      def fmt_heading(text)
        text.to_s
      end

      # Bullet line formatter.
      # @param text [String]
      # @return [String]
      def fmt_bullet(text)
        "#{BULLET} #{text}"
      end

      # Key/Value formatter (key: value)
      # @param key [String]
      # @param value [Object]
      # @return [String]
      def fmt_kv(key, value)
        "#{key}: #{value}"
      end

      # Green success line.
      # @param text [String]
      # @return [String]
      def fmt_ok(text)
        base = emoji? ? "#{CHECK} #{text}" : "OK #{text}"
        color? ? colorize(base, 32) : base
      end

      # Yellow warning line.
      # @param text [String]
      # @return [String]
      def fmt_warn(text)
        base = emoji? ? "#{WARN} #{text}" : "WARN #{text}"
        color? ? colorize(base, 33) : base
      end

      # Red error line.
      # @param text [String]
      # @return [String]
      def fmt_err(text)
        base = emoji? ? "#{CROSS} #{text}" : "ERROR #{text}"
        color? ? colorize(base, 31) : base
      end

      # Simple text wrap (hard break by width).
      # @param text [String]
      # @param width [Integer]
      # @return [String]
      def wrap(text, width: 80)
        s = text.to_s
        return s if width.to_i <= 0

        lines = []
        s.split(/\r?\n/).each do |line|
          lines << line.slice!(0, width) while line.length > width
          lines << line
        end
        lines.join("\n")
      end

      # Indent text by level (2 spaces per level by default).
      # @param text [String]
      # @param level [Integer]
      # @param spaces [Integer]
      # @return [String]
      def indent(text, level: 1, spaces: 2)
        pad = ' ' * (level.to_i * spaces.to_i)
        text.to_s.split(/\r?\n/).map { |l| pad + l }.join("\n")
      end

      # --- Path helpers ------------------------------------------------------
      # Expand a path (supporting ~).
      # @param path [String]
      # @return [String]
      def expand(path)
        File.expand_path(path.to_s)
      end

      # Render a relative path from base (defaults to Dir.pwd).
      # @param path [String]
      # @param base [String]
      # @return [String]
      def rel(path, base: Dir.pwd)
        p = Pathname.new(File.expand_path(path.to_s))
        b = Pathname.new(File.expand_path(base.to_s))
        p.relative_path_from(b).to_s
      rescue ArgumentError
        p.to_s
      end

      # Safe read file contents; returns nil if missing/unreadable.
      # @param path [String]
      # @return [String, nil]
      def safe_read(path)
        p = path.to_s
        return nil unless File.file?(p)

        File.read(p)
      rescue StandardError
        nil
      end

      # Generate a tmp file path (not created).
      # @param prefix [String]
      # @return [String]
      def tmp_path(prefix: 'se')
        t = Time.now.utc.strftime('%Y%m%d%H%M%S')
        rand_s = rand(36 ** 6).to_s(36)
        File.join(Dir.tmpdir, "search_engine-#{prefix}-#{t}-#{rand_s}")
      end

      # --- Internals ---------------------------------------------------------
      def colorize(text, code)
        "\e[#{code}m#{text}\e[0m"
      end
      private_class_method :colorize

      def truthy_env?(val)
        return false if val.nil?

        %w[1 true yes on].include?(val.to_s.strip.downcase)
      end
      private_class_method :truthy_env?
    end
  end
end
