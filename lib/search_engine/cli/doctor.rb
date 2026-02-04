# frozen_string_literal: true

require 'json'
require 'search_engine/cli/support'

module SearchEngine
  module CLI
    # Orchestrates diagnostics checks and renders output for the doctor task.
    #
    # Public API: SearchEngine::CLI::Doctor.run
    #
    # Supports FORMAT env var (table/json) and redaction-aware details.
    #
    # @since M8
    # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/cli#doctor
    module Doctor
      DOCS_BASE = 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/'

      module_function

      # Build a link to the docs site (pass slug without .md, anchors allowed)
      def wiki(slug)
        return nil if slug.nil? || slug.to_s.strip.empty?

        DOCS_BASE + slug.to_s.tr('_', '-')
      end

      class << self
        # Run all checks and print output to STDOUT.
        # Returns exit code (0 success, 1 failure).
        # @return [Integer]
        # @since M8
        # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/cli#doctor
        def run
          puts 'Executing doctor checks...'

          runner = Runner.new
          result = runner.execute

          if runner.json?
            puts(JSON.generate(result))
          else
            puts(Renderers::Table.render(result, verbose: runner.verbose?))
          end

          result[:ok] ? 0 : 1
        end
      end

      # --- Internals -----------------------------------------------------------------

      # Lightweight struct-like result builder keeping stable key ordering in JSON
      module Builder
        module_function

        def new_summary
          { passed: 0, warned: 0, failed: 0, duration_ms_total: 0.0 }
        end

        def new_result
          { ok: true, summary: new_summary, checks: [] }
        end

        def result_for_check(name:, ok:, severity:, duration_ms:, details:, hint:, doc:, error_class:, error_message:)
          {
            name: name.to_s,
            ok: ok ? true : false,
            severity: severity&.to_sym,
            duration_ms: duration_ms.to_f.round(1),
            details: details,
            hint: hint,
            doc: doc,
            error_class: error_class,
            error_message: error_message && SearchEngine::Observability.truncate_message(error_message, 200)
          }
        end
      end

      # Runner executes all doctor checks and aggregates a summary.
      class Runner
        def initialize(env = ENV)
          @env = env
          @started_ms = monotonic_ms
        end

        def execute
          results = Builder.new_result

          checks = [
            method(:check_config_presence),
            method(:check_connectivity_health),
            method(:check_api_key_validity),
            method(:check_alias_resolution),
            method(:check_dry_run_single),
            method(:check_dry_run_multi),
            method(:check_logging_mode),
            method(:check_opentelemetry)
          ]

          checks.each do |chk|
            res = safely_time(chk)
            bump_summary!(results[:summary], res)
            results[:checks] << res
          end

          results[:ok] = results[:summary][:failed].zero?
          results[:summary][:duration_ms_total] = (monotonic_ms - @started_ms).round(1)

          results
        end

        # --- Flags -----------------------------------------------------------

        def json?
          SearchEngine::CLI::Support.json_output?
        end

        def verbose?
          SearchEngine::CLI.boolean_env?('VERBOSE')
        end

        # --- Checks ----------------------------------------------------------

        def check_config_presence
          started = monotonic_ms
          cfg = SearchEngine.config

          missing = []
          missing << 'host' if cfg.host.to_s.strip.empty?
          missing << 'port' unless cfg.port.is_a?(Integer) && cfg.port.positive?
          missing << 'protocol' unless %w[http https].include?(cfg.protocol.to_s.strip.presence)
          missing << 'api_key' if cfg.api_key.to_s.strip.empty?
          missing << 'timeout_ms' unless cfg.timeout_ms.is_a?(Integer)
          missing << 'open_timeout_ms' unless cfg.open_timeout_ms.is_a?(Integer)

          ok = missing.empty?
          hint = unless ok
                   'Set TYPESENSE_* envs or configure in an initializer. ' \
                   'See https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/installation#configuration'
                 end

          details = cfg.to_h_redacted
          duration = monotonic_ms - started
          Builder.result_for_check(
            name: 'config_presence',
            ok: ok,
            severity: ok ? :info : :error,
            duration_ms: duration,
            details: details,
            hint: hint,
            doc: Doctor.wiki('installation#configuration'),
            error_class: nil,
            error_message: nil
          )
        rescue StandardError => error
          failure(
            'config_presence',
            started,
            error,
            hint: 'Unexpected error reading configuration',
            doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/configuration'
          )
        end

        def check_connectivity_health
          started = monotonic_ms
          client = client_with_overrides
          health = client.health
          ok = !(health && (health[:ok] == true || health['ok'] == true)).nil?
          details = { response: redacted_value(health) }
          hint = ok ? nil : 'Verify host/port/protocol and network reachability to Typesense.'

          Builder.result_for_check(
            name: 'health_check',
            ok: ok,
            severity: ok ? :info : :error,
            duration_ms: monotonic_ms - started,
            details: details,
            hint: hint,
            doc: Doctor.wiki('configuration#typesense-connection'),
            error_class: nil,
            error_message: nil
          )
        rescue SearchEngine::Errors::Error => error
          failure(
            'health_check',
            started,
            error,
            hint: 'Check host/port/protocol and ingress/firewall settings.',
            doc: Doctor.wiki('cli#doctor')
          )
        end

        def check_api_key_validity
          started = monotonic_ms
          client = client_with_overrides
          list = client.list_collections
          count = Array(list).size
          details = { collections_count: count }
          Builder.result_for_check(
            name: 'api_key_valid',
            ok: true,
            severity: :info,
            duration_ms: monotonic_ms - started,
            details: details,
            hint: nil,
            doc: Doctor.wiki('configuration#typesense-connection'),
            error_class: nil,
            error_message: nil
          )
        rescue SearchEngine::Errors::Api => error
          status = error.respond_to?(:status) ? error.status.to_i : nil
          if [401, 403].include?(status)
            Builder.result_for_check(
              name: 'api_key_valid',
              ok: false,
              severity: :error,
              duration_ms: monotonic_ms - started,
              details: { http_status: status },
              hint: 'Your Typesense API key is invalid or lacks permissions. Verify TYPESENSE_API_KEY.',
              doc: Doctor.wiki('configuration#typesense-connection'),
              error_class: error.class.name,
              error_message: error.message
            )
          else
            failure(
              'api_key_valid',
              started,
              error,
              hint: 'Unexpected API error while verifying key.',
              doc: Doctor.wiki('client#errors')
            )
          end
        rescue SearchEngine::Errors::Error => error
          failure(
            'api_key_valid',
            started,
            error,
            hint: 'Connectivity problem while verifying key.',
            doc: Doctor.wiki('client#errors')
          )
        end

        def check_alias_resolution
          started = monotonic_ms
          client = client_with_overrides
          mapping = SearchEngine::Registry.mapping
          if mapping.empty?
            return Builder.result_for_check(
              name: 'alias_check',
              ok: true,
              severity: :info,
              duration_ms: monotonic_ms - started,
              details: { note: 'no registered collections' },
              hint: 'Define at least one model inheriting from SearchEngine::Base and declare collection name.',
              doc: Doctor.wiki('schema'),
              error_class: nil,
              error_message: nil
            )
          end

          missing = []
          resolved = {}
          mapping.each_key do |logical|
            target = client.resolve_alias(logical)
            if target.nil?
              missing << logical
            else
              resolved[logical] = target
            end
          end

          ok = missing.empty?
          hint = if ok
                   nil
                 else
                   'Run schema lifecycle to create physical collections and swap alias. ' \
                   'See https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/schema#lifecycle'
                 end
          details = { resolved: resolved, missing: missing }
          Builder.result_for_check(
            name: 'alias_check',
            ok: ok,
            severity: ok ? :info : :error,
            duration_ms: monotonic_ms - started,
            details: details,
            hint: hint,
            doc: Doctor.wiki('schema#lifecycle'),
            error_class: nil,
            error_message: nil
          )
        rescue SearchEngine::Errors::Error => error
          failure(
            'alias_check',
            started,
            error,
            hint: 'Failed to check aliases due to API/connectivity error.',
            doc: Doctor.wiki('schema')
          )
        end

        def check_dry_run_single
          started = monotonic_ms
          klass = first_registered_class
          if klass.nil?
            return Builder.result_for_check(
              name: 'dry_run_single',
              ok: true,
              severity: :info,
              duration_ms: monotonic_ms - started,
              details: { note: 'no registered collections' },
              hint: 'Add a model inheriting from SearchEngine::Base to preview compile output.',
              doc: Doctor.wiki('dx'),
              error_class: nil,
              error_message: nil
            )
          end

          rel = klass.all
          preview = rel.dry_run!
          details = {
            url: preview[:url],
            url_opts: SearchEngine::Observability.filtered_url_opts(preview[:url_opts] || {}),
            body_preview: SearchEngine::CLI::Support.parse_json_or_string(preview[:body])
          }
          Builder.result_for_check(
            name: 'dry_run_single',
            ok: true,
            severity: :info,
            duration_ms: monotonic_ms - started,
            details: details,
            hint: nil,
            doc: Doctor.wiki('dx'),
            error_class: nil,
            error_message: nil
          )
        rescue StandardError => error
          failure(
            'dry_run_single',
            started,
            error,
            hint: 'Compile failed. Check DSL, selection, and joins configuration.',
            doc: Doctor.wiki('query_dsl')
          )
        end

        def check_dry_run_multi
          started = monotonic_ms
          klass = first_registered_class
          if klass.nil?
            return Builder.result_for_check(
              name: 'dry_run_multi',
              ok: true,
              severity: :info,
              duration_ms: monotonic_ms - started,
              details: { note: 'no registered collections' },
              hint: 'Add at least one model to preview multi-search compile.',
              doc: Doctor.wiki('multi_search'),
              error_class: nil,
              error_message: nil
            )
          end

          m = SearchEngine::Multi.new
          m.add(:a, klass.all)
          m.add(:b, klass.all)
          payloads = m.to_payloads(common: {})
          red = payloads.map { |p| SearchEngine::Observability.redact(p) }
          details = { searches_count: red.size, payloads_preview: red }
          Builder.result_for_check(
            name: 'dry_run_multi',
            ok: true,
            severity: :info,
            duration_ms: monotonic_ms - started,
            details: details,
            hint: nil,
            doc: Doctor.wiki('multi_search'),
            error_class: nil,
            error_message: nil
          )
        rescue StandardError => error
          failure(
            'dry_run_multi',
            started,
            error,
            hint: 'Multi-search compile failed. Verify relations.',
            doc: Doctor.wiki('multi_search')
          )
        end

        def check_logging_mode
          started = monotonic_ms
          log_cfg = SearchEngine.config.logging
          details = {
            mode: (log_cfg.respond_to?(:mode) ? log_cfg.mode : nil),
            level: (log_cfg.respond_to?(:level) ? log_cfg.level : nil),
            sample: (log_cfg.respond_to?(:sample) ? log_cfg.sample : nil),
            logger_present: !(log_cfg.respond_to?(:logger) ? log_cfg.logger : SearchEngine.config.logger).nil?
          }

          Builder.result_for_check(
            name: 'logging_mode',
            ok: true,
            severity: :info,
            duration_ms: monotonic_ms - started,
            details: details,
            hint: nil,
            doc: Doctor.wiki('observability'),
            error_class: nil,
            error_message: nil
          )
        rescue StandardError => error
          failure(
            'logging_mode',
            started,
            error,
            hint: 'Unable to read logging configuration.',
            doc: Doctor.wiki('observability')
          )
        end

        def check_opentelemetry
          started = monotonic_ms
          installed = SearchEngine::OTel.installed?
          enabled = begin
            SearchEngine::OTel.enabled?
          rescue StandardError
            false
          end
          svc = begin
            SearchEngine.config.opentelemetry.service_name
          rescue StandardError
            nil
          end

          if installed && !enabled
            Builder.result_for_check(
              name: 'otel_status',
              ok: true,
              severity: :warning,
              duration_ms: monotonic_ms - started,
              details: { installed: installed, enabled: enabled, service_name: svc },
              hint: 'OpenTelemetry installed but disabled. Enable via ' \
                    'SearchEngine.config.opentelemetry.enabled = true.',
              doc: Doctor.wiki('observability#opentelemetry'),
              error_class: nil,
              error_message: nil
            )
          else
            Builder.result_for_check(
              name: 'otel_status',
              ok: true,
              severity: :info,
              duration_ms: monotonic_ms - started,
              details: { installed: installed, enabled: enabled, service_name: svc },
              hint: nil,
              doc: Doctor.wiki('observability#opentelemetry'),
              error_class: nil,
              error_message: nil
            )
          end
        rescue StandardError => error
          failure(
            'otel_status',
            started,
            error,
            hint: 'Unable to determine OpenTelemetry status.',
            doc: Doctor.wiki('observability')
          )
        end

        # --- Helpers ---------------------------------------------------------

        def client_with_overrides
          base = SearchEngine.config
          cfg = SearchEngine::Config.new
          cfg.host = override_or(base.host, ENV['TYPESENSE_HOST'])
          cfg.port = override_or(base.port, env_int('TYPESENSE_PORT'))
          cfg.protocol = base.protocol.to_s.strip || '' # nil is allowed
          cfg.api_key = base.api_key

          timeout_s = env_int('TYPESENSE_TIMEOUT')
          cfg.timeout_ms = (timeout_s ? Integer(timeout_s) * 1000 : base.timeout_ms)
          cfg.open_timeout_ms = base.open_timeout_ms
          cfg.retries = base.retries
          cfg.logger = base.logger

          SearchEngine.client(config: cfg)
        end

        def env_int(name)
          val = ENV[name]
          return nil if val.nil? || val.to_s.strip.empty?

          Integer(val)
        rescue ArgumentError, TypeError
          nil
        end

        def override_or(base_value, override)
          return base_value if override.nil? || override.to_s.strip.empty?

          override
        end

        def redacted_value(value)
          case value
          when Hash, Array
            SearchEngine::Observability.redact(value)
          else
            value
          end
        end

        def failure(name, started_ms, error, hint:, doc: nil)
          Builder.result_for_check(
            name: name,
            ok: false,
            severity: :error,
            duration_ms: (monotonic_ms - started_ms),
            details: {},
            hint: hint,
            doc: doc,
            error_class: error.class.name,
            error_message: error.message
          )
        end

        def bump_summary!(summary, check)
          if check[:ok]
            if check[:severity] == :warning
              summary[:warned] += 1
            else
              summary[:passed] += 1
            end
          else
            summary[:failed] += 1
          end
          summary[:duration_ms_total] = (summary[:duration_ms_total] + check[:duration_ms].to_f).round(1)
        end

        def first_registered_class
          pair = SearchEngine::Registry.mapping.first
          pair&.last
        end

        def monotonic_ms
          SearchEngine::Instrumentation.monotonic_ms
        end

        # Time a check and handle unexpected errors uniformly.
        def safely_time(callable)
          started = monotonic_ms
          callable.call
        rescue StandardError => error
          puts "error: #{error.backtrace.join("\n")}"
          puts '--------------------------------'
          puts '--------------------------------'
          puts '--------------------------------'
          failure(callable.name, started, error, hint: 'Unexpected error', doc: nil)
        end
      end

      module Renderers
        # Table renderer for human-friendly output of doctor results.
        module Table
          module_function

          def render(result, verbose: false)
            rows = result[:checks].map { |c| to_row(c, verbose: verbose) }
            header = %w[NAME STATUS DURATION HINT DOC]
            "#{render_table([header] + rows)}\n\n#{summary_line(result)}"
          end

          def to_row(check, verbose: false)
            name = check[:name]
            status = status_str(check)
            dur = format('%.1fms', check[:duration_ms].to_f)
            hint = truncate(check[:hint].to_s, verbose: verbose)
            doc = (check[:doc] || '').to_s
            [name, status, dur, hint, doc]
          end

          def status_str(check)
            return 'FAIL' unless check[:ok]
            return 'WARN' if check[:severity] == :warning

            'OK'
          end

          def truncate(text, verbose: false, max: 80)
            return text if verbose || text.length <= max

            "#{text[0, max]}..."
          end

          def render_table(rows)
            widths = column_widths(rows)
            lines = rows.map.with_index do |row, idx|
              line = row.each_with_index.map { |cell, i| pad(cell.to_s, widths[i]) }.join(' | ')
              idx.zero? ? "#{line}\n#{separator(widths)}" : line
            end
            lines.join("\n")
          end

          def column_widths(rows)
            cols = rows.first.size
            (0...cols).map do |i|
              rows.map { |r| r[i].to_s.length }.max
            end
          end

          def pad(str, width)
            str.ljust(width)
          end

          def separator(widths)
            widths.map { |w| '-' * w }.join('-+-')
          end

          def summary_line(result)
            s = result[:summary]
            ok = result[:ok]
            "Summary: passed=#{s[:passed]} warned=#{s[:warned]} failed=#{s[:failed]} " \
              "exit_code=#{ok ? 0 : 1}"
          end
        end
      end
    end
  end
end
