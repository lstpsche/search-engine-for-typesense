# frozen_string_literal: true

require 'set'
require 'search_engine/config/typesense'
require 'search_engine/config/selection'
require 'search_engine/config/observability'
require 'search_engine/config/presets'
require 'search_engine/config/validators'

module SearchEngine
  # Central configuration container for the engine.
  #
  # Holds connection details, timeouts, retry policy, default search knobs,
  # and caching switches. Mutable by design and safe to reuse across threads
  # via the module-level {SearchEngine.configure} method.
  #
  # All attributes have sensible defaults for development. Values may be
  # hydrated from ENV via {#hydrate_from_env!}. Validation is lightweight and
  # intentionally does not require secrets at boot.
  class Config
    # @!attribute [rw] api_key
    #   @return [String, nil] secret Typesense API key (redacted in logs)
    # @!attribute [rw] host
    #   @return [String] hostname of the Typesense server
    # @!attribute [rw] port
    #   @return [Integer] TCP port for the Typesense server
    # @!attribute [rw] protocol
    #   @return [String] one of "http" or "https"
    # @!attribute [rw] timeout_ms
    #   @return [Integer] request total timeout in milliseconds
    # @!attribute [rw] open_timeout_ms
    #   @return [Integer] connect/open timeout in milliseconds
    # @!attribute [rw] retries
    #   @return [Hash] retry policy with keys { attempts: Integer, backoff: Float or Range<Float> }
    # @!attribute [rw] logger
    #   @return [#info,#warn,#error] logger to use; defaults to Rails.logger
    # @!attribute [rw] default_query_by
    #   @return [String, nil] comma-separated list of fields used to query by
    # @!attribute [rw] default_infix
    #   @return [String] Typesense infix option (e.g., "fallback")
    # @!attribute [rw] use_cache
    #   @return [Boolean] whether to allow URL-level caching
    # @!attribute [rw] cache_ttl_s
    #   @return [Integer] cache TTL in seconds (URL-level only)
    # @!attribute [rw] strict_fields
    #   @return [Boolean] when true, the Parser validates field names/types and raises
    #     friendly errors; when false, unknown fields are allowed (operators and shapes
    #     are still validated). Defaults to true in development/test.
    # @!attribute [rw] multi_search_limit
    #   @return [Integer] maximum number of searches allowed in a single multi-search call (default: 50)
    # @!attribute [rw] test_mode
    #   @return [Boolean] when true, avoid network I/O via an offline client
    # @!attribute [rw] default_console_model
    #   @return [Class, String, nil] default model used by console helpers (SE.q/SE.rel)
    # @!attribute [rw] search_engine_models
    #   @return [String, nil, false] path to host app SearchEngine models directory. May be
    #     relative to `Rails.root` (e.g., "app/search_engine") or absolute. When `nil` or
    #     `false`, gem-managed loading of host SearchEngine models is disabled.
    attr_accessor :logger,
                  :default_query_by,
                  :default_infix,
                  :use_cache,
                  :cache_ttl_s,
                  :strict_fields,
                  :test_mode,
                  :multi_search_limit,
                  :client,
                  :default_console_model,
                  :search_engine_models,
                  :relation_print_materializes

    # Lightweight nested configuration for schema lifecycle.
    class SchemaConfig
      # Retention knobs for physical collections
      class RetentionConfig
        # @return [Integer] how many previous physical collections to keep after swap (default: 0)
        attr_accessor :keep_last

        def initialize
          @keep_last = 0
        end
      end

      # @return [SearchEngine::Config::SchemaConfig::RetentionConfig]
      attr_reader :retention

      def initialize
        @retention = RetentionConfig.new
      end
    end

    # Lightweight nested configuration for indexer/import settings.
    class IndexerConfig
      # @return [Integer] default batch size when not provided explicitly
      attr_accessor :batch_size
      # @return [Integer, nil] optional override for import read timeout (ms)
      attr_accessor :timeout_ms
      # @return [Hash] retry policy: { attempts: Integer, base: Float, max: Float, jitter_fraction: Float }
      attr_accessor :retries
      # @return [Boolean] whether to gzip JSONL payloads (disabled by default)
      attr_accessor :gzip
      # @return [Symbol] dispatcher mode: :active_job or :inline
      attr_accessor :dispatch
      # @return [String] queue name for ActiveJob enqueues
      attr_accessor :queue_name

      def initialize
        @batch_size = 2000
        @timeout_ms = nil
        @retries = { attempts: 3, base: 0.5, max: 5.0, jitter_fraction: 0.2 }
        @gzip = false
        @dispatch = active_job_available? ? :active_job : :inline
        @queue_name = 'search_index'
      end

      private

      def active_job_available?
        defined?(::ActiveJob::Base)
      end
    end

    # Lightweight nested configuration for data source adapters.
    class SourcesConfig
      # Defaults for ActiveRecord-backed source adapter.
      class ActiveRecordConfig
        # @return [Integer] default batch size for ORM batching
        attr_accessor :batch_size
        # @return [Boolean] mark relations as readonly to avoid dirty tracking
        attr_accessor :readonly
        # @return [Boolean] wrap fetching into a read-only transaction (best-effort, off by default)
        attr_accessor :use_transaction

        def initialize
          @batch_size = 2000
          @readonly = true
          @use_transaction = false
        end
      end

      # Defaults for raw SQL streaming source adapter.
      class SQLConfig
        # @return [Integer] default fetch size for server-side cursor/streaming
        attr_accessor :fetch_size
        # @return [Integer, nil] optional per-statement timeout (ms)
        attr_accessor :statement_timeout_ms
        # @return [Symbol] preferred row shape (:auto, :hash)
        attr_accessor :row_shape

        def initialize
          @fetch_size = 2000
          @statement_timeout_ms = nil
          @row_shape = :auto
        end
      end

      # Defaults for lambda-backed source adapter.
      class LambdaConfig
        # @return [Integer, nil] optional hint used for validation/metrics only
        attr_accessor :max_batch_size_hint

        def initialize
          @max_batch_size_hint = nil
        end
      end

      # @return [SearchEngine::Config::SourcesConfig::ActiveRecordConfig]
      def active_record
        @active_record ||= ActiveRecordConfig.new
      end

      # @return [SearchEngine::Config::SourcesConfig::SQLConfig]
      def sql
        @sql ||= SQLConfig.new
      end

      # @return [SearchEngine::Config::SourcesConfig::LambdaConfig]
      def lambda
        @lambda ||= LambdaConfig.new
      end
    end

    # Lightweight nested configuration for mapper.
    class MapperConfig
      # @return [Boolean] when true, unknown keys raise; when false, they are reported as warnings
      attr_accessor :strict_unknown_keys
      # @return [Hash] nested coercions config: { enabled: Boolean, rules: Hash }
      attr_accessor :coercions
      # @return [Integer] maximum number of error samples to include in reports
      attr_accessor :max_error_samples

      def initialize
        @strict_unknown_keys = false
        @coercions = { enabled: false, rules: {} }
        @max_error_samples = 5
      end
    end

    # Lightweight nested configuration for partitioning.
    class PartitioningConfig
      # @return [Proc, nil] optional resolver for default physical collection
      attr_accessor :default_into_resolver
      # @return [Integer, nil] timeout in ms for before hook
      attr_accessor :before_hook_timeout_ms
      # @return [Integer, nil] timeout in ms for after hook
      attr_accessor :after_hook_timeout_ms
      # @return [Integer] maximum error samples to include in payloads
      attr_accessor :max_error_samples

      def initialize
        @default_into_resolver = nil
        @before_hook_timeout_ms = nil
        @after_hook_timeout_ms = nil
        @max_error_samples = 5
      end
    end

    # Lightweight nested configuration for stale deletes.
    class StaleDeletesConfig
      # @return [Boolean] global kill switch
      attr_accessor :enabled
      # @return [Boolean] strict mode blocks suspicious filters
      attr_accessor :strict_mode
      # @return [Integer, nil] timeout in ms for delete requests
      attr_accessor :timeout_ms
      # @return [Boolean] enable found estimation via search
      attr_accessor :estimation_enabled

      def initialize
        @enabled = true
        @strict_mode = false
        @timeout_ms = nil
        @estimation_enabled = false
      end
    end

    # Lightweight nested configuration for observability/logging.
    # Kept for backward compatibility during refactor; delegates to external class.
    #
    # Defaults are quiet by design:
    # - enabled: false (no legacy compact logger unless explicitly turned on)
    # Enable by setting `config.observability.enabled = true` in the initializer.
    class ObservabilityConfig < Observability
      # @return [Boolean] enable the compact logging subscriber automatically
      attr_accessor :enabled
      # @return [Symbol] :kv or :json
      attr_accessor :log_format
      # @return [Integer] maximum message length for error samples in logs
      attr_accessor :max_message_length
      # @return [Boolean] include short error messages in logs for batch/stale events
      attr_accessor :include_error_messages
      # @return [Boolean] also emit legacy event aliases where applicable
      attr_accessor :emit_legacy_event_aliases

      def initialize
        super()

        @enabled = false
        @log_format = :kv
        @max_message_length = 200
        @include_error_messages = false
        @emit_legacy_event_aliases = true
      end
    end

    # Lightweight nested configuration for grouping UX.
    class GroupingConfig
      # @return [Boolean] emit non-fatal warnings for ambiguous combinations
      attr_accessor :warn_on_ambiguous

      def initialize
        @warn_on_ambiguous = true
      end
    end

    # Lightweight nested configuration for selection/hydration.
    # Controls strictness of missing attributes during hydration.
    class SelectionConfig < Selection
      # @return [Boolean] when true, missing requested fields raise MissingField
      attr_accessor :strict_missing

      def initialize
        super()

        @strict_missing = false
      end
    end

    # Lightweight nested configuration for default presets resolution.
    # Controls namespacing and enablement.
    class PresetsConfig < Presets
      class << self
        # Delegate to Presets class methods to preserve existing call sites
        def normalize_enabled(value)
          Presets.normalize_enabled(value)
        end

        def normalize_namespace(value)
          Presets.normalize_namespace(value)
        end
      end
    end

    # Lightweight nested configuration for curation DSL.
    # Controls validation rules and list limits.
    class CurationConfig
      # @return [Integer] maximum number of pinned IDs allowed (default: 50)
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/curation
      attr_accessor :max_pins
      # @return [Integer] maximum number of hidden IDs allowed (default: 200)
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/curation
      attr_accessor :max_hidden
      # @return [Regexp] allowed curated ID pattern (used for IDs and override tags)
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/curation
      attr_accessor :id_regex

      def initialize
        @max_pins = 50
        @max_hidden = 200
        @id_regex = /\A[\w\-:.]+\z/
      end
    end

    # Create a new configuration with defaults, optionally hydrated from ENV.
    #
    # @param env [#[]] environment-like object (defaults to ::ENV)
    def initialize(env = ENV)
      @warned_incomplete = false
      set_defaults!(env)
      hydrate_from_env!(env, override_existing: true)
    end

    # Populate sane defaults for development.
    # @return [void]
    def set_defaults!(env = ENV)
      # typesense transport defaults
      typesense.api_key = nil
      typesense.host = 'localhost'
      typesense.port = 8108
      typesense.protocol = 'http'
      typesense.timeout_ms = 3_600_000
      typesense.open_timeout_ms = 1_000
      typesense.retries = { attempts: 2, backoff: (10.0..60.0) }
      @default_query_by = nil
      @default_infix = 'fallback'
      @use_cache = true
      @cache_ttl_s = 60
      @strict_fields = default_strict_fields
      @test_mode = default_test_mode(env)
      @logger = default_logger
      @multi_search_limit = 50
      @schema = SchemaConfig.new
      @indexer = IndexerConfig.new
      @sources = SourcesConfig.new
      @mapper = MapperConfig.new
      @partitioning = PartitioningConfig.new
      @stale_deletes = StaleDeletesConfig.new
      @observability = ObservabilityConfig.new
      @grouping = GroupingConfig.new
      @selection = SelectionConfig.new
      @presets = PresetsConfig.new
      @curation = CurationConfig.new
      @default_console_model = nil
      # Path may be relative to Rails.root or absolute. Set nil/false to disable.
      @search_engine_models = 'app/search_engine'
      # When true, Relation#inspect/pretty_print materialize a preview (AR-like).
      @relation_print_materializes = true
    end

    # Whether the engine should avoid network I/O and use an offline client.
    # @return [Boolean]
    def test_mode?
      @test_mode ? true : false
    end

    # Expose schema lifecycle configuration.
    # @return [SearchEngine::Config::SchemaConfig]
    def schema
      @schema ||= SchemaConfig.new
    end

    # Expose grouping UX configuration.
    # @return [SearchEngine::Config::GroupingConfig]
    def grouping
      @grouping ||= GroupingConfig.new
    end

    # Expose selection/hydration configuration.
    # @return [SearchEngine::Config::SelectionConfig]
    def selection
      @selection ||= SelectionConfig.new
    end

    # Expose presets configuration.
    # @return [SearchEngine::Config::PresetsConfig]
    # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/presets
    def presets
      @presets ||= PresetsConfig.new
    end

    # Assign presets configuration from a compatible object.
    # Accepts a PresetsConfig, a Hash-like, or an object responding to :namespace and/or :enabled (e.g., OpenStruct).
    # Normalizes values on assignment.
    # @param value [Object]
    # @return [void]
    # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/presets#config-default-preset
    def presets=(value)
      cfg = presets
      if value.is_a?(PresetsConfig)
        @presets = value
        return
      end

      source = if value.respond_to?(:to_h)
                 value.to_h
               else
                 hash = {}
                 hash[:enabled] = value.enabled if value.respond_to?(:enabled)
                 hash[:namespace] = value.namespace if value.respond_to?(:namespace)
                 hash[:locked_domains] = value.locked_domains if value.respond_to?(:locked_domains)
                 hash
               end

      if source.key?(:enabled)
        normalized = PresetsConfig.normalize_enabled(source[:enabled])
        cfg.enabled = normalized
      end

      return unless source.key?(:namespace) || source.key?(:locked_domains)

      cfg.namespace = PresetsConfig.normalize_namespace(source[:namespace]) if source.key?(:namespace)
      cfg.locked_domains = source[:locked_domains] if source.key?(:locked_domains)
    end

    # Expose curation configuration.
    # @return [SearchEngine::Config::CurationConfig]
    def curation
      @curation ||= CurationConfig.new
    end

    # Expose observability/logging configuration.
    # @return [SearchEngine::Config::ObservabilityConfig]
    def observability
      @observability ||= ObservabilityConfig.new
    end

    # Expose partitioning configuration.
    # @return [SearchEngine::Config::PartitioningConfig]
    def partitioning
      @partitioning ||= PartitioningConfig.new
    end

    # Expose stale deletes configuration.
    # @return [SearchEngine::Config::StaleDeletesConfig]
    def stale_deletes
      @stale_deletes ||= StaleDeletesConfig.new
    end

    # Expose structured logging configuration.
    #
    # By default `mode` is nil which disables the structured `LoggingSubscriber`.
    # Opt-in by setting `config.logging.mode = :compact` (or `:json`).
    # @return [OpenStruct]
    def logging
      require 'ostruct'
      @logging ||= OpenStruct.new(mode: nil, level: :info, sample: 1.0, logger: logger)
    end

    # Expose OpenTelemetry configuration. Optional and disabled by default.
    # @return [OpenStruct]
    def opentelemetry
      require 'ostruct'
      @opentelemetry ||= OpenStruct.new(enabled: false, service_name: 'search_engine')
    end

    # Assign OpenTelemetry configuration from a compatible object.
    # Accepts an OpenStruct, a Hash-like, or an object responding to :enabled, :service_name.
    # @param value [Object]
    # @return [void]
    def opentelemetry=(value)
      require 'ostruct'
      if value.is_a?(OpenStruct)
        @opentelemetry = value
        return
      end

      source = if value.respond_to?(:to_h)
                 value.to_h
               else
                 hash = {}
                 hash[:enabled] = value.enabled if value.respond_to?(:enabled)
                 hash[:service_name] = value.service_name if value.respond_to?(:service_name)
                 hash
               end

      otel = opentelemetry
      otel.enabled = (source[:enabled] ? true : false) if source.key?(:enabled)
      return unless source.key?(:service_name)

      otel.service_name = (source[:service_name].to_s.empty? ? 'search_engine' : source[:service_name])
    end

    # Assign curation configuration from a compatible object.
    # Accepts a CurationConfig, a Hash-like, or an object responding to :max_pins, :max_hidden, :id_regex.
    # Validates basic types on assignment.
    # @param value [Object]
    # @return [void]
    def curation=(value)
      cfg = curation
      if value.is_a?(CurationConfig)
        @curation = value
        return
      end

      source = if value.respond_to?(:to_h)
                 value.to_h
               else
                 hash = {}
                 hash[:max_pins] = value.max_pins if value.respond_to?(:max_pins)
                 hash[:max_hidden] = value.max_hidden if value.respond_to?(:max_hidden)
                 hash[:id_regex] = value.id_regex if value.respond_to?(:id_regex)
                 hash
               end

      if source.key?(:max_pins)
        pins = source[:max_pins]
        unless pins.nil? || pins.is_a?(Integer)
          raise ArgumentError, "curation.max_pins must be an Integer (got #{pins.class})"
        end

        cfg.max_pins = pins if pins
      end

      if source.key?(:max_hidden)
        hid = source[:max_hidden]
        unless hid.nil? || hid.is_a?(Integer)
          raise ArgumentError, "curation.max_hidden must be an Integer (got #{hid.class})"
        end

        cfg.max_hidden = hid if hid
      end

      return unless source.key?(:id_regex)

      rx = source[:id_regex]
      raise ArgumentError, "curation.id_regex must be a Regexp (got #{rx.class})" unless rx.is_a?(Regexp)

      cfg.id_regex = rx
    end

    # Expose indexer configuration.
    # @return [SearchEngine::Config::IndexerConfig]
    def indexer
      @indexer ||= IndexerConfig.new
    end

    # Expose sources configuration.
    # @return [SearchEngine::Config::SourcesConfig]
    def sources
      @sources ||= SourcesConfig.new
    end

    # Expose mapper configuration.
    # @return [SearchEngine::Config::MapperConfig]
    def mapper
      @mapper ||= MapperConfig.new
    end

    # Apply ENV values to any attribute, with control over overriding.
    #
    # @param env [#[]] environment-like object
    # @param override_existing [Boolean] when true, overwrite current values
    # @return [self]
    def hydrate_from_env!(env = ENV, override_existing: false)
      set_if_present(:host, env['TYPESENSE_HOST'], override_existing)
      set_if_present(:port, integer_or_nil(env['TYPESENSE_PORT']), override_existing)
      set_if_present(:protocol, env['TYPESENSE_PROTOCOL'], override_existing)
      set_if_present(:api_key, env['TYPESENSE_API_KEY'], override_existing)
      # Accept TYPESENSE_STRICT_FIELDS as 'true'/'false' when provided
      val = env['TYPESENSE_STRICT_FIELDS']
      if !val.nil? && val.is_a?(String) && !val.strip.empty?
        normalized = %w[1 true yes on].include?(val.to_s.strip.downcase)
        set_if_present(:strict_fields, normalized, override_existing)
      end
      test_mode_val = normalize_boolean(env['SEARCH_ENGINE_TEST_MODE'])
      offline_val = normalize_boolean(env['SEARCH_ENGINE_OFFLINE'])
      normalized = test_mode_val.nil? ? offline_val : test_mode_val
      set_if_present(:test_mode, normalized, override_existing) unless normalized.nil?
      self
    end

    # Validate obvious misconfigurations.
    #
    # @raise [ArgumentError] if a field is invalid
    # @return [true]
    def validate!
      errors = []
      errors.concat(SearchEngine::Config::Validators.validate_protocol(protocol))
      errors.concat(SearchEngine::Config::Validators.validate_host(host))
      errors.concat(SearchEngine::Config::Validators.validate_port(port))
      raise ArgumentError, errors.join(', ') unless errors.empty?

      true
    end

    # Log a one-time warning for incomplete non-fatal fields.
    # @return [void]
    def warn_if_incomplete!
      return if @warned_incomplete

      missing = []
      missing << 'api_key' if string_blank?(api_key)
      missing << 'default_query_by' if string_blank?(default_query_by)

      if missing.empty?
        # no-op
      else
        (logger || default_logger).warn(
          "[search_engine] configuration incomplete: missing #{missing.join(', ')}"
        )
      end

      @warned_incomplete = true
      nil
    end

    # Hash representation of the configuration.
    # Secrets are not redacted here.
    # @return [Hash]
    def to_h
      {
        api_key: api_key,
        host: host,
        port: port,
        protocol: protocol,
        timeout_ms: timeout_ms,
        open_timeout_ms: open_timeout_ms,
        retries: retries,
        logger: !logger.nil?,
        default_query_by: default_query_by,
        default_infix: default_infix,
        use_cache: use_cache ? true : false,
        cache_ttl_s: cache_ttl_s,
        strict_fields: strict_fields ? true : false,
        test_mode: test_mode? || false,
        multi_search_limit: multi_search_limit,
        default_console_model: (
          default_console_model.respond_to?(:name) ? default_console_model.name : default_console_model
        ),
        search_engine_models: search_engine_models,
        schema: schema_hash_for_to_h,
        indexer: indexer_hash_for_to_h,
        sources: sources_hash_for_to_h,
        mapper: mapper_hash_for_to_h,
        partitioning: partitioning_hash_for_to_h,
        observability: observability_hash_for_to_h,
        selection: selection_hash_for_to_h,
        presets: presets_hash_for_to_h,
        curation: curation_hash_for_to_h,
        relation_print_materializes: relation_print_materializes ? true : false
      }
    end

    # Hash representation with secrets redacted.
    # @return [Hash]
    def to_h_redacted
      redacted = to_h.dup
      redacted[:api_key] = '[REDACTED]' unless string_blank?(api_key)
      redacted
    end

    private

    def schema_hash_for_to_h
      { retention: { keep_last: schema.retention.keep_last } }
    end

    def indexer_hash_for_to_h
      {
        batch_size: indexer.batch_size,
        timeout_ms: indexer.timeout_ms,
        retries: indexer.retries,
        gzip: indexer.gzip ? true : false,
        dispatch: indexer.dispatch,
        queue_name: indexer.queue_name
      }
    end

    def sources_hash_for_to_h
      {
        active_record: {
          batch_size: sources.active_record.batch_size,
          readonly: sources.active_record.readonly ? true : false,
          use_transaction: sources.active_record.use_transaction ? true : false
        },
        sql: {
          fetch_size: sources.sql.fetch_size,
          statement_timeout_ms: sources.sql.statement_timeout_ms,
          row_shape: sources.sql.row_shape
        },
        lambda: {
          max_batch_size_hint: sources.lambda.max_batch_size_hint
        }
      }
    end

    def mapper_hash_for_to_h
      {
        strict_unknown_keys: mapper.strict_unknown_keys ? true : false,
        coercions: mapper.coercions,
        max_error_samples: mapper.max_error_samples
      }
    end

    def partitioning_hash_for_to_h
      {
        before_hook_timeout_ms: partitioning.before_hook_timeout_ms,
        after_hook_timeout_ms: partitioning.after_hook_timeout_ms,
        max_error_samples: partitioning.max_error_samples
      }
    end

    def observability_hash_for_to_h
      {
        enabled: observability.enabled ? true : false,
        log_format: observability.log_format,
        max_message_length: observability.max_message_length,
        include_error_messages: observability.include_error_messages ? true : false,
        emit_legacy_event_aliases: observability.emit_legacy_event_aliases ? true : false
      }
    end

    def selection_hash_for_to_h
      {
        strict_missing: selection.strict_missing ? true : false
      }
    end

    def presets_hash_for_to_h
      {
        enabled: presets.enabled ? true : false,
        namespace: presets.namespace,
        locked_domains: presets.locked_domains
      }
    end

    def curation_hash_for_to_h
      {
        max_pins: curation.max_pins,
        max_hidden: curation.max_hidden,
        id_regex: curation.id_regex.inspect
      }
    end

    def default_strict_fields
      if defined?(::Rails)
        !::Rails.env.production?
      else
        true
      end
    end

    def default_test_mode(env)
      if defined?(::Rails)
        ::Rails.env.test?
      else
        env_value = env['RACK_ENV'] || env['RAILS_ENV']
        env_value.to_s.strip.downcase == 'test'
      end
    rescue StandardError
      false
    end

    def default_logger
      if defined?(::Rails)
        ::Rails.logger
      else
        require 'logger'
        Logger.new($stdout)
      end
    end

    def integer_or_nil(val)
      return nil if val.nil? || (val.is_a?(String) && val.strip.empty?)

      Integer(val)
    rescue ArgumentError, TypeError
      nil
    end

    def set_if_present(attr, value, override_existing)
      return unless !value.nil? && (override_existing || instance_variable_get(:@warned_incomplete) == false)

      current = public_send(attr)
      return unless override_existing || current.nil? || (current.is_a?(String) && current.strip.empty?)

      public_send("#{attr}=", value)
    end

    def string_blank?(value)
      value.nil? || (value.respond_to?(:strip) && value.strip.empty?)
    end

    def normalize_boolean(value)
      return nil if value.nil?

      str = value.to_s.strip
      return nil if str.empty?

      downcased = str.downcase
      return true if %w[1 true yes on].include?(downcased)
      return false if %w[0 false no off].include?(downcased)

      nil
    end

    def validate_protocol
      SearchEngine::Config::Validators.validate_protocol(protocol)
    end

    def validate_host
      SearchEngine::Config::Validators.validate_host(host)
    end

    def validate_port
      SearchEngine::Config::Validators.validate_port(port)
    end

    def validate_timeouts
      SearchEngine::Config::Validators.validate_timeouts(timeout_ms, open_timeout_ms)
    end

    def validate_retries
      SearchEngine::Config::Validators.validate_retries(retries)
    end

    def retries_valid_shape?
      SearchEngine::Config::Validators.retries_valid_shape?(retries)
    end

    def validate_cache
      SearchEngine::Config::Validators.validate_cache(cache_ttl_s)
    end

    def validate_multi_search_limit
      SearchEngine::Config::Validators.validate_multi_search_limit(multi_search_limit)
    end

    def validate_presets
      SearchEngine::Config::Validators.validate_presets(presets)
    end

    public

    # Typesense transport sub-config and forwarders (public API preserved)
    # @return [SearchEngine::Config::Typesense]
    def typesense
      @typesense ||= SearchEngine::Config::Typesense.new
    end

    def api_key
      typesense.api_key
    end

    def api_key=(value)
      typesense.api_key = value
    end

    def host
      typesense.host
    end

    def host=(value)
      typesense.host = value
    end

    def port
      typesense.port
    end

    def port=(value)
      typesense.port = value
    end

    def protocol
      typesense.protocol
    end

    def protocol=(value)
      typesense.protocol = value
    end

    def timeout_ms
      typesense.timeout_ms
    end

    def timeout_ms=(value)
      typesense.timeout_ms = value
    end

    def open_timeout_ms
      typesense.open_timeout_ms
    end

    def open_timeout_ms=(value)
      typesense.open_timeout_ms = value
    end

    def retries
      typesense.retries
    end

    def retries=(value)
      typesense.retries = value
    end
  end
end
