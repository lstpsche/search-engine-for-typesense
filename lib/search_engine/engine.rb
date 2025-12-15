# frozen_string_literal: true

module SearchEngine
  # Rails engine for the SearchEngine gem.
  # Configures autoloading and eager-loading paths.
  class Engine < ::Rails::Engine
    engine_name 'search_engine'
    # isolate_namespace SearchEngine # enable later if controllers/routes appear

    # Ensure Zeitwerk loads from lib/
    config.autoload_paths << root.join('lib').to_s

    # Ensure app/search_engine is eager-loadable in production
    config.paths.add 'app/search_engine', eager_load: true

    initializer 'search_engine.configuration' do
      cfg = SearchEngine.config
      # Hydrate only blank/unset fields from ENV to avoid clobbering
      # host app overrides. ENV resolution is centralized in Config.
      cfg.hydrate_from_env!(ENV, override_existing: false)
      cfg.warn_if_incomplete!
    end

    # Ignore hyphenated compatibility shim and CLI module so Zeitwerk doesn't try to constantize them.
    # These files are manually required (CLI is required from Rake tasks) and use non-standard naming.
    initializer 'search_engine.zeitwerk_ignores', before: :set_autoload_paths do
      # Rails 6.1+ exposes a loader per engine via `loader`. Guard presence for safety.
      loader = respond_to?(:loader) ? self.loader : nil
      shim = root.join('lib', 'search-engine-for-typesense.rb').to_s
      cli_file = root.join('lib', 'search_engine', 'cli.rb').to_s
      loader&.ignore(shim)
      loader&.ignore(cli_file)

      # Also ensure Rails global autoloaders ignore these files, since the engine
      # adds lib/ to autoload paths and the main/once loaders may scan it.
      if defined?(Rails) && Rails.respond_to?(:autoloaders)
        al = Rails.autoloaders
        al.main.ignore(shim) if al.respond_to?(:main)
        al.once.ignore(shim) if al.respond_to?(:once)
        al.main.ignore(cli_file) if al.respond_to?(:main)
        al.once.ignore(cli_file) if al.respond_to?(:once)
      end
    end

    initializer 'search_engine.observability' do
      cfg = SearchEngine.config

      # Prefer new structured LoggingSubscriber when configured; otherwise
      # fall back to legacy Notifications::CompactLogger gated by cfg.observability.
      begin
        require 'search_engine/logging_subscriber'
      rescue LoadError
        # no-op; allow running without ActiveSupport
      end

      if defined?(SearchEngine::LoggingSubscriber)
        logging_cfg = cfg.respond_to?(:logging) ? cfg.logging : nil
        # Opt-out when mode is nil or sample is explicitly 0.0
        if logging_cfg.respond_to?(:mode) && !logging_cfg.mode.nil?
          sample = logging_cfg.respond_to?(:sample) ? logging_cfg.sample : nil
          if sample.nil? || sample.to_f > 0.0
            SearchEngine::LoggingSubscriber.install!(logging_cfg)
            next
          end
        end
      end

      next unless cfg.observability&.enabled

      # Defer requiring subscriber to runtime to avoid eager load issues
      begin
        require 'search_engine/notifications/compact_logger'
      rescue LoadError
        # no-op; allow running without ActiveSupport
      end

      if defined?(SearchEngine::Notifications::CompactLogger)
        # Subscribe once per boot; store handle in a class ivar in the subscriber
        SearchEngine::Notifications::CompactLogger.subscribe(
          logger: cfg.logger,
          level: :info,
          include_params: false
        )
      end
    end

    initializer 'search_engine.opentelemetry' do
      SearchEngine.config
      begin
        require 'search_engine/otel'
      rescue LoadError
        # no-op; adapter is fully optional
      end

      if defined?(SearchEngine::OTel)
        # Start adapter only when SDK is present and config enables it
        SearchEngine::OTel.start!
      end
    end

    initializer 'search_engine.console_helpers' do
      if defined?(Rails::Console) || $PROGRAM_NAME&.end_with?('console')
        begin
          require 'search_engine/console_helpers'
          SearchEngine::ConsoleHelpers.install!
        rescue LoadError
          # no-op; helpers are optional
        end
      end
    end

    # Manage a dedicated Zeitwerk loader for host app SearchEngine models.
    # Loads after Rails so application models/constants are available.
    initializer 'search_engine.models_loader' do
      # Resolve configured path; allow disabling via nil/false/empty.
      cfg = SearchEngine.config
      models_path_value = cfg.respond_to?(:search_engine_models) ? cfg.search_engine_models : nil
      next if models_path_value.nil? || models_path_value == false || models_path_value.to_s.strip.empty?

      require 'pathname'
      path = Pathname.new(models_path_value.to_s)
      path = Rails.root.join(path) unless path.absolute?
      path_s = path.to_s
      next unless File.directory?(path_s)

      # Ensure Rails' autoloaders do not also manage this directory.
      if defined?(Rails) && Rails.respond_to?(:autoloaders)
        al = Rails.autoloaders
        %i[main once].each do |key|
          al.public_send(key).ignore(path_s) if al.respond_to?(key)
        end
      end

      # Create or reuse a dedicated loader under SearchEngine namespace.
      loader = SearchEngine.instance_variable_get(:@_models_loader)
      unless loader
        loader = Zeitwerk::Loader.new
        loader.tag = 'search_engine.models'
        # Reuse Rails' inflector for consistent constantization rules.
        if defined?(Rails) && Rails.respond_to?(:autoloaders) && Rails.autoloaders.respond_to?(:main)
          loader.inflector = Rails.autoloaders.main.inflector
        end
        loader.push_dir(path_s, namespace: SearchEngine)
        loader.enable_reloading if defined?(Rails) && Rails.env.development?
        SearchEngine.instance_variable_set(:@_models_loader, loader)
      end

      # Setup immediately so host to_prepare callbacks that run early can rely on
      # SearchEngine models being available.
      unless SearchEngine.instance_variable_defined?(:@_models_loader_setup)
        loader.setup
        SearchEngine.instance_variable_set(:@_models_loader_setup, true)
      end

      # Set up and coordinate with Rails reloader lifecycle.
      config.to_prepare do
        l = SearchEngine.instance_variable_get(:@_models_loader)
        next unless l

        l.reload if defined?(Rails) && Rails.env.development?
        # Always eager-load configured SearchEngine models so their `collection` calls
        # register mappings at boot, regardless of Rails.eager_load setting.
        l.eager_load
      end
    end
  end
end
