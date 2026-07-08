# frozen_string_literal: true

module SearchEngine
  # Resolver and interface contract for async partition indexing run stores.
  module IndexingRunStore
    # Raised by run stores when a coordinated partition job belongs to a run
    # that should no longer execute.
    class StaleRun < StandardError; end

    module_function

    # Resolve a configured store or the Rails.cache-backed default.
    # @param config [SearchEngine::Config, nil]
    # @return [Object]
    def resolve(config: nil)
      cfg = config || SearchEngine.config
      configured = configured_store(cfg)
      return configured if configured

      default
    end

    # Build the default run store.
    # @return [SearchEngine::IndexingRunStore::RailsCache]
    def default
      require 'search_engine/indexing_run_store/rails_cache'
      SearchEngine::IndexingRunStore::RailsCache.new
    end

    # Validate that an object implements the run-store contract.
    # @param store [Object]
    # @return [Object]
    # @raise [ArgumentError]
    def validate!(store)
      missing = required_methods.reject { |method_name| store.respond_to?(method_name) }
      return store if missing.empty?

      raise ArgumentError, "indexing run store missing methods: #{missing.join(', ')}"
    end

    # @return [Array<Symbol>]
    def required_methods
      %i[create_run mark_started mark_succeeded mark_failed snapshot expire]
    end

    def configured_store(config)
      indexer = config.respond_to?(:indexer) ? config.indexer : nil
      return nil unless indexer.respond_to?(:partition_run_store)

      store = indexer.partition_run_store
      store ? validate!(store) : nil
    end
    private_class_method :configured_store
  end
end
