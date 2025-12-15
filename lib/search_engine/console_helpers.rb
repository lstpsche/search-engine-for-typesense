# frozen_string_literal: true

begin
  require 'active_support/inflector'
rescue LoadError
  # ActiveSupport may not be available outside Rails; constant lookup will fallback
end

module SearchEngine
  # Console-only helpers and installer for the `SE` top-level shortcut.
  #
  # In Rails console, the engine installs `::SE` so you can quickly build and
  # run queries interactively.
  module ConsoleHelpers
    # Install the top-level `SE` constant unless already defined.
    # @return [void]
    def self.install!
      return if Object.const_defined?(:SE)

      Object.const_set(:SE, HelpersModule)
      nil
    end

    # Internal module backing the `SE` constant.
    module HelpersModule
      module_function

      # Run a simple search on a default model with optional overrides.
      #
      # Default model resolution:
      # - Prefer `SearchEngine.config.default_console_model` (Class or String)
      # - Fallback to the sole registered model in the collection registry
      # - Raise a helpful error if ambiguous or none are found
      #
      # @param query [String, nil]
      # @param opts [Hash] common options (e.g., select:, per:, page:, where:, query_by:, ...)
      # @return [SearchEngine::Relation]
      # @example
      #   SE.q('milk').per(5)
      #   SE.q.where(category: 'dairy')
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/dx
      def q(query = nil, **opts)
        model = default_model!
        rel = model.all
        rel = rel.options(q: query) unless query.nil?

        select_opt = opts.delete(:select)
        rel = rel.select(*Array(select_opt)) if select_opt

        where_opt = opts.delete(:where)
        rel = rel.where(where_opt) if where_opt

        per_opt = opts.delete(:per) || opts.delete(:per_page)
        rel = rel.per(per_opt) if per_opt

        page_opt = opts.delete(:page)
        rel = rel.page(page_opt) if page_opt

        # Pass remaining options (e.g., query_by:, preset:, grouping)
        rel.options(opts)
      end

      # Multi-search convenience wrapper that delegates to SearchEngine.multi_search.
      # @param common [Hash]
      # @yieldparam m [SearchEngine::Multi]
      # @return [SearchEngine::Multi::ResultSet]
      # @example
      #   SE.ms { |m| m.add :products, SE.q('milk').per(5) }
      def ms(common: {}, &block)
        SearchEngine.multi_search(common: common, &block)
      end

      # Return a base relation for the default (or provided) model.
      # @param model [Class, nil]
      # @return [SearchEngine::Relation]
      def rel(model = nil)
        (model || default_model!).all
      end

      # Resolve the default model, honoring configuration and registry.
      # @return [Class]
      # @raise [ArgumentError] with hint and docs link when ambiguous or missing
      def default_model!
        cfg = SearchEngine.config
        if cfg.respond_to?(:default_console_model) && cfg.default_console_model
          return resolve_model_class(cfg.default_console_model)
        end

        mapping = SearchEngine::Registry.mapping
        if mapping.empty?
          raise ArgumentError,
                'No default model configured. Set SearchEngine.config.default_console_model ' \
                'or define a single SearchEngine::Base model. See ' \
                'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/dx#generators--console-helpers.'
        end

        uniq_klasses = mapping.values.uniq
        return uniq_klasses.first if uniq_klasses.size == 1

        names = uniq_klasses.map { |k| k.respond_to?(:name) && k.name ? k.name : k.to_s }.sort
        raise ArgumentError,
              "Ambiguous default model: #{names.join(', ')}. Set SearchEngine.config.default_console_model. " \
              'See https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/dx#generators--console-helpers.'
      end

      def resolve_model_class(value)
        return value if value.is_a?(Class) && value < SearchEngine::Base

        name =
          case value
          when Symbol then value.to_s
          when String then value
          else value.to_s
          end

        if defined?(ActiveSupport::Inflector)
          Object.const_get(name)
        else
          name.split('::').reduce(Object) { |mod, part| mod.const_get(part) }
        end
      rescue NameError
        raise ArgumentError,
              "Unknown model constant #{name.inspect} for default_console_model. Ensure it's loaded. " \
              'See https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/dx#generators--console-helpers.'
      end

      private_class_method :resolve_model_class
    end
  end
end
