# frozen_string_literal: true

require 'active_support/inflector'

# Internal SearchEngine registry and APIs for model mapping.
module SearchEngine
  # Internal registry for mapping Typesense collection names to model classes.
  #
  # Exposes stable module-level APIs on {SearchEngine}:
  # - {SearchEngine.register_collection!}
  # - {SearchEngine.collection_for}
  #
  # The registry uses a copy-on-write Hash guarded by a small Mutex. Reads are
  # lock-free and writes are atomic. This makes it safe under concurrency and
  # friendly to Rails code reloading in development.
  module Registry
    class << self
      # @return [Hash{String=>Class}] frozen snapshot of the registry
      def mapping
        @mapping ||= {}.freeze
      end

      # @return [Mutex] global write mutex for registry updates
      def mutex
        @mutex ||= Mutex.new
      end

      # Replace the current mapping with a new frozen Hash.
      # @param new_map [Hash{String=>Class}]
      # @return [void]
      def replace!(new_map)
        @mapping = new_map.freeze
        nil
      end

      # Reset to an empty mapping (used by tests).
      # @api private
      # @return [void]
      def __reset_for_tests!
        mutex.synchronize { replace!({}) }
      end
    end
  end
end

# Top-level APIs for configuration and model registry.
module SearchEngine
  class << self
    # Register a model class for a given Typesense collection name.
    #
    # Idempotent when re-registering with the same class or with a class that
    # has the same name (to support Rails code reloading). If an existing
    # mapping is present for the collection name and points to a different
    # class (by class name), an ArgumentError is raised.
    #
    # @param name [#to_s] Typesense collection name
    # @param klass [Class] model class to associate
    # @return [Class] the registered class
    # @raise [ArgumentError] when attempting to change an existing mapping to a different class
    def register_collection!(name, klass)
      normalized_name = name.to_s
      raise ArgumentError, 'collection name must be non-empty' if normalized_name.strip.empty?
      raise ArgumentError, 'klass must be a Class' unless klass.is_a?(Class)

      Registry.mutex.synchronize do
        current = Registry.mapping[normalized_name]

        return write_mapping!(normalized_name, klass) if mapping_idempotent?(current, klass)

        raise_conflict_if_needed!(normalized_name, current, klass)
        write_mapping!(normalized_name, klass)
      end
    end

    # Resolve the model class for a given Typesense collection name.
    #
    # @param name [#to_s]
    # @yield when no model is registered after autoload
    # @return [Class, Object] registered model or the value of the supplied block
    # @raise [ArgumentError] when the collection is not registered and no block is supplied
    def collection_for(name)
      normalized_name = name.to_s
      klass = Registry.mapping[normalized_name]
      if klass
        # A class can call `collection` before the rest of its file has loaded.
        # Let Ruby finish (or retry) its autoload before using its identity strategy.
        load_model_constant(Object, klass.name) if klass.name
      else
        load_model_constant(self, ActiveSupport::Inflector.classify(normalized_name))
      end
      klass = Registry.mapping[normalized_name]
      return klass if klass
      return yield if block_given?

      message = 'Unregistered collection: ' \
                "'#{normalized_name}'. " \
                'Define a model inheriting from SearchEngine::Base and call ' \
                "`collection '#{normalized_name}'`."
      raise ArgumentError, message
    end

    private

    def load_model_constant(namespace, name)
      name.split('::').each do |part|
        break unless namespace.is_a?(Module) && collection_constant_defined?(namespace, part)

        namespace = namespace.const_get(part, false)
      end
    end

    def collection_constant_defined?(namespace, part)
      namespace.const_defined?(part, false)
    rescue NameError
      # Physical collection names can contain characters that cannot appear in Ruby constants.
      false
    end

    def mapping_idempotent?(current, new_klass)
      return false unless current

      current == new_klass || safe_class_name(current) == safe_class_name(new_klass)
    end

    def raise_conflict_if_needed!(name, current, new_klass)
      return unless current

      old_name = safe_class_name(current)
      new_name = safe_class_name(new_klass)
      return if old_name == new_name

      message = "Collection '#{name}' already registered to #{old_name}; " \
                "cannot re-register to #{new_name}"
      raise ArgumentError, message
    end

    def write_mapping!(name, klass)
      new_map = Registry.mapping.dup
      new_map[name] = klass
      Registry.replace!(new_map)
      klass
    end

    def safe_class_name(klass)
      klass.respond_to?(:name) && klass.name ? klass.name : klass.to_s
    end

    # Clear the registry (intended for test suites).
    # @api private
    # @return [void]
    def __reset_registry_for_tests!
      Registry.__reset_for_tests!
    end

    private :__reset_registry_for_tests!
  end
end
