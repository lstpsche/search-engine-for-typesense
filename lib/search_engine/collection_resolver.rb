# frozen_string_literal: true

require 'search_engine/registry'
require 'search_engine/cascade'

module SearchEngine
  # Helper utilities to resolve collection models from Typesense collection names.
  #
  # Public API:
  # - {.model_for_physical(physical, client: nil)} => Class or nil
  # - {.model_for_logical(logical)} => Class or nil
  # - {.physicals_for_logical(client, logical)} => Array<[String, Integer]>
  module CollectionResolver
    class << self
      # Build a map of logical names => model classes by merging registry with
      # a scan of the SearchEngine namespace for subclasses of Base.
      # @return [Hash{String=>Class}]
      def models_map
        map = {}
        reg = SearchEngine::Registry.mapping
        reg.each { |k, v| map[k.to_s] = v } if reg && !reg.empty?

        # Walk the SearchEngine namespace to find Base descendants
        begin
          SearchEngine.constants.each do |c|
            const = SearchEngine.const_get(c)
            next unless const.is_a?(Class)
            next unless const.ancestors.include?(SearchEngine::Base)

            logical = if const.respond_to?(:collection)
                        const.collection.to_s
                      else
                        demod = const.name.split('::').last
                        demod.respond_to?(:underscore) ? demod.underscore.pluralize : "#{demod.downcase}s"
                      end
            map[logical] ||= const
          end
        rescue StandardError
          # best-effort; namespace may not be fully loaded
        end

        map
      end

      # Resolve a model class for a physical Typesense collection name.
      # Tries, in order: normalized base logical, reverse alias lookup, and returns nil when not found.
      # @param physical [#to_s]
      # @param client [SearchEngine::Client, nil]
      # @return [Class, nil]
      def model_for_physical(physical, client: nil)
        phys = physical.to_s
        base = logical_from_physical(phys)

        # Prefer models_map first to handle classes that don't invoke collection macro yet
        mm = models_map
        klass = mm[base]
        klass ||= model_for_logical(base)
        return klass if klass

        # Reverse alias mapping: find a registered logical whose alias targets this physical
        reg = SearchEngine::Registry.mapping
        return nil if reg.nil? || reg.empty?

        cli = client || SearchEngine.client
        reg.each_key do |logical|
          target = cli.resolve_alias(logical)
          return reg[logical] if target && target.to_s == phys
        rescue StandardError
          # ignore and continue
        end

        nil
      end

      # Resolve a model class for a logical collection name using registry, falling
      # back to autoloading a namespaced model constant.
      # @param logical [#to_s]
      # @return [Class, nil]
      def model_for_logical(logical)
        name = logical.to_s

        mm = models_map
        return mm[name] if mm.key?(name)

        begin
          return SearchEngine.collection_for(name)
        rescue StandardError
          # fall through
        end

        # Heuristic: SearchEngine::<Classify(name)>
        m = classify_model(name)
        return m if m

        # Heuristic: nested modules e.g. foo_bar_baz -> SearchEngine::FooBar::Baz
        parts = name.split('_')
        if parts.size >= 2
          last = parts.pop
          mod = parts.map { |p| camelize(p) }.join
          candidate = "SearchEngine::#{mod}::#{camelize(last)}"
          begin
            const = Object.const_get(candidate)
            return const if const.is_a?(Class) && const.ancestors.include?(SearchEngine::Base)
          rescue StandardError
            # ignore
          end
        end

        nil
      end

      # Convert physical name into a logical base by stripping timestamp suffix when present.
      # @param name [#to_s]
      # @return [String]
      def logical_from_physical(name)
        s = name.to_s
        begin
          out = SearchEngine::Cascade.normalize_physical_to_logical(s)
          return out if out && !out.to_s.empty? && out.to_s != s
        rescue StandardError
          # fall through to regex fallback
        end

        # Regex fallback independent of Cascade implementation
        m = s.match(/\A(.+)_\d{8}_\d{6}_\d{3}\z/)
        return m[1].to_s if m && m[1]

        s
      end

      # List physical collections associated with a logical alias.
      # Prefer the alias target when present; otherwise scan server collections and
      # group by normalized base.
      # @param client [SearchEngine::Client]
      # @param logical [#to_s]
      # @return [Array<Array(String, Integer)>]
      def physicals_for_logical(client, logical)
        list = Array(client.list_collections)
        pairs = list.map do |h|
          name = (h[:name] || h['name']).to_s
          num = (h[:num_documents] || h['num_documents']).to_i
          [name, num]
        end

        # Filter all physicals that normalize to the logical name
        filtered = pairs.select do |(physical, _num)|
          logical_from_physical(physical).to_s == logical.to_s
        end

        # If no filtered physicals (unexpected), fallback to alias target if present
        begin
          aliased = client.resolve_alias(logical)
          if filtered.empty? && aliased && !aliased.to_s.strip.empty?
            # Retrieve live schema to confirm presence; if present, synthesize with count 0
            schema = client.retrieve_collection_schema(aliased)
            return [[aliased.to_s, (schema && (schema[:num_documents] || schema['num_documents'])).to_i]]
          end
        rescue StandardError
          # ignore alias/schema issues
        end

        filtered
      end

      private

      def classify_model(name)
        if defined?(ActiveSupport::Inflector)
          klass_name = ActiveSupport::Inflector.classify(name.to_s)
          const = Object.const_get("SearchEngine::#{klass_name}")
        else
          base = name.to_s.split('_').map { |s| s[0].upcase + s[1..] }.join
          const = Object.const_get("SearchEngine::#{base}")
        end
        return const if const.is_a?(Class) && const.ancestors.include?(SearchEngine::Base)

        nil
      rescue StandardError
        nil
      end

      def camelize(token)
        if defined?(ActiveSupport::Inflector)
          ActiveSupport::Inflector.camelize(token.to_s)
        else
          token.to_s.split('_').map { |s| s[0].upcase + s[1..] }.join
        end
      end
    end
  end
end
