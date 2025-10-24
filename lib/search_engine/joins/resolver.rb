# frozen_string_literal: true

module SearchEngine
  module Joins
    # Key resolver for join associations.
    #
    # Determines { local_key, foreign_key } to use for an association, honoring
    # explicitly provided keys and auto-inferring when absent. Auto-inference
    # prefers a single shared attribute name between the base and target models
    # that ends with `_id`. When ambiguity is detected, an error is raised with
    # short suggestions.
    module Resolver
      module_function

      # Resolve join keys for an association.
      #
      # @param base_klass [Class] the model on which the join is declared
      # @param assoc_cfg [Hash] normalized join config (from joins_config)
      # @return [Hash] { local_key: Symbol, foreign_key: Symbol }
      # @raise [SearchEngine::Errors::InvalidJoinConfig]
      def resolve_keys(base_klass, assoc_cfg)
        local_key = assoc_cfg[:local_key]
        foreign_key = assoc_cfg[:foreign_key]

        if present?(local_key) && present?(foreign_key)
          validate_key_known!(base_klass, local_key, side: :local)
          validate_foreign_key_known!(assoc_cfg, foreign_key)
          return { local_key: local_key.to_sym, foreign_key: foreign_key.to_sym }
        end

        # Infer when any of the keys is missing
        base_attrs = safe_attributes(base_klass).keys.map(&:to_s)
        target_klass = safe_target_klass(assoc_cfg[:collection])
        target_attrs = safe_attributes(target_klass).keys.map(&:to_s)

        shared = (base_attrs & target_attrs)
        candidates = shared.select { |n| n.end_with?('_id') }
        candidates = shared if candidates.empty?

        raise_ambiguous_keys!(base_klass, assoc_cfg, candidates) if candidates.length != 1

        key = candidates.first.to_sym
        { local_key: key, foreign_key: key }
      end

      # --- internals --------------------------------------------------------

      def validate_key_known!(klass, key, side:)
        attrs = safe_attributes(klass)
        return if key.to_sym == :id || attrs.key?(key.to_sym)

        model_name = klass.respond_to?(:name) && klass.name ? klass.name : klass.to_s
        raise SearchEngine::Errors::InvalidJoin.new(
          "Unknown #{side} key :#{key} for #{model_name}. Declare it via `attribute :#{key}, ...`.",
          doc: 'docs/joins.md#troubleshooting',
          details: { side: side, key: key, model: model_name }
        )
      end

      def validate_foreign_key_known!(assoc_cfg, key)
        tklass = safe_target_klass(assoc_cfg[:collection])
        validate_key_known!(tklass, key, side: :foreign)
      end

      def safe_target_klass(collection_name)
        SearchEngine.collection_for(collection_name)
      rescue StandardError
        nil
      end

      def safe_attributes(klass)
        if klass && klass.respond_to?(:attributes)
          klass.attributes || {}
        else
          {}
        end
      end

      def raise_ambiguous_keys!(base_klass, assoc_cfg, candidates)
        base_name = base_klass.respond_to?(:name) && base_klass.name ? base_klass.name : base_klass.to_s
        assoc_name = assoc_cfg[:name] || assoc_cfg[:collection] || :unknown
        sugg = candidates.map { |n| ":#{n}" }.join(', ')
        msg = "Ambiguous join keys for :#{assoc_name} on #{base_name}. " \
              "Could not infer a unique shared key. Candidates: #{sugg}"
        raise SearchEngine::Errors::InvalidJoinConfig.new(
          msg,
          doc: 'docs/joins.md#client-side-fallback',
          details: { assoc: assoc_name, candidates: candidates }
        )
      end

      def present?(v)
        !(v.nil? || v.to_s.strip.empty?)
      end
    end
  end
end
