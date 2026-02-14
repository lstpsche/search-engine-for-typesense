# frozen_string_literal: true

module SearchEngine
  module Joins
    # Stateless guard for validating join usage across Relation and Parser.
    #
    # Public API is module functions that raise {SearchEngine::Errors::*}
    # on invalid inputs; successful validations return nil.
    module Guard
      module_function

      # Ensure the association exists on +klass+.
      #
      # @param klass [Class] model class
      # @param assoc [Symbol, String]
      # @raise [SearchEngine::Errors::InvalidJoin]
      # @return [void]
      def ensure_assoc_exists!(klass, assoc)
        key = assoc.to_sym
        cfg = safe_joins_config(klass)[key]
        return if cfg

        suggestions = suggest(key, safe_joins_config(klass).keys)
        model_name = safe_class_name(klass)
        msg = "association :#{key} is not declared on #{model_name} "
        msg += "(declare with `join :#{key}, ...`)"
        msg += suggestion_suffix(suggestions)
        raise SearchEngine::Errors::InvalidJoin.new(
          msg,
          hint: (suggestions&.any? ? "Did you mean #{suggestions.map { |s| ":#{s}" }.join(', ')}?" : nil),
          doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/joins#troubleshooting',
          details: { assoc: key, known: safe_joins_config(klass).keys }
        )
      end

      # Ensure the association config is complete (local_key and foreign_key present and non-blank).
      #
      # @param klass [Class]
      # @param assoc [Symbol, String]
      # @raise [SearchEngine::Errors::InvalidJoinConfig]
      # @return [void]
      def ensure_config_complete!(klass, assoc)
        key = assoc.to_sym
        cfg = safe_joins_config(klass)[key]
        # If missing entirely, surface presence error via ensure_assoc_exists!
        unless cfg
          ensure_assoc_exists!(klass, key)
          return
        end

        missing = []
        missing << :local_key if blank?(cfg[:local_key])
        missing << :foreign_key if blank?(cfg[:foreign_key])
        return if missing.empty?

        model_name = safe_class_name(klass)
        msg = "join :#{key} on #{model_name} is missing "
        msg += missing.map { |m| ":#{m}" }.join(' and ')
        msg += ' (declare with `join '
        msg += ":#{key}, collection: ..., local_key: ..., foreign_key: ...`)"
        raise SearchEngine::Errors::InvalidJoinConfig.new(
          msg,
          hint: 'Declare local_key and foreign_key in join config.',
          doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/joins#troubleshooting',
          details: { assoc: key, missing: missing }
        )
      end

      # Ensure the relation has applied the association via .joins(:assoc) before use.
      #
      # @param joins [Array<Symbol>, nil]
      # @param assoc [Symbol, String]
      # @param context [String] optional human-friendly action (e.g., "filtering/sorting")
      # @raise [SearchEngine::Errors::JoinNotApplied]
      # @return [void]
      def ensure_join_applied!(joins, assoc, context: 'filtering/sorting')
        key = assoc.to_sym
        list = Array(joins)
        return if list.include?(key)

        raise SearchEngine::Errors::JoinNotApplied.new(
          "Call .joins(:#{key}) before #{context} on #{key} fields",
          doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/joins#troubleshooting',
          details: { assoc: key, context: context }
        )
      end

      # Validate that a joined field exists on the target collection when the
      # target model is registered and exposes attributes.
      #
      # Best-effort: when registry or attributes are unavailable, no exception
      # is raised (prefer low-noise behavior on missing metadata).
      #
      # @param assoc_cfg [Hash] normalized association config
      # @param field [Symbol, String]
      # @param source_klass [Class, nil] optional source model class for error messages
      # @raise [SearchEngine::Errors::UnknownJoinField]
      # @return [void]
      def validate_joined_field!(assoc_cfg, field, source_klass: nil)
        return if assoc_cfg.nil?

        collection = assoc_cfg[:collection]
        return if blank?(collection)

        target_klass = begin
          SearchEngine.collection_for(collection)
        rescue StandardError
          nil
        end
        return unless target_klass.respond_to?(:attributes)

        known = Array(target_klass.attributes).map { |k, _| k.to_s }
        known |= ['id'] # allow id implicitly on joined collections
        return if known.empty?

        fname = field.to_s
        return if known.include?(fname)

        suggestions = suggest(fname, known)
        # Prefer the source model name for message clarity when provided
        model_name = safe_class_name(source_klass || target_klass)
        assoc_name = assoc_cfg[:name] || begin
          # best effort: derive from collection
          collection.to_s
        end
        msg = "UnknownJoinField: :#{fname} is not declared on association :#{assoc_name} for #{model_name}"
        msg += suggestion_suffix(suggestions)
        raise SearchEngine::Errors::UnknownJoinField.new(
          msg,
          details: { assoc: assoc_name, field: fname }
        )
      end

      # Reject multi-hop paths like $authors.publisher.name
      #
      # @param path [String]
      # @raise [SearchEngine::Errors::UnsupportedJoinNesting]
      # @return [void]
      def ensure_single_hop_path!(path)
        return if path.to_s.count('.') <= 1

        raise SearchEngine::Errors::UnsupportedJoinNesting.new(
          'Only one join hop is supported: `$assoc.field`. Use a separate pipeline step to denormalize deeper paths.',
          doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/joins#troubleshooting',
          details: { path: path }
        )
      end

      # --- internals -------------------------------------------------------

      def suggest(input, known)
        return [] if known.nil? || known.empty?

        begin
          require 'did_you_mean'
          require 'did_you_mean/levenshtein'
        rescue StandardError
          return []
        end

        candidates = Array(known).map(&:to_s)
        str = input.to_s
        distances = candidates.each_with_object({}) do |cand, acc|
          acc[cand] = DidYouMean::Levenshtein.distance(str, cand)
        end
        min = distances.values.min
        return [] if min.nil? || min > 2

        best = distances.select { |_, d| d == min }.keys.sort
        best.first(3).map(&:to_sym)
      end
      private_class_method :suggest

      def suggestion_suffix(suggestions)
        return '' if suggestions.nil? || suggestions.empty?

        tail = suggestions.map { |s| ":#{s}" }.join(', ')
        " (did you mean #{tail}?)"
      end
      private_class_method :suggestion_suffix

      def safe_joins_config(klass)
        if klass.respond_to?(:joins_config)
          klass.joins_config || {}
        else
          {}
        end
      end
      private_class_method :safe_joins_config

      def safe_class_name(klass)
        klass.respond_to?(:name) && klass.name ? klass.name : klass.to_s
      end
      private_class_method :safe_class_name

      def blank?(value)
        value.nil? || value.to_s.strip.empty?
      end
      private_class_method :blank?
    end
  end
end
