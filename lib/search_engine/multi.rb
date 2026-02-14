# frozen_string_literal: true

require 'set'

module SearchEngine
  # Builder for assembling labeled Relations into a federated multi-search.
  #
  # Pure collector: validates labels and relations, preserves insertion order,
  # and produces per-search payload hashes ready for {SearchEngine::Client#multi_search}.
  #
  # Usage (via the module-level convenience):
  #   SearchEngine.multi_search(common: { query_by: SearchEngine.config.default_query_by }) do |m|
  #     m.add :products, Product.all.where(active: true).per(10)
  #     m.add :brands,   Brand.all.where('name:~rud').per(5)
  #   end
  class Multi
    # Lightweight internal entry
    Entry = Struct.new(:label, :key, :relation, :api_key, keyword_init: true)

    # URL-level options that must never appear in request bodies.
    URL_ONLY_KEYS = %i[use_cache cache_ttl].freeze

    # Keys that must be present/retained for :only mode in multi-search payloads.
    ESSENTIAL_MULTI_KEYS = %i[collection q page per_page preset].freeze

    # Canonicalize a label into a case-insensitive Symbol key.
    # @param label [String, Symbol]
    # @return [Symbol]
    # @raise [ArgumentError] when label is invalid
    def self.canonicalize_label(label)
      raise ArgumentError, 'label must be a Symbol or String' unless label.is_a?(String) || label.is_a?(Symbol)

      s = label.to_s.strip
      raise ArgumentError, 'label cannot be blank' if s.empty?

      s.downcase.to_sym
    end

    # Create a new Multi builder.
    # @return [void]
    def initialize
      @entries = []
      @keys = Set.new
    end

    # Add a labeled search relation.
    #
    # @param label [String, Symbol] unique label (case-insensitive)
    # @param relation [Object] object responding to +to_typesense_params+ and +klass+
    # @param api_key [String, nil] per-search API key (unsupported; see below)
    # @return [self]
    # @raise [ArgumentError] when label is duplicate/invalid, relation is invalid, or api_key is provided
    # @note Per-search api_key is not supported by the underlying Typesense client and will raise.
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/multi-search-guide`
    def add(label, relation, api_key: nil)
      key = Multi.canonicalize_label(label)
      raise ArgumentError, "Multi#add: duplicate label #{label.inspect} (labels must be unique)." if @keys.include?(key)

      validate_relation!(relation)
      if api_key
        raise ArgumentError,
              'Per-search api_key is not supported by the Typesense multi-search API; ' \
              'set a global API key in SearchEngine.config instead.'
      end

      @entries << Entry.new(label: label, key: key, relation: relation, api_key: nil)
      @keys << key
      self
    end

    # Ordered list of canonical labels (Symbols).
    # @return [Array<Symbol>]
    def labels
      @entries.map(&:key)
    end

    # Build per-search payloads (order preserved), merging common params.
    #
    # For each entry, the payload is:
    #   { collection: <String>, **common_filtered, **per_search_params_filtered }
    # Per-search values win over +common+ on shallow merge.
    # URL-only options (:use_cache, :cache_ttl) are filtered from both sources.
    # Empty values are omitted for cleanliness.
    #
    # Curation:
    # - Reuses Relation#to_typesense_params to inject per-entry curation keys
    # - Body-only: curation keys never appear in URL opts or top-level
    # - Deterministic: preserves first-occurrence order for pinned IDs
    #
    # Presets:
    # - Preset token lives inside each per-search payload under :preset (never top-level)
    # - mode=:merge — pass through compiler output (includes :preset)
    # - mode=:only  — retain only ESSENTIAL_MULTI_KEYS (collection,q,page,per_page,preset) after common merge
    # - mode=:lock  — defensively drop any keys present in Config.presets.locked_domains from the merged payload
    #                 (conflict recording remains in single-search compile)
    #
    # @param common [Hash] optional parameters applied to each per-search payload
    # @return [Array<Hash>] array of request bodies suitable for Client#multi_search
    # @raise [ArgumentError] when +common+ is not a Hash, when duplicate labels are detected,
    #   or when a relation is invalid / lacks a bound collection
    # @example
    #   m = SearchEngine::Multi.new
    #   m.add(:products, Product.all.per(10))
    #   m.to_payloads(common: { query_by: SearchEngine.config.default_query_by })
    #   # => [{ collection: "products", q: "*", query_by: "name", per_page: 10 }]
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/multi-search-guide`
    def to_payloads(common: {})
      raise ArgumentError, 'common must be a Hash' unless common.is_a?(Hash)

      filtered_common = filter_url_only_keys(common)

      seen = Set.new
      @entries.map do |e|
        # Guard against external mutation that might have introduced duplicates
        raise ArgumentError, "duplicate label: #{e.label.inspect}" if seen.include?(e.key)

        seen << e.key

        # Validate relation contract at compile time (in case of external mutation)
        begin
          validate_relation!(e.relation)
        rescue ArgumentError
          raise ArgumentError,
                "invalid relation for label #{e.label.inspect}: expected a Relation with a bound collection"
        end

        per_search = SearchEngine::CompiledParams.from(e.relation.to_typesense_params).to_h
        per_search = filter_url_only_keys(per_search)

        collection = collection_name_for_relation(e.relation)

        # Shallow-merge with per-search winning
        merged = filtered_common.merge(per_search)
        payload = { collection: collection }.merge(EssentialPruner.prune(merged, relation: e.relation))
        prune_empty_values(payload)
      end
    end

    # Ordered list of model classes bound to each entry.
    # @return [Array<Class>]
    def klasses
      @entries.map { |e| e.relation.klass }
    end

    # ResultSet maps labels to {SearchEngine::Result} while preserving order.
    class ResultSet
      # @param pairs [Array<Array(Symbol, SearchEngine::Result)>>] ordered (label, result) pairs
      def initialize(pairs)
        @labels = []
        @map = {}
        pairs.each do |(label, result)|
          key = Multi.canonicalize_label(label)
          @labels << key
          @map[key] = result
        end
        freeze
      end

      # Fetch a result by label (String or Symbol).
      # @param label [String, Symbol]
      # @return [SearchEngine::Result, nil]
      def [](label)
        @map[Multi.canonicalize_label(label)]
      end

      # Alias for {#[]} to support dig-like access.
      # @param label [String, Symbol]
      # @return [SearchEngine::Result, nil]
      def dig(label)
        self[label]
      end

      # Ordered label list (canonical Symbol keys).
      # @return [Array<Symbol>]
      def labels
        @labels.dup
      end

      # Shallow Hash mapping labels to results.
      # @return [Hash{Symbol=>SearchEngine::Result}]
      def to_h
        @map.dup
      end

      # Iterate over (label, result) in order.
      # @yieldparam label [Symbol]
      # @yieldparam result [SearchEngine::Result]
      # @return [Enumerator]
      def each_pair
        return enum_for(:each_pair) unless block_given?

        @labels.each { |l| yield(l, @map[l]) }
      end
    end

    private

    def validate_relation!(relation)
      unless relation.respond_to?(:to_typesense_params) && relation.respond_to?(:klass)
        raise ArgumentError, 'relation must respond to :to_typesense_params and :klass'
      end

      k = relation.klass
      raise ArgumentError, 'relation.klass must be a Class' unless k.is_a?(Class)

      # Ensure collection can be resolved early for clearer errors
      collection_name_for_relation(relation)
    end

    def collection_name_for_relation(relation)
      k = relation.klass
      return k.collection if k.respond_to?(:collection) && k.collection && !k.collection.to_s.strip.empty?

      # Fallback: reverse-lookup in registry (mirrors Relation#collection_name_for_klass)
      begin
        mapping = SearchEngine::Registry.mapping
        found = mapping.find { |(_, cls)| cls == k }
        return found.first if found
      rescue StandardError
        # ignore
      end

      name = k.respond_to?(:name) && k.name ? k.name : k.to_s
      raise ArgumentError, "Unknown collection for #{name}"
    end

    # Remove URL-only options to avoid leaking them into request bodies.
    # Returns a new Hash.
    def filter_url_only_keys(hash)
      return {} unless hash.is_a?(Hash)

      hash.reject { |k, _| URL_ONLY_KEYS.include?(k.is_a?(Symbol) ? k : k.to_s.to_sym) }
    end

    # Remove empty/nil values from the payload for a clean body.
    # Returns a new Hash preserving insertion order.
    def prune_empty_values(hash)
      out = {}
      hash.each do |k, v|
        next if v.nil?

        if v.is_a?(String)
          next if v.strip.empty?
        elsif v.respond_to?(:empty?)
          next if v.empty?
        end
        out[k] = v
      end
      out
    end

    # Internal helpers for applying preset-mode safeguards deterministically.
    module EssentialPruner
      module_function

      def prune(merged_hash, relation:)
        preset = relation.respond_to?(:preset_name) ? relation.preset_name : nil
        return merged_hash unless preset

        mode = relation.respond_to?(:preset_mode) ? relation.preset_mode : :merge
        case mode
        when :only
          keep_only_essentials(merged_hash)
        when :lock
          drop_locked_domains(merged_hash)
        else
          merged_hash
        end
      end

      def keep_only_essentials(hash)
        out = {}
        ESSENTIAL_MULTI_KEYS.each do |k|
          out[k] = hash[k] if hash.key?(k)
        end
        out
      end

      def drop_locked_domains(hash)
        locked = SearchEngine.config.presets.locked_domains_set
        # Return a shallow copy with locked keys removed
        hash.reject { |k, _| locked.include?(k) }
      end
    end
  end
end
