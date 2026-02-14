# frozen_string_literal: true

module SearchEngine
  # MultiResult wraps a Typesense multi-search response and exposes
  # labeled Result objects while preserving insertion order.
  #
  # Labels are canonicalized using {SearchEngine::Multi.canonicalize_label}
  # to case-insensitive Symbols. Accessors accept String or Symbol labels.
  #
  # Construction expects parallel arrays of labels and raw result items
  # (as returned by Typesense under 'results'). An optional mapping of
  # model classes may be provided either as an Array (parallel to labels)
  # or as a Hash keyed by label. When a class is not provided, hydration
  # falls back to the collection registry if the raw result exposes a
  # collection name; otherwise OpenStruct is used.
  #
  # Network calls are performed only once by the caller (e.g.,
  # {SearchEngine.multi_search_result}). This wrapper operates purely
  # in-memory and will never make HTTP requests.
  #
  # @example
  #   mr = SearchEngine::MultiResult.new(
  #     labels: [:products, :brands],
  #     raw_results: [raw_a, raw_b],
  #     klasses: [Product, Brand]
  #   )
  #   mr[:products].found
  #   mr.dig('brands').to_a
  #   mr.labels #=> [:products, :brands]
  class MultiResult
    # Build a new MultiResult.
    #
    # @param labels [Array<String, Symbol>] ordered labels
    # @param raw_results [Array<Hash>] ordered raw result items (one per label)
    # @param klasses [Array<Class>, Hash{(String,Symbol)=>Class}, nil] optional model classes
    # @raise [ArgumentError] when sizes mismatch, labels invalid/duplicate, or inputs malformed
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/multi-search-guide`
    def initialize(labels:, raw_results:, klasses: nil)
      @labels = canonicalize_labels(labels)
      @map = {}

      # Store the raw array privately to document the one-time network call and hydration
      @raw_results = Array(raw_results).freeze

      validate_sizes!(@labels, @raw_results, klasses)

      klass_by_index = build_klass_index(@labels, klasses)

      @labels.each_with_index do |label, index|
        raw = @raw_results[index]
        kls = klass_by_index[index] || infer_klass_from_raw(raw)
        @map[label] = SearchEngine::Result.new(raw, klass: kls)
      end

      freeze
    end

    # Return a shallow copy of labels to preserve immutability.
    # @return [Array<Symbol>]
    def labels
      @labels.dup
    end

    # Fetch a Result by label.
    # @param label [String, Symbol]
    # @return [SearchEngine::Result, nil]
    def [](label)
      @map[SearchEngine::Multi.canonicalize_label(label)]
    rescue ArgumentError
      nil
    end

    # Alias for {#[]} to support dig-like ergonomics.
    # @param label [String, Symbol]
    # @return [SearchEngine::Result, nil]
    def dig(label)
      self[label]
    end

    # Hash-like alias for {#labels}.
    # @return [Array<Symbol>]
    def keys
      labels
    end

    # Shallow Hash mapping labels to results.
    # Pure helper: returns a shallow copy and never performs HTTP.
    # @return [Hash{Symbol=>SearchEngine::Result}]
    def to_h
      @map.dup
    end

    # Iterate over (label, result) in insertion order.
    # Yields a two-element array to support destructuring via `|(label, result)|`.
    # Pure helper: operates on cached hydration and never performs HTTP.
    # @yieldparam pair [Array(Symbol, SearchEngine::Result)]
    # @return [Enumerator] when no block is given
    def each_label
      return enum_for(:each_label) unless block_given?

      @labels.each { |l| yield([l, @map[l]]) }
    end

    # Convenience to map over (label, result) pairs.
    # Implemented via {#each_label}, purely in-memory.
    # @yieldparam label [Symbol]
    # @yieldparam result [SearchEngine::Result]
    # @return [Enumerator, Array] Enumerator when no block is given; otherwise the mapped Array
    # @example
    #   mr.map_labels { |label, result| [label, result.found] }
    def map_labels
      return each_label.map unless block_given?

      each_label.map { |(label, result)| yield(label, result) }
    end

    private

    def canonicalize_labels(labels)
      unless labels.is_a?(Array) && labels.all? { |l| l.is_a?(String) || l.is_a?(Symbol) }
        raise ArgumentError, 'labels must be an Array of String/Symbol'
      end

      out = []
      seen = {}
      labels.each do |l|
        key = SearchEngine::Multi.canonicalize_label(l)
        raise ArgumentError, "duplicate label: #{l.inspect}" if seen[key]

        seen[key] = true
        out << key
      end
      out
    end

    def validate_sizes!(labels, raw_results, klasses)
      raise ArgumentError, 'raw_results must be an Array' unless raw_results.is_a?(Array)

      if labels.size != raw_results.size
        raise ArgumentError,
              [
                "labels count (#{labels.size}) does not match raw_results count (#{raw_results.size}).",
                'Verify builder vs. client mapping by index.'
              ].join(' ')
      end

      return unless klasses.is_a?(Array) && klasses.size != labels.size

      raise ArgumentError, "klasses count (#{klasses.size}) does not match labels count (#{labels.size})."
    end

    def build_klass_index(labels, klasses)
      return {} if klasses.nil?

      if klasses.is_a?(Array)
        return klasses.each_with_index.to_h { |kls, idx| [idx, (kls if kls.is_a?(Class))] }
      end

      if klasses.is_a?(Hash)
        index = {}
        labels.each_with_index do |label, idx|
          k = klasses[label] || klasses[label.to_s] || klasses[label.to_sym]
          index[idx] = k if k.is_a?(Class)
        end
        return index
      end

      raise ArgumentError, 'klasses must be an Array, a Hash, or nil'
    end

    def infer_klass_from_raw(raw)
      # Some Typesense clients may include `collection` alongside each result item
      # in multi-search responses. Resolve via registry when present; otherwise nil.
      name = begin
        raw && (raw['collection'] || raw[:collection])
      rescue StandardError
        nil
      end
      return nil if name.nil?

      SearchEngine.collection_for(name)
    rescue ArgumentError
      nil
    end
  end
end
