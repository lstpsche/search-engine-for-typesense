# frozen_string_literal: true

module SearchEngine
  class Relation
    module DSL
      # Vector search chainers and normalizers.
      # Mixed into Relation's DSL; preserves copy-on-write semantics.
      module Vectors
        VECTOR_SEARCH_DOC_URL = SearchEngine::Errors::InvalidVectorQuery::DOC_URL

        # Perform a vector (semantic / hybrid / ANN) search on an embedding field.
        #
        # Last call wins — Typesense supports a single `vector_query` per search.
        # When +field+ is omitted the sole embedding declared on the model is
        # used automatically; raises when the model has zero or multiple embeddings.
        #
        # @param field [Symbol, String, nil] embedding field name (auto-resolved when nil)
        # @param k [Integer, nil] number of nearest neighbors
        # @param alpha [Float, nil] hybrid blend weight (0.0 = keyword, 1.0 = vector)
        # @param query [Array<Numeric>, nil] explicit embedding vector
        # @param id [#to_s, nil] document ID for similarity search
        # @param distance_threshold [Float, nil] max cosine distance
        # @param queries [Array<String>, nil] historical query strings (auto-embedded).
        #   Requires Typesense >= 27.0 for strings containing commas.
        # @param weights [Array<Numeric>, nil] per-query weights (must sum to ~1.0)
        # @param ef [Integer, nil] HNSW ef override
        # @param flat_search_cutoff [Integer, nil] brute-force threshold
        # @return [SearchEngine::Relation]
        def vector_search(field = nil, k: nil, alpha: nil, query: nil, id: nil,
                          distance_threshold: nil, queries: nil, weights: nil,
                          ef: nil, flat_search_cutoff: nil)
          resolved = resolve_vector_field(field)
          normalized = normalize_vector_search(
            resolved, k: k, alpha: alpha, query: query, id: id,
            distance_threshold: distance_threshold, queries: queries,
            weights: weights, ef: ef, flat_search_cutoff: flat_search_cutoff
          )
          spawn { |s| s[:vector_query] = normalized }
        end

        # Find documents similar to a given document ID.
        #
        # Sugar over `vector_search` with `id:`.
        # When +field+ is omitted the sole embedding declared on the model is
        # used automatically (same resolution as {#vector_search}).
        #
        # @param document_id [#to_s] ID of the reference document
        # @param field [Symbol, String, nil] embedding field name (auto-resolved when nil)
        # @param k [Integer, nil] number of nearest neighbors
        # @param distance_threshold [Float, nil] max cosine distance
        # @return [SearchEngine::Relation]
        def find_similar(document_id, field: nil, k: nil, distance_threshold: nil)
          vector_search(field, id: document_id, k: k, distance_threshold: distance_threshold)
        end

        private

        def normalize_vector_search(field, k:, alpha:, query:, id:,
                                    distance_threshold:, queries:, weights:,
                                    ef:, flat_search_cutoff:)
          validate_vector_field!(field)
          field_sym = field.to_sym

          validate_vector_query_mode_exclusivity!(query, id, queries)
          validate_vector_query_array!(query)
          validate_vector_queries!(queries)
          validate_vector_weights!(weights, queries)

          coerced_k = coerce_vector_positive_integer(k, :k)
          coerced_ef = coerce_vector_positive_integer(ef, :ef)
          coerced_flat = coerce_vector_positive_integer(flat_search_cutoff, :flat_search_cutoff)
          validate_vector_alpha!(alpha)
          validate_vector_distance_threshold!(distance_threshold)

          result = { field: field_sym }
          result[:k] = coerced_k if coerced_k
          result[:alpha] = Float(alpha) if alpha
          result[:query] = query if query
          result[:id] = id.to_s if id
          result[:distance_threshold] = Float(distance_threshold) if distance_threshold
          result[:queries] = queries if queries
          result[:weights] = weights if weights
          result[:ef] = coerced_ef if coerced_ef
          result[:flat_search_cutoff] = coerced_flat if coerced_flat
          result
        end

        # -- Field validation ---------------------------------------------------

        def validate_vector_field!(field)
          unless field.is_a?(Symbol) || field.is_a?(String)
            raise SearchEngine::Errors::InvalidVectorQuery.new(
              "InvalidVectorQuery: field must be a Symbol or String (got #{field.class})",
              doc: VECTOR_SEARCH_DOC_URL,
              details: { field: field }
            )
          end

          name = field.to_s.strip
          if name.empty?
            raise SearchEngine::Errors::InvalidVectorQuery.new(
              'InvalidVectorQuery: field name must be non-empty',
              doc: VECTOR_SEARCH_DOC_URL
            )
          end

          sym = field.to_sym
          attrs = safe_attributes_map
          return if attrs.nil? || attrs.empty?

          embeddings = vector_embeddings_map

          return if attrs.key?(sym) || embeddings.key?(sym)

          known = (attrs.keys + embeddings.keys).uniq
          suggestions = suggest_fields(sym, known)
          suggest_str = build_vector_field_suggestion(suggestions)

          raise SearchEngine::Errors::InvalidVectorQuery.new(
            "InvalidVectorQuery: unknown vector field :#{sym} on #{klass_name_for_inspect}#{suggest_str}",
            hint: 'Declare the field with `embedding` in your model DSL',
            doc: VECTOR_SEARCH_DOC_URL,
            details: { field: sym, known_vector_fields: embeddings.keys, known_attributes: attrs.keys }
          )
        end

        # -- Mode exclusivity ---------------------------------------------------

        def validate_vector_query_mode_exclusivity!(query, id, queries)
          modes = []
          modes << :query if query
          modes << :id if id
          modes << :queries if queries
          return if modes.length <= 1

          raise SearchEngine::Errors::InvalidVectorQuery.new(
            "InvalidVectorQuery: #{modes.map { |m| "#{m}:" }.join(', ')} are mutually exclusive",
            hint: 'Provide only one of query:, id:, or queries:',
            doc: VECTOR_SEARCH_DOC_URL,
            details: { provided_modes: modes }
          )
        end

        # -- Individual param validators ----------------------------------------

        def validate_vector_query_array!(query)
          return if query.nil?

          unless query.is_a?(Array) && query.all? { |v| v.is_a?(Numeric) }
            raise SearchEngine::Errors::InvalidVectorQuery.new(
              'InvalidVectorQuery: query: must be an Array of Numeric values',
              hint: 'Pass a float array like [0.1, 0.2, ...]',
              doc: VECTOR_SEARCH_DOC_URL,
              details: { query_class: query.class.name }
            )
          end

          return unless query.empty?

          raise SearchEngine::Errors::InvalidVectorQuery.new(
            'InvalidVectorQuery: query: must be a non-empty Array',
            hint: 'Provide at least one dimension value',
            doc: VECTOR_SEARCH_DOC_URL
          )
        end

        def validate_vector_queries!(queries)
          return if queries.nil?

          unless queries.is_a?(Array) && queries.all? { |v| v.is_a?(String) }
            raise SearchEngine::Errors::InvalidVectorQuery.new(
              'InvalidVectorQuery: queries: must be an Array of String',
              hint: 'Pass an array of query strings like ["smart phone", "tablet"]',
              doc: VECTOR_SEARCH_DOC_URL,
              details: { queries_class: queries.class.name }
            )
          end

          if queries.empty?
            raise SearchEngine::Errors::InvalidVectorQuery.new(
              'InvalidVectorQuery: queries: must be a non-empty Array',
              doc: VECTOR_SEARCH_DOC_URL
            )
          end

          blank_indices = queries.each_index.select { |i| queries[i].strip.empty? }
          return if blank_indices.empty?

          raise SearchEngine::Errors::InvalidVectorQuery.new(
            "InvalidVectorQuery: queries: contains blank entries at index #{blank_indices.join(', ')}",
            hint: 'Each query string must contain non-whitespace text',
            doc: VECTOR_SEARCH_DOC_URL,
            details: { blank_indices: blank_indices }
          )
        end

        def validate_vector_weights!(weights, queries)
          return if weights.nil?

          unless weights.is_a?(Array) && weights.all? { |v| v.is_a?(Numeric) }
            raise SearchEngine::Errors::InvalidVectorQuery.new(
              'InvalidVectorQuery: weights: must be an Array of Numeric',
              doc: VECTOR_SEARCH_DOC_URL,
              details: { weights_class: weights.class.name }
            )
          end

          if queries.nil?
            raise SearchEngine::Errors::InvalidVectorQuery.new(
              'InvalidVectorQuery: weights: requires queries: to be set',
              hint: 'Provide queries: alongside weights:',
              doc: VECTOR_SEARCH_DOC_URL
            )
          end

          if weights.length != queries.length
            raise SearchEngine::Errors::InvalidVectorQuery.new(
              "InvalidVectorQuery: weights: length (#{weights.length}) must match queries: length (#{queries.length})",
              doc: VECTOR_SEARCH_DOC_URL,
              details: { weights_length: weights.length, queries_length: queries.length }
            )
          end

          sum = weights.sum.to_f
          tolerance = SearchEngine.config.embedding.weights_sum_tolerance
          return if (sum - 1.0).abs <= tolerance

          raise SearchEngine::Errors::InvalidVectorQuery.new(
            "InvalidVectorQuery: weights: must sum to ~1.0 (got #{sum.round(4)})",
            hint: "Adjust weights so they sum to 1.0 (tolerance: #{tolerance}, " \
                  'configurable via config.embedding.weights_sum_tolerance)',
            doc: VECTOR_SEARCH_DOC_URL,
            details: { weights: weights, sum: sum.round(4), tolerance: tolerance }
          )
        end

        def validate_vector_alpha!(alpha)
          return if alpha.nil?

          begin
            fv = Float(alpha)
          rescue ArgumentError, TypeError
            raise SearchEngine::Errors::InvalidVectorQuery.new(
              "InvalidVectorQuery: alpha: must be a Numeric between 0.0 and 1.0 (got #{alpha.inspect})",
              doc: VECTOR_SEARCH_DOC_URL,
              details: { alpha: alpha }
            )
          end

          return if fv >= 0.0 && fv <= 1.0 && fv.finite?

          raise SearchEngine::Errors::InvalidVectorQuery.new(
            "InvalidVectorQuery: alpha: must be between 0.0 and 1.0 (got #{fv})",
            hint: '0.0 = pure keyword, 1.0 = pure vector',
            doc: VECTOR_SEARCH_DOC_URL,
            details: { alpha: fv }
          )
        end

        def validate_vector_distance_threshold!(distance_threshold)
          return if distance_threshold.nil?

          begin
            fv = Float(distance_threshold)
          rescue ArgumentError, TypeError
            raise SearchEngine::Errors::InvalidVectorQuery.new(
              'InvalidVectorQuery: distance_threshold: must be a non-negative Numeric ' \
              "(got #{distance_threshold.inspect})",
              doc: VECTOR_SEARCH_DOC_URL,
              details: { distance_threshold: distance_threshold }
            )
          end

          return if fv >= 0.0 && fv.finite?

          raise SearchEngine::Errors::InvalidVectorQuery.new(
            "InvalidVectorQuery: distance_threshold: must be >= 0.0 (got #{fv})",
            doc: VECTOR_SEARCH_DOC_URL,
            details: { distance_threshold: fv }
          )
        end

        def coerce_vector_positive_integer(value, name)
          return nil if value.nil?

          begin
            iv = Integer(value)
          rescue ArgumentError, TypeError
            raise SearchEngine::Errors::InvalidVectorQuery.new(
              "InvalidVectorQuery: #{name}: must be a positive Integer (got #{value.inspect})",
              doc: VECTOR_SEARCH_DOC_URL,
              details: { name => value }
            )
          end

          return iv if iv.positive?

          raise SearchEngine::Errors::InvalidVectorQuery.new(
            "InvalidVectorQuery: #{name}: must be > 0 (got #{iv})",
            doc: VECTOR_SEARCH_DOC_URL,
            details: { name => iv }
          )
        end

        # -- Field resolution ---------------------------------------------------

        # Auto-resolve the embedding field when none is given explicitly.
        # Returns the field as-is when provided, or the sole embedding field
        # declared on the model. Raises when resolution is ambiguous or impossible.
        #
        # @param field [Symbol, String, nil] explicit field or nil for auto-resolution
        # @return [Symbol, String]
        def resolve_vector_field(field)
          return field unless field.nil?

          embeddings = vector_embeddings_map
          case embeddings.size
          when 1
            embeddings.each_key.first
          when 0
            raise SearchEngine::Errors::InvalidVectorQuery.new(
              "InvalidVectorQuery: cannot auto-resolve embedding field \u2014 " \
              "no embeddings declared on #{klass_name_for_inspect}",
              hint: 'Declare one with `embedding` in your model DSL',
              doc: VECTOR_SEARCH_DOC_URL
            )
          else
            fields_list = embeddings.keys.map { |k| ":#{k}" }.join(', ')
            raise SearchEngine::Errors::InvalidVectorQuery.new(
              "InvalidVectorQuery: cannot auto-resolve embedding field \u2014 " \
              "#{klass_name_for_inspect} has multiple embeddings (#{fields_list})",
              hint: 'Pass the field explicitly: .vector_search(:field_name)',
              doc: VECTOR_SEARCH_DOC_URL,
              details: { available_embeddings: embeddings.keys }
            )
          end
        end

        # -- Helpers ------------------------------------------------------------

        def vector_embeddings_map
          return {} unless @klass.respond_to?(:embeddings_config)

          @klass.embeddings_config || {}
        rescue StandardError
          {}
        end

        def build_vector_field_suggestion(suggestions)
          return '' if suggestions.empty?

          if suggestions.length == 1
            " (did you mean :#{suggestions.first}?)"
          else
            last = suggestions.last
            others = suggestions[0..-2].map { |s| ":#{s}" }.join(', ')
            " (did you mean #{others}, or :#{last}?)"
          end
        end
      end
    end
  end
end
