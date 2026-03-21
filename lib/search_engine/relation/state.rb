# frozen_string_literal: true

module SearchEngine
  class Relation
    # Canonical immutable state helpers and defaults for Relation.
    # Owns deep-freeze and duplication utilities, plus initial state normalization.
    module State
      # Internal normalized state keys (authoritative defaults)
      DEFAULT_STATE = {
        filters: [].freeze,
        ast:     [].freeze,
        orders:  [].freeze,
        select:  [].freeze,
        select_nested: {}.freeze,
        select_nested_order: [].freeze,
        exclude: [].freeze,
        exclude_nested: {}.freeze,
        exclude_nested_order: [].freeze,
        joins:   [].freeze,
        limit:   nil,
        offset:  nil,
        page:    nil,
        per_page: nil,
        grouping: nil,
        options: {}.freeze,
        preset_name: nil,
        preset_mode: nil,
        facet_fields: [].freeze,
        facet_max_values: [].freeze,
        facet_queries: [].freeze,
        highlight: {}.freeze,
        highlight_fields: [].freeze,
        highlight_full_fields: [].freeze,
        highlight_start_tag: nil,
        highlight_end_tag: nil,
        highlight_affix_num_tokens: nil,
        highlight_snippet_threshold: nil,
        use_synonyms: nil,
        use_stopwords: nil,
        ranking: nil,
        hit_limits: {}.freeze,
        vector_query: nil
      }.freeze

      # Normalize the provided initial state Hash using the Relation's normalizers.
      # @param state [Hash]
      # @return [Hash]
      def normalize_initial_state(state)
        return {} if state.nil? || state.empty?
        raise ArgumentError, 'state must be a Hash' unless state.is_a?(Hash)

        normalized = {}
        state.each { |key, value| apply_initial_state_key!(normalized, key, value) }
        normalized
      end

      # Apply a single initial state key with normalization.
      # Delegates to the same normalizers used by DSL chainers.
      def apply_initial_state_key!(normalized, key, value)
        handlers = {
          filters: :handle_state_filters!,
          filters_ast: :handle_state_filters_ast!,
          ast: :handle_state_ast!,
          orders: :handle_state_orders!,
          select: :handle_state_select!,
          select_nested: :handle_state_select_nested!,
          select_nested_order: :handle_state_select_nested_order!,
          exclude: :handle_state_exclude!,
          exclude_nested: :handle_state_exclude_nested!,
          exclude_nested_order: :handle_state_exclude_nested_order!,
          joins: :handle_state_joins!,
          limit: :handle_state_limit!,
          offset: :handle_state_offset!,
          page: :handle_state_page!,
          per_page: :handle_state_per_page!,
          options: :handle_state_options!,
          grouping: :handle_state_grouping!,
          preset_name: :handle_state_preset_name!,
          preset_mode: :handle_state_preset_mode!,
          curation: :handle_state_curation!,
          facet_fields: :handle_state_facet_fields!,
          facet_max_values: :handle_state_facet_max_values!,
          facet_queries: :handle_state_facet_queries!,
          highlight: :handle_state_highlight!,
          ranking: :handle_state_ranking!,
          hit_limits: :handle_state_hit_limits!,
          vector_query: :handle_state_vector_query!
        }
        h = handlers[key.to_sym]
        return unless h

        send(h, normalized, value)
      end

      private

      def handle_state_filters!(normalized, value)
        normalized[:filters] = normalize_where(Array(value))
      end

      def handle_state_filters_ast!(normalized, value)
        nodes = Array(value).flatten.compact
        normalized[:ast] ||= []
        normalized[:ast] += if nodes.all? { |n| n.is_a?(SearchEngine::AST::Node) }
                              nodes
                            else
                              SearchEngine::DSL::Parser.parse_list(nodes, klass: @klass)
                            end
      end

      def handle_state_ast!(normalized, value)
        nodes = Array(value).flatten.compact
        normalized[:ast] = if nodes.all? { |n| n.is_a?(SearchEngine::AST::Node) }
                             nodes
                           else
                             SearchEngine::DSL::Parser.parse_list(nodes, klass: @klass)
                           end
      end

      # One-time migration: when AST is empty and legacy string filters exist, map legacy to AST::Raw.
      # Idempotent and safe for repeated calls.
      # @param state [Hash]
      # @return [void]
      def migrate_legacy_filters_to_ast!(state)
        return unless state.is_a?(Hash)

        ast_nodes = Array(state[:ast]).flatten.compact
        legacy = Array(state[:filters]).flatten.compact
        return if !ast_nodes.empty? || legacy.empty?

        raw_nodes = legacy.map { |fragment| SearchEngine::AST.raw(String(fragment)) }
        state[:ast] = raw_nodes
        nil
      end

      def handle_state_options!(normalized, value)
        normalized[:options] = (value || {}).dup
      end

      def handle_state_grouping!(normalized, value)
        normalized[:grouping] = normalize_grouping(value)
      end

      def handle_state_preset_name!(normalized, value)
        normalized[:preset_name] = value&.to_s&.strip
      end

      def handle_state_preset_mode!(normalized, value)
        normalized[:preset_mode] = value&.to_sym
      end

      def handle_state_curation!(normalized, value)
        normalized[:curation] = normalize_curation_input(value)
      end

      def handle_state_facet_fields!(normalized, value)
        normalized[:facet_fields] = Array(value).flatten.compact
      end

      def handle_state_facet_max_values!(normalized, value)
        normalized[:facet_max_values] = Array(value).flatten.compact
      end

      def handle_state_facet_queries!(normalized, value)
        normalized[:facet_queries] = Array(value).flatten.compact
      end

      def handle_state_highlight!(normalized, value)
        normalized[:highlight] = normalize_highlight_input(value)
      end

      def handle_state_ranking!(normalized, value)
        normalized[:ranking] = normalize_ranking_input(value || {})
      end

      def handle_state_hit_limits!(normalized, value)
        normalized[:hit_limits] = normalize_hit_limits_input(value || {})
      end

      def handle_state_vector_query!(normalized, value)
        return if value.nil?
        raise ArgumentError, 'vector_query state must be a Hash' unless value.is_a?(Hash)

        normalized[:vector_query] = value.dup
      end

      # Newly added handlers for remaining state keys
      def handle_state_orders!(normalized, value)
        additions = normalize_order(value)
        normalized[:orders] = dedupe_orders_last_wins(additions)
      end

      def handle_state_select!(normalized, value)
        normalized[:select] = normalize_select(value)
      end

      def handle_state_select_nested!(normalized, value)
        nested_in = value || {}
        nested = {}
        nested_in.each do |k, v|
          key = k.to_sym
          fields = Array(v).flatten.compact
          nested[key] = fields
        end
        normalized[:select_nested] = nested
      end

      def handle_state_select_nested_order!(normalized, value)
        normalized[:select_nested_order] = Array(value).flatten.compact.map(&:to_sym)
      end

      def handle_state_exclude!(normalized, value)
        normalized[:exclude] = normalize_select(value)
      end

      def handle_state_exclude_nested!(normalized, value)
        nested_in = value || {}
        nested = {}
        nested_in.each do |k, v|
          key = k.to_sym
          fields = Array(v).flatten.compact
          nested[key] = fields
        end
        normalized[:exclude_nested] = nested
      end

      def handle_state_exclude_nested_order!(normalized, value)
        normalized[:exclude_nested_order] = Array(value).flatten.compact.map(&:to_sym)
      end

      def handle_state_joins!(normalized, value)
        normalized[:joins] = normalize_joins(value)
      end

      def handle_state_limit!(normalized, value)
        normalized[:limit] = coerce_integer_min(value, :limit, 1)
      end

      def handle_state_offset!(normalized, value)
        normalized[:offset] = coerce_integer_min(value, :offset, 0)
      end

      def handle_state_page!(normalized, value)
        normalized[:page] = coerce_integer_min(value, :page, 1)
      end

      def handle_state_per_page!(normalized, value)
        normalized[:per_page] = coerce_integer_min(value, :per, 1)
      end

      # Deep duplicate Hash/Array trees; leaves are returned as-is.
      # @param obj [Object]
      # @return [Object]
      def deep_dup(obj)
        case obj
        when Hash
          obj.transform_values(&method(:deep_dup))
        when Array
          obj.map(&method(:deep_dup))
        else
          obj
        end
      end

      # Deep-freeze Hash/Array/String trees in-place; returns the frozen object.
      # @param obj [Object]
      # @return [Object]
      def deep_freeze_inplace(obj)
        case obj
        when Hash
          obj.each_value { |v| deep_freeze_inplace(v) }
          obj.freeze
        when Array
          obj.each { |el| deep_freeze_inplace(el) }
          obj.freeze
        else
          obj.freeze if obj.is_a?(String)
        end
        obj
      end
    end
  end
end
