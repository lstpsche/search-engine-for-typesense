# frozen_string_literal: true

module SearchEngine
  # Immutable, chainable query relation bound to a model class.
  #
  # Facade wiring that composes State, Options, DSL, Compiler, and Materializers.
  class Relation
    # Limited set of Array/Enumerable methods we intentionally delegate to the
    # materialized results for convenience. Restricting this list prevents
    # accidental network calls during reflection/printing in non‑interactive contexts.
    ARRAY_DELEGATED_METHODS = %i[
      to_a each map collect select filter reject find find_all detect any? all? none? one? empty?
      size length include? first last take drop [] at slice reduce inject sum uniq
      compact flatten each_with_index each_with_object index_by group_by partition grep grep_v flat_map collect_concat
      each_slice each_cons reverse_each sort sort_by min max min_by max_by minmax minmax_by
      take_while drop_while chunk chunk_while slice_when slice_before slice_after lazy
      find_index index rindex values_at sample shuffle rotate
    ].freeze
    # Keys considered essential for :only preset mode.
    ESSENTIAL_PARAM_KEYS = %i[q page per_page].freeze

    # @return [Class] bound model class (typically a SearchEngine::Base subclass)
    attr_reader :klass

    # Modules are required explicitly to keep require graph stable
    require 'search_engine/relation/state'
    require 'search_engine/relation/options'
    require 'search_engine/relation/dsl'
    require 'search_engine/relation/compiler'
    require 'search_engine/relation/deletion'
    require 'search_engine/relation/updating'
    require 'search_engine/relation/materializers'

    include State
    include Options
    include DSL
    include Compiler
    include Deletion
    include Updating
    include Materializers

    # Convenience conversion to compiled body params as a plain Hash.
    def to_h
      v = to_typesense_params
      v.respond_to?(:to_h) ? v.to_h : v
    end

    # Return compiled Typesense filter_by string for this relation.
    # Pure and deterministic; delegates to the compiler without I/O.
    #
    # @return [String, nil] the filter_by string or nil when absent
    def filter_params
      params = to_typesense_params
      params[:filter_by]
    end

    # Return a Hash of simple base-field equality filters accumulated on this relation.
    # Extracts only Eq/In predicates on base fields (no joins, no ranges, no negations, no ORs).
    # When the predicate set contains constructs that cannot be represented as a flat Hash
    # (e.g. OR, NOT, range comparisons, join predicates), returns an empty Hash to avoid
    # misrepresenting the filter semantics.
    #
    # @return [Hash{Symbol=>Object}] symbolized base field => value(s) or {}
    def filter_params_hash
      nodes = Array(@state[:ast]).flatten.compact
      return {} if nodes.empty?

      result = {}
      ambiguous = false

      walker = lambda do |node|
        return if ambiguous || node.nil?

        case node
        when SearchEngine::AST::And
          Array(node.children).each { |child| walker.call(child) }
        when SearchEngine::AST::Group
          inner = Array(node.children).first
          walker.call(inner)
        when SearchEngine::AST::Eq
          field = node.field.to_s
          # Only base fields: exclude joins like "$assoc.field"
          result[field.to_sym] = node.value unless join_field?(field)
        when SearchEngine::AST::In
          field = node.field.to_s
          result[field.to_sym] = Array(node.values) unless join_field?(field)
        else
          # Any non-equality, negation, OR, range, or raw fragment makes this ambiguous
          ambiguous ||= ambiguous_ast_node?(node)
        end
      end

      nodes.each { |n| walker.call(n) }
      return {} if ambiguous

      result
    end

    private

    # True when the field name refers to a joined field like "$assoc.field".
    # @param field [String]
    # @return [Boolean]
    def join_field?(field)
      field.start_with?('$') || field.include?('.')
    end

    # True when node type cannot be represented as a flat Hash of base Eq/In.
    # @param node [SearchEngine::AST::Node]
    # @return [Boolean]
    def ambiguous_ast_node?(node)
      node.is_a?(SearchEngine::AST::Or) ||
        node.is_a?(SearchEngine::AST::NotEq) ||
        node.is_a?(SearchEngine::AST::NotIn) ||
        node.is_a?(SearchEngine::AST::Gt) ||
        node.is_a?(SearchEngine::AST::Gte) ||
        node.is_a?(SearchEngine::AST::Lt) ||
        node.is_a?(SearchEngine::AST::Lte) ||
        node.is_a?(SearchEngine::AST::Raw)
    end

    # Read-only access to accumulated predicate AST nodes.
    # @return [Array<SearchEngine::AST::Node>] a frozen Array of AST nodes
    def ast
      nodes = Array(@state[:ast])
      nodes.frozen? ? nodes : nodes.dup.freeze
    end

    # Return the effective preset mode when a preset is applied.
    # Falls back to :merge when not explicitly set.
    # @return [Symbol]
    def preset_mode
      (@state[:preset_mode] || :merge).to_sym
    end

    # Return the effective preset token (namespaced if configured) or nil.
    # @return [String, nil]
    def preset_name
      @state[:preset_name]
    end

    public :ast, :preset_mode, :preset_name, :to_typesense_params

    # Create a new Relation.
    # @param klass [Class]
    # @param state [Hash]
    def initialize(klass, state = {})
      @klass = klass
      normalized = normalize_initial_state(state)
      @state = DEFAULT_STATE.merge(normalized)
      migrate_legacy_filters_to_ast!(@state)
      deep_freeze_inplace(@state)
      @__result_memo = nil
      @__loaded = false
      @__load_lock = Mutex.new
    end

    # Return self for AR-like parity.
    # @return [SearchEngine::Relation]
    def all
      self
    end

    # True when the relation has no accumulated state beyond defaults.
    # @return [Boolean]
    def empty?
      @state == DEFAULT_STATE
    end

    public

    # Console-friendly inspect without network I/O.
    # Always return a concise, stable summary to avoid surprises across consoles.
    # @return [String]
    def inspect
      cfg = begin
        SearchEngine.config
      rescue StandardError
        nil
      end

      materialize = cfg.respond_to?(:relation_print_materializes) ? cfg.relation_print_materializes : true
      return summary_inspect_string unless materialize

      preview_size = 11
      begin
        items = SearchEngine::Hydration::Materializers.preview(self, preview_size)
        entries = Array(items).map { |obj| obj.respond_to?(:inspect) ? obj.inspect : obj.to_s }
        entries[10] = '...' if entries.size == preview_size
        +"#<#{self.class.name} [#{entries.join(', ')}]>"
      rescue StandardError
        # Defensive fallback to non-I/O summary when materialization fails
        summary_inspect_string
      end
    end

    # String form mirrors inspect to support printers that prefer to_s.
    # @return [String]
    def to_s
      inspect
    end

    # Pry hooks into pretty_inspect in many cases. Keep it consistent with #inspect
    # to avoid delegating to model class printers accidentally.
    # @return [String]
    def pretty_inspect
      inspect
    end

    # Pretty print the concise, stable summary without network I/O.
    # Avoid hydration during console pretty printing to keep behavior predictable.
    # @param pp [PP]
    # @return [void]
    def pretty_print(pp)
      cfg = begin
        SearchEngine.config
      rescue StandardError
        nil
      end

      materialize = cfg.respond_to?(:relation_print_materializes) ? cfg.relation_print_materializes : true
      unless materialize
        pp.text(summary_inspect_string)
        return
      end

      preview_size = 11
      begin
        items = SearchEngine::Hydration::Materializers.preview(self, preview_size)

        pp.group(2, "#<#{self.class.name} [", ']>') do
          items.each_with_index do |obj, idx|
            if idx.positive?
              pp.text(',')
              pp.breakable ' '
            end
            pp.pp(obj)
          end

          if items.size == preview_size
            pp.text(',') unless items.empty?
            pp.breakable ' '
            pp.text('...')
          end
        end
      rescue StandardError
        pp.text(summary_inspect_string)
      end
    end

    public :ast, :preset_mode, :preset_name, :to_typesense_params

    # Explain the current relation without performing any network calls.
    # @return [String]
    def explain(to: nil)
      params = to_typesense_params

      lines = []
      header = "#{klass_name_for_inspect} Relation"
      lines << header

      append_preset_explain_line(lines, params)
      append_curation_explain_lines(lines)
      append_boolean_knobs_explain_lines(lines)
      append_where_and_order_lines(lines, params)
      append_grouping_explain_lines(lines)
      append_selection_explain_lines(lines, params)
      add_effective_selection_tokens!(lines)
      add_pagination_line!(lines, params)

      out = lines.join("\n")
      puts(out) if to == :stdout
      out
    end

    # Read-only list of join association names accumulated on this relation.
    # @return [Array<Symbol>]
    def joins_list
      list = Array(@state[:joins])
      list.frozen? ? list : list.dup.freeze
    end

    # Read-only grouping state for debugging/explain.
    # @return [Hash, nil]
    def grouping
      g = @state[:grouping]
      return nil if g.nil?

      g.frozen? ? g : g.dup.freeze
    end

    # Read-only selected fields state for debugging (base + nested).
    # @return [Hash]
    def selected_fields_state
      base = Array(@state[:select])
      nested = @state[:select_nested] || {}
      order = Array(@state[:select_nested_order])

      {
        base: base.dup.freeze,
        nested: nested.transform_values { |arr| Array(arr).dup.freeze }.freeze,
        nested_order: order.dup.freeze
      }.freeze
    end

    # Programmatic accessor for preset conflicts in :lock mode.
    # @return [Array<Hash{Symbol=>Symbol}>]
    def preset_conflicts
      params = to_typesense_params
      keys = Array(params[:_preset_conflicts]).map { |k| k.respond_to?(:to_sym) ? k.to_sym : k }.grep(Symbol)
      return [].freeze if keys.empty?

      keys.sort.map { |k| { key: k, reason: :locked_by_preset } }.freeze
    end

    # Read-only hit limits state for debugging/explain.
    # @return [Hash, nil]
    def hit_limits
      hl = @state[:hit_limits]
      return nil if hl.nil?

      hl.frozen? ? hl : hl.dup.freeze
    end

    # Handle unknown methods by delegating to the materialized Array.
    # Allows callers to use enumerable helpers directly on Relation.
    #
    # @param method_name [Symbol]
    # @param args [Array<Object>]
    # @return [Object]
    # @raise [NoMethodError] when the delegated Array doesn't support the method
    def method_missing(method_name, *args, &block)
      # Delegate to the model class first (AR-like behavior)
      # Avoid delegating reflective/identity methods that console printers may use
      # to derive labels; these should reflect the Relation itself.
      blocked_class_delegations = %i[
        name inspect pretty_inspect to_s to_str to_ary class object_id __id__
        methods public_methods singleton_class respond_to? respond_to_missing?
      ]
      if @klass.respond_to?(method_name) && !blocked_class_delegations.include?(method_name.to_sym)
        return @klass.public_send(method_name, *args, &block)
      end

      return super if blocked_class_delegations.include?(method_name.to_sym)

      # Only delegate to the materialized Array for a conservative whitelist of methods.
      unless ARRAY_DELEGATED_METHODS.include?(method_name.to_sym)
        raise NoMethodError, %(undefined method `#{method_name}` for #{@klass}:Class)
      end

      arr = to_a
      arr.public_send(method_name, *args, &block)
    end

    public :ast, :preset_mode, :preset_name, :to_typesense_params

    # Ensure reflective APIs correctly report delegated methods on the
    # materialized Array target. This keeps semantics consistent with
    # method_missing above for interactive consoles and chaining.
    # @param method_name [Symbol]
    # @param include_private [Boolean]
    # @return [Boolean]
    def respond_to_missing?(method_name, include_private = false)
      # Explicitly avoid implicit conversions that change object identity in consoles
      return false if %i[to_ary to_str].include?(method_name.to_sym)

      # Whitelist a conservative set of Enumerable-like methods for convenience.
      ARRAY_DELEGATED_METHODS.include?(method_name.to_sym) || super
    end

    protected

    # Spawn a new relation with a deep-duplicated mutable state.
    # @yieldparam state [Hash]
    # @return [SearchEngine::Relation]
    def spawn
      mutable_state = deep_dup(@state)
      yield mutable_state
      self.class.new(@klass, mutable_state)
    end

    private

    # True when the relation has already executed and memoized the result.
    # @return [Boolean]
    def loaded?
      @__loaded == true
    end

    def summary_inspect_string
      parts = []
      parts << "Model=#{klass_name_for_inspect}"

      if (pn = @state[:preset_name])
        pm = @state[:preset_mode] || :merge
        parts << %(preset=#{pn}(mode=#{pm}))
      end

      filters = Array(@state[:filters])
      parts << "filters=#{filters.length}" unless filters.empty?

      ast_nodes = Array(@state[:ast])
      parts << "ast=#{ast_nodes.length}" unless ast_nodes.empty?

      compiled = begin
        SearchEngine::CompiledParams.from(to_typesense_params)
      rescue StandardError
        {}
      end

      sort_str = compiled[:sort_by]
      parts << %(sort="#{truncate_for_inspect(sort_str)}") if sort_str && !sort_str.to_s.empty?

      append_selection_inspect_parts(parts, compiled)

      if (g = @state[:grouping])
        gparts = ["group_by=#{g[:field]}"]
        gparts << "limit=#{g[:limit]}" if g[:limit]
        gparts << 'missing_values=true' if g[:missing_values]
        parts << gparts.join(' ')
      end

      parts << "page=#{compiled[:page]}" if compiled.key?(:page)
      parts << "per=#{compiled[:per_page]}" if compiled.key?(:per_page)

      "#<#{self.class.name} #{parts.join(' ')} >"
    end

    def interactive_console?
      return true if defined?(Rails::Console)
      return true if defined?(IRB) && $stdout.respond_to?(:tty?) && $stdout.tty?

      # Pry detection (best-effort, without hard dependency)
      return true if defined?(Pry) && (Pry.respond_to?(:active?) ? Pry.active? : true)

      return true if $PROGRAM_NAME&.end_with?('console')

      false
    end

    def klass_name_for_inspect
      @klass.respond_to?(:name) && @klass.name ? @klass.name : @klass.to_s
    end

    def safe_attributes_map
      if @klass.respond_to?(:attributes)
        base = @klass.attributes || {}
        return base if base.key?(:id)

        # Mirror parser logic for implicit id type
        if @klass.instance_variable_defined?(:@__identify_by_kind__) &&
           @klass.instance_variable_get(:@__identify_by_kind__) == :symbol &&
           @klass.instance_variable_defined?(:@__identify_by_symbol__) &&
           @klass.instance_variable_get(:@__identify_by_symbol__) == :id
          base.merge(id: :integer)
        else
          base.merge(id: :string)
        end
      else
        { id: :string }
      end
    end

    def validate_hash_keys!(hash, attributes_map)
      return if hash.nil? || hash.empty?

      known = attributes_map.keys.map(&:to_sym)
      # Ignore association-style keys whose values are Hash (handled by DSL::Parser for joins)
      candidate_keys = hash.reject { |_, v| v.is_a?(Hash) }.keys
      unknown = candidate_keys.map(&:to_sym) - known
      return if unknown.empty?

      begin
        cfg = SearchEngine.config
        return unless cfg.respond_to?(:strict_fields) ? cfg.strict_fields : true
      rescue StandardError
        nil
      end

      klass_name = klass_name_for_inspect
      known_list = known.map(&:to_s).sort.join(', ')
      unknown_name = unknown.first.inspect
      raise ArgumentError, "Unknown attribute #{unknown_name} for #{klass_name}. Known: #{known_list}"
    end

    def collection_name_for_klass
      return @klass.collection if @klass.respond_to?(:collection) && @klass.collection

      begin
        mapping = SearchEngine::Registry.mapping
        found = mapping.find { |(_, kls)| kls == @klass }
        return found.first if found
      rescue StandardError
        nil
      end

      raise ArgumentError, "Unknown collection for #{klass_name_for_inspect}"
    end

    def client
      # Prefer legacy ivar when explicitly set (tests or injected stubs), otherwise memoize with conventional name
      return @__client if instance_variable_defined?(:@__client) && @__client

      @client ||= (SearchEngine.config.respond_to?(:client) && SearchEngine.config.client) || SearchEngine::Client.new
    end

    def build_url_opts
      opts = @state[:options] || {}
      url = {}
      url[:use_cache] = option_value(opts, :use_cache) if opts.key?(:use_cache) || opts.key?('use_cache')
      if opts.key?(:cache_ttl) || opts.key?('cache_ttl')
        url[:cache_ttl] = begin
          Integer(option_value(opts, :cache_ttl))
        rescue StandardError
          nil
        end
      end
      url.compact
    end

    # pluck helpers reside in Materializers

    def curated_indices_for_current_result
      @__result_memo.to_a.each_with_index.select do |obj, _idx|
        obj.respond_to?(:curated_hit?) && obj.curated_hit?
      end.map(&:last)
    end

    def curation_filter_curated_hits?
      @state[:curation] && @state[:curation][:filter_curated_hits]
    end

    def enforce_hit_validator_if_needed!(total_hits, collection: nil)
      hl = @state[:hit_limits]
      return unless hl && hl[:max]

      th = total_hits.to_i
      max = hl[:max].to_i
      return unless th > max && max.positive?

      coll = collection || begin
        collection_name_for_klass
      rescue StandardError
        nil
      end

      msg = "HitLimitExceeded: #{th} results exceed max=#{max}"
      raise SearchEngine::Errors::HitLimitExceeded.new(
        msg,
        hint: 'Increase `validate_hits!(max:)` or narrow filters. Prefer `limit_hits(n)` to avoid work when supported.',
        doc: 'docs/hit_limits.md#validation',
        details: { total_hits: th, max: max, collection: coll, relation_summary: inspect }
      )
    end

    def append_boolean_knobs_explain_lines(lines)
      lines << "  use_synonyms: #{@state[:use_synonyms]}" if @state.key?(:use_synonyms) && !@state[:use_synonyms].nil?
      return unless @state.key?(:use_stopwords) && !@state[:use_stopwords].nil?

      lines << "  use_stopwords: #{@state[:use_stopwords]} (maps to remove_stop_words=#{!@state[:use_stopwords]})"
    end

    def append_where_and_order_lines(lines, params)
      if params[:filter_by] && !params[:filter_by].to_s.strip.empty?
        where_str = friendly_where(params[:filter_by].to_s)
        lines << "  where: #{where_str}"
      end
      lines << "  order: #{params[:sort_by]}" if params[:sort_by] && !params[:sort_by].to_s.strip.empty?
    end

    def append_grouping_explain_lines(lines)
      if (g = @state[:grouping])
        gparts = ["group_by=#{g[:field]}"]
        gparts << "limit=#{g[:limit]}" if g[:limit]
        gparts << 'missing_values=true' if g[:missing_values]
        lines << "  group: #{gparts.join(' ')}"
      end
    end
  end
end
