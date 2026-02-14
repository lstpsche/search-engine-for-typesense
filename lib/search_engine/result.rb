# frozen_string_literal: true

module SearchEngine
  # Result wraps a Typesense search response and exposes hydrated hits.
  #
  # Hydration converts each hit's document into either an instance of the
  # provided model class or a generic OpenStruct when no class is available.
  #
  # - Enumeration yields hydrated objects (includes Enumerable)
  # - Metadata readers: {#found}, {#out_of}, {#facets}, {#raw}
  # - Selection is respected implicitly by hydrating only keys present in the
  #   returned document; no missing attributes are synthesized.
  #
  # Unknown collections: when +klass+ is +nil+, hydration falls back to
  # OpenStruct.
  class Result
    include Enumerable

    # Immutable lightweight group record for grouped responses.
    #
    # @!attribute [r] key
    #   @return [Hash{String=>Object}] mapping of field name to group value
    # @!attribute [r] hits
    #   @return [Array<Object>] hydrated hits within the group
    # @!attribute [r] size
    #   @return [Integer] number of hits in the group
    class Group
      attr_reader :key, :hits

      # @param key [Hash{String=>Object}]
      # @param hits [Array<Object>]
      def initialize(key:, hits:)
        @key = (key || {}).dup.freeze
        @hits = Array(hits).freeze
        freeze
      end

      # @return [Integer]
      def size
        @hits.size
      end

      # @return [String]
      def inspect
        "#<SearchEngine::Result::Group key=#{key.inspect} size=#{size}>"
      end

      def ==(other)
        other.is_a?(Group) && other.key == key && other.hits == hits
      end
    end

    # @return [Array<Object>] hydrated hits (frozen internal array)
    # @return [Integer] number of documents that matched the search
    # @return [Integer] number of documents searched
    # @return [Array<Hash>, nil] facet counts as returned by Typesense
    # @return [Hash] raw Typesense response (unmodified)
    attr_reader :hits, :found, :out_of, :raw

    # Build a new result wrapper.
    #
    # @param raw [Hash] Parsed Typesense response ("hits"/"grouped_hits", "found", "out_of", "facet_counts")
    # @param klass [Class, nil] Optional model class used to hydrate each document
    # @param selection [Hash, nil] Optional selection context for strict missing checks
    # @param facets [Hash, nil] Optional facets context carrying declared facet queries/labels
    # @param highlight [Hash, nil] Optional highlight context carrying configured tags and knobs
    def initialize(raw, klass: nil, selection: nil, facets: nil, highlight: nil)
      require 'ostruct'

      @raw   = raw || {}
      @found = @raw['found'] || @raw[:found]
      @out_of = @raw['out_of'] || @raw[:out_of]
      # raw facet_counts preserved in @raw; parsed via #facets helper
      @klass  = klass
      @selection_ctx = selection if selection
      @facets_ctx = facets if facets
      @highlight_ctx = highlight if highlight

      @__groups_memo = nil
      # Precompute facets memo before freeze to avoid later mutation
      @__facets_parsed_memo = build_parsed_facets(@raw, @facets_ctx).freeze

      if grouped?
        groups_built = build_groups
        @__groups_memo = groups_built.freeze
        first_hits = groups_built.map { |g| g.hits.first }.compact
        @hits = first_hits.freeze
        instrument_group_parse(groups_built)
      else
        entries = Array(@raw['hits'] || @raw[:hits]).map { |h| symbolize_hit(h) }
        hydrated = []
        entries.each do |entry|
          next unless entry[:document]

          obj = hydrate(entry[:document])
          attach_highlighting!(obj, entry)
          hydrated << obj
        end
        @hits = hydrated.freeze
      end

      freeze
    end

    # Iterate over hydrated hits.
    # @yieldparam obj [Object] hydrated object
    # @return [Enumerator] when no block is given
    def each(&block)
      return @hits.each unless block_given?

      @hits.each(&block)
    end

    # @return [Array<Object>] a shallow copy of hydrated hits
    def to_a
      @hits.dup
    end

    # @return [Integer]
    def size
      @hits.size
    end

    # @return [Boolean]
    def empty?
      @hits.empty?
    end

    # Whether this result represents a grouped response.
    # Detection prefers presence and Array-ness of a grouped section.
    # @return [Boolean]
    def grouped?
      gh = @raw['grouped_hits'] || @raw[:grouped_hits]
      gh.is_a?(Array)
    end

    # Groups for grouped responses. Returns an empty Array when not grouped.
    # The returned Array is frozen; each Group is immutable.
    # @return [Array<SearchEngine::Result::Group>]
    def groups
      return [].freeze unless grouped?

      @__groups_memo.dup
    end

    # Enumerate over groups. Returns an Enumerator when no block given.
    # Empty enumerator when not grouped.
    # @yieldparam group [SearchEngine::Result::Group]
    # @return [Enumerator]
    def each_group(&block)
      return enum_for(:each_group) unless block_given?

      groups.each(&block)
    end

    # Number of groups present in this result page.
    # When grouping is disabled, returns 0.
    # @return [Integer]
    # @example
    #   res = SearchEngine::Product.group_by(:brand_id, limit: 1).execute
    #   res.groups_count #=> number of groups in this page
    def groups_count
      return 0 unless grouped?

      @__groups_memo.size
    end

    # Total documents found by the backend for this query (not page-limited).
    # Reads the backend-provided scalar (e.g., Typesense's `found`).
    # @return [Integer, nil]
    # @example
    #   res = SearchEngine::Product.group_by(:brand_id, limit: 1).execute
    #   res.total_found #=> total documents found
    def total_found
      @found
    end

    # Total number of groups for this query.
    # If the backend exposes a total groups count, returns that value.
    # Otherwise, falls back to the number of groups in the current page
    # (i.e., {#groups_count}). When grouping is disabled, returns +nil+.
    # @return [Integer, nil]
    # @example
    #   res = SearchEngine::Product.group_by(:brand_id, limit: 1).execute
    #   res.total_groups #=> global groups if available; else groups_count (page-scoped)
    def total_groups
      return nil unless grouped?

      api_total = detect_total_groups_from_raw(@raw)
      api_total.nil? ? @__groups_memo.size : api_total
    end

    # First group in this page or +nil+ when there are no groups.
    # Returns a reference to the memoized group; no new objects are allocated.
    # @return [SearchEngine::Result::Group, nil]
    def first_group
      return nil unless grouped?

      @__groups_memo.first
    end

    # Last group in this page or +nil+ when there are no groups.
    # Returns a reference to the memoized group; no new objects are allocated.
    # @return [SearchEngine::Result::Group, nil]
    def last_group
      return nil unless grouped?

      @__groups_memo.last
    end

    # Facets helpers
    # ---------------
    #
    # Parse Typesense facet_counts into a stable Hash mapping field => [ { value:, count:, highlighted:, label: } ].
    # Returns an empty Hash when no facets are present.
    # Arrays/hashes in the returned structure are defensive copies and can be safely mutated by callers.
    # @return [Hash{String=>Array<Hash{Symbol=>Object}>}]
    def facets
      parsed = parse_facets
      parsed.dup
    end

    # Facet values for a given field name.
    # @param name [#to_s]
    # @return [Array<Hash{Symbol=>Object}>]
    def facet_values(name)
      field = name.to_s
      arr = parse_facets[field] || []
      arr.dup
    end

    # Optional convenience: map of value => count for a given facet field.
    # @param name [#to_s]
    # @return [Hash{Object=>Integer}]
    def facet_value_map(name)
      facet_values(name).each_with_object({}) { |h, acc| acc[h[:value]] = h[:count] }
    end

    private

    # Per-hit highlighting mixin: added onto hydrated objects.
    module HitHighlighting
      # @return [Hash{String=>Array<Hash{Symbol=>Object}>}] normalized highlights by field
      def highlights
        h = instance_variable_get(:@__se_highlights_map__)
        h ? h.dup : {}
      end

      # Return a sanitized HTML snippet or full highlighted value for a field.
      # @param field [Symbol, String]
      # @param full [Boolean] when true, prefer full highlighted value
      # @return [String] HTML-safe string; SafeBuffer when ActiveSupport present
      def snippet_for(field, full: false)
        map = instance_variable_get(:@__se_highlights_map__)
        return nil unless map && field

        key = field.to_s
        list = map[key]
        return nil unless Array(list).any?

        ctx = instance_variable_get(:@__se_highlight_ctx__)
        entry = if full
                  list.find { |h| h[:snippet] == false } || list.first
                else
                  list.find { |h| h[:snippet] == true } || list.first
                end

        return nil unless entry

        value = entry[:value].to_s
        matched = Array(entry[:matched_tokens]).map(&:to_s)
        ctx && ctx[:affix_tokens]
        threshold = ctx && ctx[:snippet_threshold]

        html = SearchEngine::Result.send(:sanitize_highlight_html, value, ctx)
        return SearchEngine::Result.send(:wrap_safe_if_rails, html) if entry[:snippet] == true

        # Full value requested or only full value available
        return SearchEngine::Result.send(:wrap_safe_if_rails, html) if full || threshold.nil?

        # Compute a minimal snippet when server didn't provide one
        snippet = SearchEngine::Result.send(:compute_snippet_from_full, html, matched, ctx)
        SearchEngine::Result.send(:wrap_safe_if_rails, snippet)
      end
    end

    def parse_facets
      @__facets_parsed_memo || {}.freeze
    end

    def build_parsed_facets(raw, ctx)
      raw_facets = (raw && (raw['facet_counts'] || raw[:facet_counts])) || []
      result = {}
      Array(raw_facets).each do |entry|
        field = (entry['field_name'] || entry[:field_name]).to_s
        next if field.empty?

        values = Array(entry['counts'] || entry[:counts])
        list = build_facet_value_list(values)

        if ctx && Array(ctx[:queries]).any?
          q_for_field = Array(ctx[:queries]).select { |q| (q[:field] || q['field']).to_s == field }
          annotate_labels_for_field!(list, q_for_field) if q_for_field.any?
        end

        result[field] = list.freeze
      end

      result
    end

    def build_facet_value_list(values)
      Array(values).map do |v|
        value = v['value'] || v[:value]
        count = v['count'] || v[:count]
        highlighted = v['highlighted'] || v[:highlighted]
        { value: value, count: Integer(count || 0), highlighted: highlighted, label: nil }
      end
    end

    def annotate_labels_for_field!(list, queries)
      list.each do |h|
        val_str = h[:value].to_s
        match = queries.find { |q| (q[:expr] || q['expr']).to_s == val_str }
        h[:label] = ((match && (match[:label] || match['label'])) || nil)
      end
    end

    # Attempt to read a total groups count from the raw payload using common keys.
    # Returns +nil+ when the backend does not provide a value.
    # @param raw [Hash]
    # @return [Integer, nil]
    def detect_total_groups_from_raw(raw)
      keys = %w[total_groups group_count groups_count found_groups total_group_count total_grouped total_group_matches]
      keys.each do |key|
        val = raw[key] || raw[key.to_sym]
        next if val.nil?
        return Integer(val) if val.is_a?(Integer) || (val.is_a?(String) && val.match?(/\A-?\d+\z/))
      end
      nil
    rescue StandardError
      nil
    end

    # Hydrate a single Typesense document (Hash) into a Ruby object.
    #
    # If +@klass+ is present, an instance of that class is allocated and each
    # document key is assigned as an instance variable on the object. No reader
    # methods are generated; callers may access via the model's own readers (if
    # defined) or via reflection. Unknown keys are permitted.
    #
    # If +@klass+ is +nil+, an OpenStruct is created with the same keys.
    #
    # @param doc [Hash]
    # @return [Object]
    def hydrate(doc)
      keys = doc.is_a?(Hash) ? doc.keys.map(&:to_s) : []
      enforce_strict_missing_if_needed!(keys)
      if @klass
        if @klass.respond_to?(:from_document)
          @klass.from_document(doc)
        else
          @klass.new.tap do |obj|
            doc.each do |key, value|
              obj.instance_variable_set(ivar_name(key), value)
            end
          end
        end
      else
        OpenStruct.new(doc)
      end
    end

    # Build Group objects from the raw grouped response.
    # Preserves backend order and hydrates documents once.
    # @return [Array<SearchEngine::Result::Group>]
    def build_groups
      grouped = @raw['grouped_hits'] || @raw[:grouped_hits] || []
      fields = group_by_fields_from_raw

      grouped.map do |entry|
        key_values = Array(entry['group_key'] || entry[:group_key])
        key_hash = build_group_key_hash(fields, key_values)

        subhits = Array(entry['hits'] || entry[:hits])
        hydrated = []
        subhits.each do |sub|
          doc = sub && (sub['document'] || sub[:document])
          next unless doc

          obj = hydrate(doc)
          attach_highlighting!(obj, symbolize_hit(sub))
          hydrated << obj
        end

        Group.new(key: key_hash, hits: hydrated)
      end
    end

    # Derive group_by fields from echoed request params when available.
    # Returns an Array of field names (Strings). Empty when unknown.
    def group_by_fields_from_raw
      params = @raw['request_params'] || @raw[:request_params] || @raw['search_params'] || @raw[:search_params]
      return [] unless params

      gb = params['group_by'] || params[:group_by]
      return [] unless gb.is_a?(String) && !gb.strip.empty?

      gb.split(',').map!(&:strip).tap { |a| a.reject!(&:empty?) }
    end

    # Build a Hash mapping field names to coerced group key values.
    # Falls back to a single-field synthetic key when fields are unknown.
    def build_group_key_hash(fields, values)
      return {} if values.empty?

      if fields.any?
        out = {}
        fields.each_with_index do |field, idx|
          break if idx >= values.size

          out[field.to_s] = coerce_group_value(values[idx])
        end
        return out
      end

      return { 'group' => coerce_group_value(values.first) } if values.size == 1

      out = {}
      values.each_with_index do |val, idx|
        out["group_#{idx}"] = coerce_group_value(val)
      end
      out
    end

    # Best-effort coercion for common scalar types.
    def coerce_group_value(value)
      return nil if value.nil?

      return true if value == true || value.to_s == 'true'
      return false if value == false || value.to_s == 'false'
      return Integer(value) if value.is_a?(String) && value.match?(/\A-?\d+\z/)
      return Float(value) if value.is_a?(String) && value.match?(/\A-?\d+\.\d+\z/)

      value
    end

    def ivar_name(key)
      @ivar_prefix_cache ||= {}
      @ivar_prefix_cache[key] ||= "@#{key}"
    end

    def instrument_group_parse(groups)
      count = groups.size
      total = groups.inject(0) { |acc, g| acc + g.size }
      avg = count.positive? ? (total.to_f / count) : 0.0
      coll = begin
        @klass.respond_to?(:collection) ? @klass.collection : nil
      rescue StandardError
        nil
      end

      SearchEngine::Instrumentation.instrument(
        'search_engine.result.grouped_parsed',
        collection: coll,
        groups_count: count,
        avg_group_size: avg
      )
    end

    # Enforce strict-missing behavior when enabled.
    # Computes missing = requested_root − present_keys and raises when non-empty.
    def enforce_strict_missing_if_needed!(present_keys)
      ctx = @selection_ctx || {}
      strict = (ctx[:strict_missing] == true)
      return unless strict

      requested = Array(ctx[:requested_root]).map(&:to_s).reject(&:empty?)
      return if requested.empty?

      missing = requested - present_keys
      return if missing.empty?

      model_name = begin
        @klass&.name || 'Object'
      rescue StandardError
        'Object'
      end

      sample = missing.take(3)
      more = missing.size - sample.size
      sample_str = sample.map { |f| %("#{f}") }.join(', ')
      sample_str << " (+#{more} more)" if more.positive?

      msg = 'MissingField: requested fields absent for ' \
            "#{model_name}: #{sample_str}. " \
            'They may be excluded by selection or upstream Typesense mapping. ' \
            'Fix by adjusting select/exclude/reselect, relaxing strictness, or ' \
            'ensuring the mapping includes these fields.'
      raise SearchEngine::Errors::MissingField.new(
        msg,
        hint: 'Adjust select/exclude or disable strict_missing to avoid raising.',
        doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/field-selection#strict-vs-lenient-selection',
        details: { requested: requested, present_keys: present_keys }
      )
    end

    # --- Highlight internals -------------------------------------------------

    def symbolize_hit(h)
      return {} unless h.is_a?(Hash)

      out = {}
      h.each { |k, v| out[k.is_a?(String) ? k.to_sym : k] = v }
      out
    rescue StandardError
      {}
    end

    def attach_highlighting!(obj, hit_entry)
      raw_list = Array(hit_entry[:highlights])
      return obj if raw_list.empty?

      map = normalize_highlights(raw_list)
      return obj if map.empty?

      # Extend object once and inject context + normalized map
      obj.extend(HitHighlighting) unless obj.singleton_class.included_modules.include?(HitHighlighting)
      obj.instance_variable_set(:@__se_highlights_map__, map)
      obj.instance_variable_set(:@__se_highlight_ctx__, safe_highlight_ctx)
      obj
    rescue StandardError
      obj
    end

    def safe_highlight_ctx
      ctx = @highlight_ctx || {}
      return {} unless ctx.is_a?(Hash)

      out = {}
      out[:fields] = Array(ctx[:fields]).map(&:to_s).reject(&:empty?) if ctx[:fields]
      out[:full_fields] = Array(ctx[:full_fields]).map(&:to_s).reject(&:empty?) if ctx[:full_fields]
      out[:start_tag] = ctx[:start_tag].to_s if ctx[:start_tag]
      out[:end_tag] = ctx[:end_tag].to_s if ctx[:end_tag]
      out[:affix_tokens] = ctx[:affix_tokens] if ctx.key?(:affix_tokens)
      out[:snippet_threshold] = ctx[:snippet_threshold] if ctx.key?(:snippet_threshold)
      out
    rescue StandardError
      {}
    end

    def normalize_highlights(list)
      result = {}
      Array(list).each do |h|
        field = (h['field'] || h[:field]).to_s
        next if field.empty?

        value = h['snippet'] || h[:snippet] || h['value'] || h[:value]
        h.key?('snippet') || h.key?(:snippet)
        snippet_flag = !(h['snippet'] || h[:snippet]).nil?
        tokens = h['matched_tokens'] || h[:matched_tokens] || []

        entry = {
          value: value.to_s,
          matched_tokens: Array(tokens).map(&:to_s),
          snippet: snippet_flag
        }

        (result[field] ||= []) << entry
      end

      result
    rescue StandardError
      {}
    end

    class << self
      # Replace allowed highlight tags with placeholders, escape HTML, then restore configured tags.
      def sanitize_highlight_html(text, ctx)
        s = text.to_s
        return '' if s.empty?

        start_tag = (ctx && ctx[:start_tag]) || '<mark>'
        end_tag   = (ctx && ctx[:end_tag])   || '</mark>'
        placeholders = {
          start: "\u0001__SE_HL_START__\u0001",
          end:   "\u0001__SE_HL_END__\u0001"
        }

        # Normalize known tokens from server (<mark>) and configured ones
        start_tokens = [start_tag, '<mark>'].uniq
        end_tokens   = [end_tag, '</mark>'].uniq
        start_tokens.each { |tok| s = s.gsub(tok, placeholders[:start]) }
        end_tokens.each   { |tok| s = s.gsub(tok, placeholders[:end]) }

        # Escape everything
        require 'cgi'
        escaped = CGI.escapeHTML(s)

        # Restore configured tags; any other tags remain escaped
        escaped = escaped.gsub(placeholders[:start], start_tag)
        escaped.gsub(placeholders[:end], end_tag)
      rescue StandardError
        text.to_s
      end

      # Build a minimal snippet around first matched token when server didn't provide one
      def compute_snippet_from_full(html, matched_tokens, ctx)
        plain = strip_tags_preserving_space(html)
        return sanitize_highlight_html(html, ctx) if plain.empty?

        tokens = tokenize(plain)
        return sanitize_highlight_html(html, ctx) if tokens.empty?

        # Find first occurrence of any matched token (case-insensitive)
        target = matched_tokens.find { |t| !t.to_s.strip.empty? }
        return sanitize_highlight_html(html, ctx) unless target

        target_down = target.downcase
        idx = tokens.index { |t| t.downcase.include?(target_down) } || 0
        window = (ctx && ctx[:affix_tokens]).to_i
        window = 8 if window.negative? || window.zero?
        left = [idx - window, 0].max
        right = [idx + window, tokens.size - 1].min
        segment = tokens[left..right].join(' ')
        # Wrap the first occurrence with configured tags
        start_tag = (ctx && ctx[:start_tag]) || '<mark>'
        end_tag   = (ctx && ctx[:end_tag])   || '</mark>'
        highlighted = segment.sub(/(#{Regexp.escape(target)})/i, "#{start_tag}\\1#{end_tag}")
        sanitize_highlight_html(highlighted, ctx)
      rescue StandardError
        sanitize_highlight_html(html, ctx)
      end

      def strip_tags_preserving_space(html)
        s = html.to_s
        # Remove any HTML tags
        s.gsub(/<[^>]*>/, '')
      rescue StandardError
        html.to_s
      end

      def tokenize(text)
        s = text.to_s
        return [] if s.empty?

        tokens = s.split(/\s+/)
        # Fast-path: if there are no empty tokens, return as-is
        return tokens unless tokens.any?(&:empty?)

        tokens.reject!(&:empty?)
        tokens
      end

      def wrap_safe_if_rails(html)
        if defined?(ActiveSupport::SafeBuffer)
          ActiveSupport::SafeBuffer.new(html.to_s)
        else
          html.to_s
        end
      end
    end
  end
end
