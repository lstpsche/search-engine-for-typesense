# frozen_string_literal: true

module SearchEngine
  class Relation
    # Compile immutable relation state and options into Typesense body params.
    # This module is pure/deterministic and avoids any network I/O.
    module Compiler
      # Compile immutable relation state and options into Typesense body params.
      #
      # Pure function w.r.t. relation state; performs no I/O and emits
      # redaction-aware instrumentation events for DX surfaces.
      #
      # @return [SearchEngine::CompiledParams] deterministic, deeply frozen params
      # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/query-dsl`
      # @see `https://typesense.org/docs/latest/api/documents.html#search-document`
      def to_typesense_params
        cfg = SearchEngine.config
        opts = @state[:options] || {}

        params = {}
        runtime_flags = {}

        # Query basics
        apply_query_basics!(params, opts, cfg)

        # Filters and sorting
        ast_nodes = Array(@state[:ast]).flatten.compact
        SearchEngine::Instrumentation.monotonic_ms if defined?(SearchEngine::Instrumentation)
        filter_str = assign_filter_by!(params, ast_nodes)

        orders = Array(@state[:orders])
        sort_str = assign_sort_by!(params, orders)

        # Field selection and instrumentation
        include_str, exclude_str = compile_selection_fields!(params)
        instrument_selection_compile(include_str, exclude_str)

        # Highlighting
        apply_highlighting!(params)

        # Faceting
        apply_faceting!(params)

        # Curation (body params only)
        apply_curation!(params)

        # Pagination and early limit (compiler mapping)
        hits_info = apply_pagination_and_hit_limits!(params)

        # Grouping
        apply_grouping!(params)

        # Keep infix last for stability; include when configured or overridden
        apply_infix!(params, opts, cfg)

        # Ranking & typo tuning — authoritative mapping
        apply_ranking!(params)

        # Internal join context (for downstream components; may be stripped before HTTP)
        compile_started_ms = SearchEngine::Instrumentation.monotonic_ms
        join_ctx = build_join_context(ast_nodes: ast_nodes, orders: orders)
        params[:_join] = join_ctx unless join_ctx.nil? || join_ctx.empty?
        instrument_join_compile(join_ctx, include_str, filter_str, sort_str, compile_started_ms)

        # Preset emission and merge strategies
        params = apply_presets!(params)

        # Synonyms/Stopwords toggles
        apply_text_processing_flags!(params, runtime_flags)

        # Attach internal-only runtime flags preview
        params[:_runtime_flags] = runtime_flags unless runtime_flags.empty?

        # Attach internal-only hit limits preview for DX surfaces; stripped client-side
        attach_hits_info!(params, hits_info)

        SearchEngine::CompiledParams.new(params)
      end

      # Compile filter_by string from AST nodes or legacy fragments.
      # @param ast_nodes [Array<SearchEngine::AST::Node>]
      # @return [String, nil] a Typesense filter string or nil when absent
      # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/compiler`
      def compiled_filter_by(ast_nodes)
        unless ast_nodes.empty?
          compiled = SearchEngine::Compiler.compile(ast_nodes, klass: @klass)
          return nil if compiled.to_s.empty?

          return compiled
        end

        fragments = Array(@state[:filters])
        return nil if fragments.empty?

        fragments.join(' && ')
      end

      # Compile sort_by from normalized order entries.
      # @param orders [Array<String>]
      # @return [String, nil] comma-separated sort tokens or nil
      def compiled_sort_by(orders)
        list = Array(orders)
        return nil if list.empty?

        list.join(',')
      end

      # Build include_fields string with nested association segments first, then base fields.
      def compile_include_fields_string
        include_nested_map = @state[:select_nested] || {}
        include_base = Array(@state[:select])

        exclude_base = Array(@state[:exclude])
        exclude_nested_map = @state[:exclude_nested] || {}

        base_segment = include_base.empty? ? [] : (include_base - exclude_base)

        applied_joins = joins_list
        nested_segments = []
        Array(@state[:select_nested_order]).each do |assoc|
          next unless applied_joins.include?(assoc)

          inc_fields = Array(include_nested_map[assoc])
          next if inc_fields.empty?

          exc_fields = Array(exclude_nested_map[assoc])
          # Full-association exclusion sentinel wins: omit nested include segment entirely
          next if exc_fields.include?(:__all) || exc_fields.map(&:to_s).include?('__all')

          fields = (inc_fields - exc_fields).map(&:to_s).reject(&:empty?)
          fields = fields.sort
          next if fields.empty?

          nested_segments << "$#{assoc}(#{fields.join(',')})"
        end

        segments = []
        segments.concat(nested_segments)
        segments.concat(base_segment) unless base_segment.empty?

        segments.join(',')
      end

      # Build exclude_fields string with nested association segments first, then base fields.
      def compile_exclude_fields_string
        exclude_nested_order = Array(@state[:exclude_nested_order])
        exclude_nested_map = @state[:exclude_nested] || {}
        exclude_base = Array(@state[:exclude])

        include_base = Array(@state[:select])
        base_part = include_base.empty? ? exclude_base : []

        segments = []

        include_nested_map = @state[:select_nested] || {}

        exclude_nested_order.each do |assoc|
          exc_raw = Array(exclude_nested_map[assoc])
          # If full-association exclusion sentinel present, emit "$assoc(*,doc_updated_at)" regardless of includes
          if exc_raw.include?(:__all) || exc_raw.map(&:to_s).include?('__all')
            segments << "$#{assoc}(*,doc_updated_at)"
            next
          end

          # Otherwise, only emit nested exclude when there are no nested includes for this assoc
          next if Array(include_nested_map[assoc]).any?

          fields = exc_raw.map(&:to_s).reject(&:empty?)
          fields = fields.sort
          next if fields.empty?

          segments << "$#{assoc}(#{fields.join(',')})"
        end

        segments.concat(base_part) unless base_part.empty?
        segments.join(',')
      end

      # Build a JSON-serializable join context for Typesense.
      # @param ast_nodes [Array<SearchEngine::AST::Node>]
      # @param orders [Array<String>]
      # @return [Hash]
      def build_join_context(ast_nodes:, orders:)
        applied = Array(@state[:joins])
        return {} if applied.empty?

        assocs = []
        applied.each { |a| assocs << a unless assocs.include?(a) }

        nested_map = @state[:select_nested] || {}
        nested_order = Array(@state[:select_nested_order])

        fields_by_assoc = {}
        assocs.each do |assoc|
          fields = Array(nested_map[assoc]).map(&:to_s).reject(&:empty?)
          fields_by_assoc[assoc] = fields unless fields.empty?
        end

        include_refs = nested_order.select { |a| Array(nested_map[a]).any? }
        filter_refs = extract_assocs_from_ast(ast_nodes)
        sort_refs = extract_assocs_from_orders(orders)

        referenced_in = {}
        referenced_in[:include] = include_refs unless include_refs.empty?
        referenced_in[:filter] = filter_refs unless filter_refs.empty?
        referenced_in[:sort] = sort_refs unless sort_refs.empty?

        out = {}
        out[:assocs] = assocs unless assocs.empty?
        out[:fields_by_assoc] = fields_by_assoc unless fields_by_assoc.empty?
        out[:referenced_in] = referenced_in unless referenced_in.empty?
        out
      end

      # Walk AST nodes and collect association names used via "$assoc.field" LHS.
      # @param nodes [Array<SearchEngine::AST::Node>]
      # @return [Array<Symbol>] unique assoc names in first-mention order
      def extract_assocs_from_ast(nodes)
        list = Array(nodes).flatten.compact
        return [] if list.empty?

        seen = []
        walker = lambda do |node|
          return unless node.is_a?(SearchEngine::AST::Node)

          if node.respond_to?(:field)
            field = node.field.to_s
            if field.start_with?('$')
              m = field.match(/^\$(\w+)\./)
              if m
                name = m[1].to_sym
                seen << name unless seen.include?(name)
              end
            end
          end

          Array(node.children).each { |child| walker.call(child) }
        end

        list.each { |n| walker.call(n) }
        seen
      end

      # Parse order strings and collect assoc names used via "$assoc.field:dir".
      # @param orders [Array<String>]
      # @return [Array<Symbol>] unique assoc names in first-mention order
      def extract_assocs_from_orders(orders)
        list = Array(orders).flatten.compact
        return [] if list.empty?

        seen = []
        list.each do |entry|
          field, _dir = entry.to_s.split(':', 2)
          next unless field&.start_with?('$')

          m = field.match(/^\$(\w+)\./)
          next unless m

          name = m[1].to_sym
          seen << name unless seen.include?(name)
        end
        seen
      end

      # Helpers for inspect/DX
      def friendly_where(filter_by)
        SearchEngine::Relation::Dx::FriendlyWhere.render(filter_by)
      end

      def add_pagination_line!(lines, params)
        page = params[:page]
        per = params[:per_page]
        return unless page || per

        if page && per
          lines << "  page/per: #{page}/#{per}"
        elsif page
          lines << "  page/per: #{page}/"
        elsif per
          lines << "  page/per: /#{per}"
        end
      end

      def append_selection_inspect_parts(parts, compiled)
        selected_len = Array(@state[:select]).length
        parts << "select=#{selected_len}" unless selected_len.zero?

        inc_str = compiled[:include_fields]
        parts << %(sel="#{truncate_for_inspect(inc_str)}") if inc_str && !inc_str.to_s.empty?
        exc_str = compiled[:exclude_fields]
        parts << %(xsel="#{truncate_for_inspect(exc_str)}") if exc_str && !exc_str.to_s.empty?
      end

      def append_selection_explain_lines(lines, params)
        if params[:include_fields] && !params[:include_fields].to_s.strip.empty?
          lines << "  select: #{params[:include_fields]}"
        end
        return lines unless params[:exclude_fields] && !params[:exclude_fields].to_s.strip.empty?

        lines << "  exclude: #{params[:exclude_fields]}"
      end

      def append_curation_explain_lines(lines)
        cur = @state[:curation]
        return lines unless cur

        pinned = Array(cur[:pinned]).map(&:to_s).reject(&:empty?)
        hidden = Array(cur[:hidden]).map(&:to_s).reject(&:empty?)
        tags   = Array(cur[:override_tags]).map(&:to_s).reject(&:empty?)
        fch    = cur[:filter_curated_hits]

        lines << "  Pinned: #{pinned.join(', ')}" unless pinned.empty?
        lines << "  Hidden: #{hidden.join(', ')}" unless hidden.empty?
        lines << "  Override tags: #{tags.join(', ')}" unless tags.empty?
        lines << "  Filter curated hits: #{fch}" unless fch.nil?
        lines
      end

      def add_effective_selection_tokens!(lines)
        include_root = Array(@state[:select]).map(&:to_s)
        exclude_root = Array(@state[:exclude]).map(&:to_s)
        return if include_root.empty? && exclude_root.empty?

        effective = include_root.empty? ? include_root : (include_root - exclude_root)
        parts = ['selection:']
        parts << "sel=#{effective.join(',')}" if effective.any?
        parts << "xsel=#{exclude_root.join(',')}" if exclude_root.any?
        lines << "  #{parts.join(' ')}"
      end

      def append_preset_explain_line(lines, params)
        return lines unless @state[:preset_name]

        mode = @state[:preset_mode] || :merge
        if (conf = Array(params[:_preset_conflicts])) && !conf.empty?
          keys = conf.map(&:to_s).sort
          lines << "  preset: #{@state[:preset_name]} (mode=#{mode} dropped: #{keys.join(',')})"
        else
          lines << "  preset: #{@state[:preset_name]} (mode=#{mode})"
        end
        lines
      end

      # Instrument preset conflicts in :lock mode without affecting compile flow.
      # @param mode [Symbol]
      # @param name [String]
      # @param conflicts [Array<Symbol>]
      # @return [void]
      def instrument_preset_conflicts(mode, name, conflicts)
        return if Array(conflicts).empty?

        payload = {
          keys: Array(conflicts).map(&:to_sym).sort,
          mode: mode,
          preset_name: name,
          count: Array(conflicts).size
        }
        SearchEngine::Instrumentation.instrument('search_engine.preset.conflict', payload) {}
      rescue StandardError
        nil
      end

      private

      def apply_query_basics!(params, opts, cfg)
        q_val = option_value(opts, :q) || '*'
        model_qb = begin
          @klass.query_by if @klass.respond_to?(:query_by)
        rescue StandardError
          nil
        end
        query_by_val = option_value(opts, :query_by) || model_qb || cfg.default_query_by
        params[:q] = q_val
        params[:query_by] = query_by_val if query_by_val
      end

      def assign_filter_by!(params, ast_nodes)
        filter_str = compiled_filter_by(ast_nodes)
        filter_str = map_join_tokens(filter_str)
        params[:filter_by] = filter_str if filter_str
        filter_str
      end

      def assign_sort_by!(params, orders)
        sort_str = compiled_sort_by(orders)
        sort_str = map_join_tokens(sort_str)
        params[:sort_by] = sort_str if sort_str
        sort_str
      end

      def compile_selection_fields!(params)
        include_str = compile_include_fields_string
        exclude_str = compile_exclude_fields_string
        include_str = map_join_tokens(include_str)
        exclude_str = map_join_tokens(exclude_str)

        params[:include_fields] = include_str unless include_str.to_s.strip.empty?
        params[:exclude_fields] = exclude_str unless exclude_str.to_s.strip.empty?

        [include_str, exclude_str]
      end

      def map_join_tokens(value)
        return value if value.nil?

        str = value.to_s
        return value if str.strip.empty?

        rewrites = join_token_rewrites
        return value if rewrites.empty?

        mapped = str.dup
        rewrites.each do |assoc, collection|
          coll = collection.to_s
          assoc_token = assoc.to_s
          next if coll.strip.empty? || assoc_token.empty? || coll == assoc_token

          pattern = /\$#{Regexp.escape(assoc_token)}(?=[.(])/
          mapped = mapped.gsub(pattern, "$#{coll}")
        end

        mapped
      end

      def join_token_rewrites
        return {} unless @klass.respond_to?(:join_for)

        applied = joins_list
        return {} if applied.empty?

        applied.each_with_object({}) do |assoc, acc|
          cfg = @klass.join_for(assoc)
          coll = cfg[:collection].to_s
          next if coll.strip.empty?

          acc[assoc.to_sym] = coll
        rescue StandardError
          next
        end
      end

      def instrument_selection_compile(include_str, exclude_str)
        included_count = 0
        excluded_count = 0
        nested_assocs = []

        unless include_str.to_s.strip.empty?
          include_str.split(',').each do |segment|
            seg = segment.strip
            if (m = seg.match(/^\$(\w+)\(([^)]*)\)$/))
              assoc = m[1]
              inner = m[2]
              nested_assocs << assoc
              inner_fields = inner.to_s.split(',').map(&:strip).reject(&:empty?)
              included_count += inner_fields.length
            else
              included_count += 1
            end
          end
        end

        unless exclude_str.to_s.strip.empty?
          exclude_str.split(',').each do |segment|
            seg = segment.strip
            if (m = seg.match(/^\$(\w+)\(([^)]*)\)$/))
              assoc = m[1]
              inner = m[2]
              nested_assocs << assoc
              inner_fields = inner.to_s.split(',').map(&:strip).reject(&:empty?)
              excluded_count += inner_fields.length
            else
              excluded_count += 1
            end
          end
        end

        s_payload = {
          include_count: included_count,
          exclude_count: excluded_count,
          nested_assoc_count: nested_assocs.uniq.length
        }
        SearchEngine::Instrumentation.instrument('search_engine.selection.compile', s_payload) {}
      rescue StandardError
        # swallow observability errors
      end

      def apply_highlighting!(params)
        return unless (h = @state[:highlight])

        hf = Array(h[:fields]).map(&:to_s).reject(&:empty?)
        params[:highlight_fields] = hf.join(',') unless hf.empty?

        hff = Array(h[:full_fields]).map(&:to_s).reject(&:empty?)
        params[:highlight_full_fields] = hff.join(',') unless hff.empty?

        params[:highlight_start_tag] = h[:start_tag] if h[:start_tag]
        params[:highlight_end_tag] = h[:end_tag] if h[:end_tag]

        params[:highlight_affix_num_tokens] = h[:affix_tokens] unless h[:affix_tokens].nil?
        params[:snippet_threshold] = h[:snippet_threshold] unless h[:snippet_threshold].nil?
      end

      def apply_curation!(params)
        return unless (cur = @state[:curation])

        pinned = Array(cur[:pinned]).map(&:to_s).reject(&:empty?)
        hidden = Array(cur[:hidden]).map(&:to_s).reject(&:empty?)
        tags   = Array(cur[:override_tags]).map(&:to_s).reject(&:empty?)
        fch    = cur[:filter_curated_hits]

        params[:pinned_hits] = pinned.join(',') if pinned.any?
        params[:hidden_hits] = hidden.join(',') if hidden.any?
        params[:override_tags] = tags.join(',') if tags.any?
        params[:filter_curated_hits] = fch unless fch.nil?
        # Expose a compact curation meta segment for callers (not sent over HTTP)
        params[:_curation] = { filter_curated_hits: fch } if cur.key?(:filter_curated_hits)

        instrument_curation_compile(pinned, hidden, tags, cur)
      end

      def instrument_curation_compile(pinned, hidden, tags, cur)
        c_payload = {
          pinned_count: pinned.size.positive? ? pinned.size : nil,
          hidden_count: hidden.size.positive? ? hidden.size : nil,
          has_override_tags: tags.any? || nil,
          filter_curated_hits: (cur.key?(:filter_curated_hits) ? cur[:filter_curated_hits] : nil)
        }.compact
        SearchEngine::Instrumentation.instrument('search_engine.curation.compile', c_payload) {}

        overlap = (pinned & hidden)
        if overlap.any?
          SearchEngine::Instrumentation.instrument(
            'search_engine.curation.conflict',
            { type: :overlap, count: overlap.size }
          ) {}
        end
      rescue StandardError
        # swallow observability errors
      end

      def apply_pagination_and_hit_limits!(params)
        hits_info = {}
        pagination = compute_pagination
        if (hl = @state[:hit_limits]) && hl[:early_limit]
          if pagination.key?(:per_page) && pagination[:per_page].to_i > hl[:early_limit].to_i
            pagination = pagination.merge(per_page: hl[:early_limit].to_i)
            hits_info[:per_adjusted] = true
          else
            hits_info[:per_adjusted] = false
          end
          hits_info[:early_limit] = hl[:early_limit].to_i
        end
        params[:page] = pagination[:page] if pagination.key?(:page)
        params[:per_page] = pagination[:per_page] if pagination.key?(:per_page)
        hits_info
      end

      def apply_grouping!(params)
        grouping = @state[:grouping]
        return unless grouping

        field = grouping[:field]
        limit = grouping[:limit]
        missing_values = grouping[:missing_values]

        if field
          params[:group_by] = field.to_s
          params[:group_limit] = limit if limit
          params[:group_missing_values] = true if missing_values
        end

        instrument_grouping_compile(field, limit, missing_values)
      end

      def instrument_grouping_compile(field, limit, missing_values)
        payload = {
          collection: klass_name_for_inspect,
          field: field&.to_s,
          limit: limit,
          missing_values: missing_values
        }.compact
        SearchEngine::Instrumentation.instrument('search_engine.grouping.compile', payload) {}
      rescue StandardError
        # swallow observability errors
      end

      def apply_infix!(params, opts, cfg)
        infix_val = option_value(opts, :infix) || cfg.default_infix
        params[:infix] = infix_val if infix_val
      end

      def apply_ranking!(params)
        return unless (rk = @state[:ranking])

        plan = SearchEngine::RankingPlan.new(relation: self, query_by: params[:query_by], ranking: rk)
        rparams = plan.params
        params.merge!(rparams) unless rparams.empty?
      rescue SearchEngine::Errors::Error
        raise
      rescue StandardError => error
        raise SearchEngine::Errors::InvalidOption.new(
          "InvalidOption: ranking options could not be compiled (#{error.class}: #{error.message})",
          doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/ranking#options'
        )
      end

      def instrument_join_compile(join_ctx, include_str, filter_str, sort_str, compile_started_ms)
        assocs = Array(join_ctx[:assocs]).map(&:to_s)
        used = join_ctx[:referenced_in] || {}
        used_in = {}
        %i[include filter sort].each do |k|
          arr = Array(used[k]).map(&:to_s)
          used_in[k] = arr unless arr.empty?
        end

        payload = {
          collection: klass_name_for_inspect,
          join_count: assocs.size,
          assocs: (assocs unless assocs.empty?),
          used_in: (used_in unless used_in.empty?),
          include_len: (include_str.to_s.length unless include_str.to_s.strip.empty?),
          filter_len: (filter_str.to_s.length unless filter_str.to_s.strip.empty?),
          sort_len:   (sort_str.to_s.length unless sort_str.to_s.strip.empty?),
          duration_ms: (SearchEngine::Instrumentation.monotonic_ms - compile_started_ms if compile_started_ms),
          has_joins: !assocs.empty?
        }
        SearchEngine::Instrumentation.instrument('search_engine.joins.compile', payload)
      rescue StandardError
        # swallow observability errors
      end

      def apply_presets!(params)
        return params unless (pn = @state[:preset_name])

        pmode = (@state[:preset_mode] || :merge).to_sym
        params[:preset] = pn

        case pmode
        when :only
          allowed = ESSENTIAL_PARAM_KEYS
          minimal = {}
          (allowed + [:preset]).each do |k|
            minimal[k] = params[k] if params.key?(k)
          end
          # Preserve internal join context if present for observability
          minimal[:_join] = params[:_join] if params.key?(:_join)
          minimal
        when :lock
          conflicts = []
          locked = SearchEngine.config.presets.locked_domains_set
          params.each_key do |k|
            next unless locked.include?(k)

            params.delete(k)
            conflicts << k
          end
          params[:_preset_conflicts] = conflicts unless conflicts.empty?

          instrument_preset_conflicts(pmode, pn, conflicts)
          params
        else
          params
        end
      end

      def attach_hits_info!(params, hits_info)
        return unless (hl = @state[:hit_limits])

        hits_info[:max] = hl[:max].to_i if hl[:max]
        params[:_hits] = hits_info unless hits_info.empty?
      end

      # Faceting block extracted for clarity
      def apply_faceting!(params)
        facet_fields = Array(@state[:facet_fields]).map(&:to_s).reject(&:empty?)
        params[:facet_by] = facet_fields.join(',') unless facet_fields.empty?

        caps = Array(@state[:facet_max_values]).compact
        if caps.any?
          valid_caps = []
          caps.each do |v|
            valid_caps << Integer(v)
          rescue ArgumentError, TypeError
            # skip invalid cap
          end
          max_cap = valid_caps.max
          params[:max_facet_values] = max_cap if max_cap&.positive?
        end

        queries = Array(@state[:facet_queries])
        return unless queries.any?

        tokens = queries.map { |q| "#{q[:field]}:#{q[:expr]}" }
        params[:facet_query] = tokens.join(',') unless tokens.empty?
      end

      # Synonyms/stopwords toggles extracted for clarity
      def apply_text_processing_flags!(params, runtime_flags)
        unless @state[:use_synonyms].nil?
          params[:enable_synonyms] = @state[:use_synonyms]
          runtime_flags[:use_synonyms] = @state[:use_synonyms]
        end
        return if @state[:use_stopwords].nil?

        remove = !@state[:use_stopwords]
        params[:remove_stop_words] = remove
        runtime_flags[:use_stopwords] = @state[:use_stopwords]
      end
    end
  end
end
