# frozen_string_literal: true

module SearchEngine
  module Hydration
    # Centralized executors for materialization: to_a/each/count/ids/pluck.
    # Accepts a Relation instance; never mutates it; performs at most one HTTP call
    # per relation instance by reusing its internal memoization lock.
    module Materializers
      module_function

      # Execute the relation and return a Result, memoizing on the relation.
      # @param relation [SearchEngine::Relation]
      # @return [SearchEngine::Result]
      def execute(relation)
        loaded = relation.instance_variable_get(:@__loaded)
        memo = relation.instance_variable_get(:@__result_memo)
        return memo if loaded && memo

        load_lock = relation.instance_variable_get(:@__load_lock)
        load_lock.synchronize do
          loaded = relation.instance_variable_get(:@__loaded)
          memo = relation.instance_variable_get(:@__result_memo)
          return memo if loaded && memo

          collection = relation.send(:collection_name_for_klass)
          params = SearchEngine::CompiledParams.from(relation.to_typesense_params)
          url_opts = relation.send(:build_url_opts)

          raw_result = nil

          # Preflight client-side fallback (extracted for readability)
          preflight_raw, relation, params = preflight_join_fallback_if_needed(relation, params)

          begin
            raw_result = preflight_raw || relation.send(:client).search(
              collection: collection,
              params: params,
              url_opts: url_opts
            )
          rescue SearchEngine::Errors::Api => error
            # Graceful empty fallback for infix/prefix configuration errors
            if infix_missing_error?(error)
              empty = { 'hits' => [], 'found' => 0, 'out_of' => 0 }
              raw_result = SearchEngine::Result.new(empty, klass: relation.klass)
            else
              # Client-side join fallback: handle missing Typesense reference for joined filters
              raise unless join_reference_missing_error?(error) && Array(relation.joins_list).any?

              fallback_rel = build_client_side_join_fallback_relation(relation)
              if fallback_rel.equal?(:__empty__)
                # Short-circuit: no matches
                empty = { 'hits' => [], 'found' => 0, 'out_of' => 0 }
                raw_result = SearchEngine::Result.new(empty, klass: relation.klass)
              else
                # Retry with rewritten base relation
                new_params = SearchEngine::CompiledParams.from(fallback_rel.to_typesense_params)
                raw_result = relation.send(:client).search(collection: collection, params: new_params,
                                                           url_opts: url_opts
                )
                instrument_client_side_fallback(relation)
                # Replace relation for selection/facets context below
                relation = fallback_rel
              end
            end
          end

          selection_ctx = SearchEngine::Hydration::SelectionContext.build(relation)
          facets_ctx = build_facets_context_from_state(relation)

          result = if selection_ctx || facets_ctx
                     SearchEngine::Result.new(
                       raw_result.raw,
                       klass: relation.klass,
                       selection: selection_ctx,
                       facets: facets_ctx
                     )
                   else
                     raw_result
                   end

          relation.send(:enforce_hit_validator_if_needed!, result.found, collection: collection)
          relation.instance_variable_set(:@__result_memo, result)
          relation.instance_variable_set(:@__loaded, true)
          result
        end
      end

      # --- Public materializers (delegation targets) ------------------------

      # Return a shallow copy of hydrated hits.
      # @return [Array<Object>]
      def to_a(relation)
        result = execute(relation)
        result.to_a
      end

      # Lightweight preview for console rendering; fetches up to the given limit
      # without memoizing as the relation’s main result. Does not mutate relation state.
      # @param relation [SearchEngine::Relation]
      # @param limit [Integer]
      # @return [Array<Object>]
      def preview(relation, limit)
        limit = limit.to_i
        return [] unless limit.positive?

        if relation.send(:loaded?)
          memo = relation.instance_variable_get(:@__result_memo)
          return [] unless memo

          array = memo.respond_to?(:to_a) ? memo.to_a : Array(memo)
          return array.first(limit)
        end

        if relation.instance_variable_defined?(:@__preview_memo)
          cached = relation.instance_variable_get(:@__preview_memo)
          return Array(cached).first(limit)
        end

        # Use the relation's effective per_page for the underlying request to keep
        # ordering consistent with to_a; slice to the preview limit locally.
        per_for_request = effective_per_page(relation)
        per_for_request = limit if per_for_request.to_i <= 0

        preview_relation = relation.send(:spawn) do |state|
          state[:page] = 1
          state[:per_page] = per_for_request
        end

        collection = preview_relation.send(:collection_name_for_klass)
        params = SearchEngine::CompiledParams.from(preview_relation.to_typesense_params)
        url_opts = preview_relation.send(:build_url_opts)

        raw_result = perform_preview_search_with_fallback(preview_relation, collection, params, url_opts)

        selection_ctx = SearchEngine::Hydration::SelectionContext.build(preview_relation)
        facets_ctx = build_facets_context_from_state(preview_relation)

        result = if selection_ctx || facets_ctx
                   SearchEngine::Result.new(
                     raw_result.raw,
                     klass: preview_relation.klass,
                     selection: selection_ctx,
                     facets: facets_ctx
                   )
                 else
                   raw_result
                 end

        array = Array(result.respond_to?(:to_a) ? result.to_a : result).first(limit)
        relation.instance_variable_set(:@__preview_memo, array)
        array
      end

      # Internal: perform preview search, applying client-side fallback when Typesense
      # reports a missing reference for joined filters.
      # @param preview_relation [SearchEngine::Relation]
      # @param collection [String]
      # @param params [SearchEngine::CompiledParams]
      # @param url_opts [Hash]
      # @return [Object] raw result from client
      def perform_preview_search_with_fallback(preview_relation, collection, params, url_opts)
        preview_relation.send(:client).search(
          collection: collection,
          params: params,
          url_opts: url_opts
        )
      rescue SearchEngine::Errors::Api => error
        # Graceful empty fallback for infix/prefix configuration errors
        if infix_missing_error?(error)
          empty_raw = { 'hits' => [], 'found' => 0, 'out_of' => 0 }
          return SearchEngine::Result.new(empty_raw, klass: preview_relation.klass)
        end
        raise unless join_reference_missing_error?(error) && Array(preview_relation.joins_list).any?

        fallback_rel = build_client_side_join_fallback_relation(preview_relation)
        if fallback_rel.equal?(:__empty__)
          empty_raw = { 'hits' => [], 'found' => 0, 'out_of' => 0 }
          return SearchEngine::Result.new(empty_raw, klass: preview_relation.klass)
        end

        new_params = SearchEngine::CompiledParams.from(fallback_rel.to_typesense_params)
        res = preview_relation.send(:client).search(
          collection: collection,
          params: new_params,
          url_opts: url_opts
        )
        instrument_client_side_fallback(preview_relation)
        res
      end

      def each(relation, &block)
        arr = to_a(relation)
        block_given? ? arr.each(&block) : arr.each
      end

      def first(relation, n = nil)
        # Fast path: when relation has a single equality predicate on id and n=nil, use document GET
        if n.nil?
          id_value = detect_equality_id_predicate_value(relation)
          if id_value
            doc = retrieve_document_by_id(relation, id_value)
            return doc unless doc.nil?
          end
        end

        arr = to_a(relation)
        return arr.first if n.nil?

        arr.first(n)
      end

      def last(relation, n = nil)
        arr = to_a(relation)
        return arr.last if n.nil?

        arr.last(n)
      end

      def take(relation, n = 1)
        arr = to_a(relation)
        return arr.first if n == 1

        arr.first(n)
      end

      def ids(relation)
        pluck(relation, :id)
      end

      def pluck(relation, *fields)
        raise ArgumentError, 'pluck requires at least one field' if fields.nil? || fields.empty?

        names = coerce_pluck_field_names(fields)
        validate_pluck_fields_allowed!(relation, names)

        result = execute(relation)
        raw_hits = Array(result.raw['hits'])
        objects = result.to_a

        enforce_strict_for_pluck_row = lambda do |doc, requested|
          present_keys = doc.keys.map(&:to_s)
          if result.respond_to?(:send)
            ctx = result.instance_variable_get(:@selection_ctx) || {}
            if ctx[:strict_missing] == true
              result.send(:enforce_strict_missing_if_needed!, present_keys, requested_override: requested)
            end
          end
        end

        if names.length == 1
          field = names.first
          return objects.each_with_index.map do |obj, idx|
            doc = (raw_hits[idx] && raw_hits[idx]['document']) || {}
            # Enforce strict missing for the requested field against present keys
            enforce_strict_for_pluck_row.call(doc, [field])
            if obj.respond_to?(field)
              obj.public_send(field)
            else
              doc[field]
            end
          end
        end

        objects.each_with_index.map do |obj, idx|
          doc = (raw_hits[idx] && raw_hits[idx]['document']) || {}
          # Enforce strict missing for all requested fields against present keys
          enforce_strict_for_pluck_row.call(doc, names)
          names.map do |field|
            if obj.respond_to?(field)
              obj.public_send(field)
            else
              doc[field]
            end
          end
        end
      end

      def exists?(relation)
        loaded = relation.instance_variable_get(:@__loaded)
        memo = relation.instance_variable_get(:@__result_memo)
        return memo.found.to_i.positive? if loaded && memo

        fetch_found_only(relation).positive?
      end

      def count(relation)
        if relation.send(:curation_filter_curated_hits?)
          to_a(relation)
          return relation.send(:curated_indices_for_current_result).size
        end

        loaded = relation.instance_variable_get(:@__loaded)
        memo = relation.instance_variable_get(:@__result_memo)
        return memo.found.to_i if loaded && memo

        fetch_found_only(relation)
      end

      # Compute number of result pages based on total hits and per-page value.
      # @return [Integer]
      def pages_count(relation)
        total_hits = count(relation).to_i
        return 0 if total_hits <= 0

        per_page = effective_per_page(relation)
        return total_hits if per_page <= 0

        (total_hits.to_f / per_page).ceil
      end

      # --- internals --------------------------------------------------------

      def fetch_found_only(relation)
        collection = relation.send(:collection_name_for_klass)
        base = SearchEngine::CompiledParams.from(relation.to_typesense_params).to_h

        minimal = base.dup
        minimal[:per_page] = 1
        minimal[:page] = 1
        minimal[:include_fields] = 'id'

        url_opts = relation.send(:build_url_opts)
        begin
          result = relation.send(:client).search(collection: collection, params: minimal, url_opts: url_opts)
        rescue SearchEngine::Errors::Api => error
          return 0 if infix_missing_error?(error)
          # Client-side join fallback: handle missing Typesense reference for joined filters in count path
          raise unless join_reference_missing_error?(error) && Array(relation.joins_list).any?

          fallback_rel = build_client_side_join_fallback_relation(relation)
          return 0 if fallback_rel.equal?(:__empty__)

          new_params = SearchEngine::CompiledParams.from(fallback_rel.to_typesense_params).to_h
          new_minimal = new_params.dup
          new_minimal[:per_page] = 1
          new_minimal[:page] = 1
          new_minimal[:include_fields] = 'id'

          result = relation.send(:client).search(collection: collection, params: new_minimal, url_opts: url_opts)
          instrument_client_side_fallback(relation)
        end

        count = result.found.to_i
        relation.send(:enforce_hit_validator_if_needed!, count, collection: collection)
        count
      end
      module_function :fetch_found_only

      # Detect Typesense 400 errors caused by missing infix/prefix configuration
      # e.g., "Could not find `name` in the infix index. Make sure to enable infix search by specifying `infix: true` in the schema."
      def infix_missing_error?(error)
        return false unless error.is_a?(SearchEngine::Errors::Api)

        status = error.status.to_i
        return false unless status == 400

        body = error.body
        msg = error.message.to_s
        # Match common phrasing from Typesense for missing infix/prefix index
        need_infix = 'infix index'
        enable_infix = 'enable infix'
        could_not_find = 'Could not find'
        missing_prefix = 'prefix index'
        (
          body.is_a?(String) && (body.include?(need_infix) ||
          body.include?(enable_infix) || body.include?(missing_prefix))
        ) ||
          msg.include?(need_infix) || msg.include?(enable_infix) ||
          msg.include?(missing_prefix) || msg.include?(could_not_find)
      rescue StandardError
        false
      end

      # --- client-side join fallback helpers ---------------------------------

      # True when the relation uses joined fields in filters and the base
      # collection schema lacks a matching reference for at least one of
      # those associations.
      def join_fallback_preflight_required?(relation)
        state = relation.instance_variable_get(:@state) || {}
        ast_nodes = Array(state[:ast]).flatten.compact
        assocs = extract_join_assocs_from_ast(ast_nodes)
        return false if assocs.empty?

        base_klass = relation.klass
        compiled = SearchEngine::Schema.compile(base_klass)
        fields = Array(compiled[:fields])
        by_name = {}
        fields.each do |f|
          name = (f[:name] || f['name']).to_s
          by_name[name] = f
        end

        assocs.any? do |assoc|
          begin
            cfg = base_klass.join_for(assoc)
            lk = (cfg[:local_key] || '').to_s
            fk = (cfg[:foreign_key] || '').to_s
            coll = (cfg[:collection] || '').to_s
            expected = "#{coll}.#{fk}"
            entry = by_name[lk]
            actual = entry && (entry[:reference] || entry['reference'])
            actual_str = actual.to_s
            # Accept async suffix on actual
            next true if actual_str.empty? || !actual_str.start_with?(expected)
          rescue StandardError
            next true
          end
          false
        end
      rescue StandardError
        false
      end

      # Walk AST nodes and collect association names used via "$assoc.field".
      def extract_join_assocs_from_ast(nodes)
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

      # Attempt a client-side fallback rewrite before making a request when
      # joined filters are present but the base schema lacks the needed reference.
      # Returns [raw_result_or_nil, relation, params]
      def preflight_join_fallback_if_needed(relation, params)
        raw_result = nil
        try_fallback = begin
          join_fallback_preflight_required?(relation)
        rescue StandardError
          false
        end

        if try_fallback
          begin
            fallback_rel = build_client_side_join_fallback_relation(relation)
            if fallback_rel.equal?(:__empty__)
              empty = { 'hits' => [], 'found' => 0, 'out_of' => 0 }
              raw_result = SearchEngine::Result.new(empty, klass: relation.klass)
            else
              relation = fallback_rel
              params = SearchEngine::CompiledParams.from(relation.to_typesense_params)
              instrument_client_side_fallback(relation)
            end
          rescue StandardError
            # ignore and proceed
          end
        end
        [raw_result, relation, params]
      end

      def join_reference_missing_error?(error)
        return false unless error.is_a?(SearchEngine::Errors::Api)

        body = error.body
        msg = error.message.to_s
        needle = 'No reference field found'
        (body.is_a?(String) && body.include?(needle)) || msg.include?(needle)
      rescue StandardError
        false
      end

      def build_client_side_join_fallback_relation(relation)
        state = relation.instance_variable_get(:@state) || {}
        ast_nodes = Array(state[:ast]).flatten.compact
        joins = Array(state[:joins]).flatten.compact
        return relation if joins.empty?

        # Guard: sorting or selection on joined fields not supported in v1
        orders = Array(state[:orders]).map(&:to_s)
        if orders.any? { |o| o.start_with?('$') }
          raise SearchEngine::Errors::InvalidOption.new(
            'Sorting by joined fields is not supported by client-side join fallback',
            doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/joins#client-side-fallback'
          )
        end
        include_str = begin
          relation.send(:compile_include_fields_string)
        rescue StandardError
          nil
        end
        if include_str&.split(',')&.any? { |seg| seg.strip.start_with?('$') }
          raise SearchEngine::Errors::InvalidOption.new(
            'Selecting joined fields is not supported by client-side join fallback',
            doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/joins#client-side-fallback'
          )
        end

        # For each applied assoc, collect inner predicates (Eq/In only)
        per_assoc_inners = extract_join_inners(ast_nodes)
        return relation if per_assoc_inners.empty?

        # Resolve keys and fetch key sets via pre-query
        key_sets = {}
        base_klass = relation.klass
        joins.each do |assoc|
          cfg = base_klass.join_for(assoc)
          inners = per_assoc_inners[assoc] || []
          next if inners.empty?

          keys = fetch_keys_for_assoc(base_klass, cfg, inners)
          key_sets[assoc] = keys
        end

        # If any assoc produced no keys, the AND semantics imply empty
        return :__empty__ if key_sets.values.any? { |arr| Array(arr).empty? }

        # Rewrite AST: remove joined nodes and add base IN(local_key, keys) per assoc
        rewritten_ast = rewrite_ast_with_key_sets(ast_nodes, key_sets, base_klass)

        relation.send(:spawn) do |s|
          s[:ast] = rewritten_ast
          # NOTE: s[:filters] retained (base fragments only); joins preserved for DX
        end
      end

      def extract_join_inners(ast_nodes)
        map = {}
        walker = lambda do |node|
          return unless node.is_a?(SearchEngine::AST::Node)

          node.children.each { |ch| walker.call(ch) } if node.respond_to?(:children) && node.children

          if node.respond_to?(:field)
            field = node.field.to_s
            if (m = field.match(/^\$(\w+)\.(.+)$/))
              assoc = m[1].to_sym
              inner_field = m[2]
              case node
              when SearchEngine::AST::Eq
                (map[assoc] ||= []) << [:eq, inner_field, node.value]
              when SearchEngine::AST::In
                (map[assoc] ||= []) << [:in, inner_field, node.values]
              else
                # Unsupported node type for fallback (e.g., ranges, not_eq, etc.)
                raise SearchEngine::Errors::InvalidOption.new(
                  'Only equality and IN predicates on joined fields are supported by client-side join fallback',
                  doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/joins#client-side-fallback'
                )
              end
            end
          end
        end

        Array(ast_nodes).each { |n| walker.call(n) }
        map
      end

      def fetch_keys_for_assoc(base_klass, assoc_cfg, inners)
        require 'search_engine/joins/resolver'
        keys = SearchEngine::Joins::Resolver.resolve_keys(base_klass, assoc_cfg)
        collection = assoc_cfg[:collection]
        target_klass = SearchEngine.collection_for(collection)

        # Build target relation by AND-ing inner predicates
        rel = target_klass.all
        inners.each do |(op, field, value)|
          case op
          when :eq
            rel = rel.where(field.to_s => value)
          when :in
            rel = rel.where(field.to_s => Array(value))
          end
        end

        vals = rel.pluck(keys[:foreign_key])
        Array(vals).flatten.compact.uniq
      end

      def rewrite_ast_with_key_sets(ast_nodes, key_sets, base_klass)
        # Remove joined predicates and append base IN(local_key, keys) for each assoc
        stripped = strip_join_nodes(ast_nodes)
        added = []
        key_sets.each do |assoc, keys|
          cfg = base_klass.join_for(assoc)
          require 'search_engine/joins/resolver'
          lk = SearchEngine::Joins::Resolver.resolve_keys(base_klass, cfg)[:local_key]
          added << SearchEngine::AST.in_(lk.to_sym, keys)
        end
        (Array(stripped) + added).flatten.compact
      end

      def strip_join_nodes(nodes)
        out = []
        Array(nodes).each do |node|
          next unless node.is_a?(SearchEngine::AST::Node)

          case node
          when SearchEngine::AST::And
            children = strip_join_nodes(node.children)
            out.concat(Array(children))
          when SearchEngine::AST::Or
            # Fallback does not support OR with joined nodes; reject early
            raise SearchEngine::Errors::InvalidOption.new(
              'OR with joined predicates is not supported by client-side join fallback',
              doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/joins#client-side-fallback'
            )
          else
            if node.respond_to?(:field)
              f = node.field.to_s
              next if f.start_with?('$')
            end
            out << node
          end
        end
        out
      end

      def instrument_client_side_fallback(relation)
        return unless defined?(SearchEngine::Instrumentation)

        SearchEngine::Instrumentation.instrument(
          'search_engine.joins.client_side_fallback',
          collection: (relation.klass.respond_to?(:collection) ? relation.klass.collection : nil),
          joins: Array(relation.joins_list).map(&:to_s)
        )
      rescue StandardError
        nil
      end

      def effective_per_page(relation)
        state = relation.instance_variable_get(:@state) || {}
        per_page = state[:per_page]
        return per_page.to_i if per_page.to_i.positive?

        loaded = relation.instance_variable_get(:@__loaded)
        memo = relation.instance_variable_get(:@__result_memo)
        if loaded && memo
          request_params = memo.raw['request_params'] || memo.raw[:request_params]
          request_params ||= memo.raw['search_params'] || memo.raw[:search_params]
          if request_params
            per = request_params['per_page'] || request_params[:per_page]
            return per.to_i if per.to_i.positive?
          end

          return memo.hits.size if memo.respond_to?(:hits)
        end

        10
      end
      module_function :effective_per_page

      # Retrieve and hydrate all matching records by paging via multi-search.
      # Uses maximum per_page=250 to minimize requests and chunks batches under multi_search_limit.
      # @param relation [SearchEngine::Relation]
      # @return [Array<Object>]
      def all!(relation)
        total = count(relation)
        return [] if total.to_i <= 0

        per = 250
        pages = (total.to_f / per).ceil

        # Fast path for a single page
        return relation.page(1).per(per).to_a if pages <= 1

        limit = begin
          SearchEngine.config.multi_search_limit.to_i
        rescue StandardError
          50
        end
        limit = 1 if limit <= 0

        page_numbers = (1..pages).to_a
        out = []

        page_numbers.each_slice(limit) do |batch|
          mr = SearchEngine.multi_search_result do |m|
            batch.each do |p|
              m.add("p#{p}", relation.page(p).per(per))
            end
          end

          batch.each do |p|
            res = mr["p#{p}"]
            out.concat(Array(res&.to_a))
          end
        end

        out
      end

      def coerce_pluck_field_names(fields)
        Array(fields).flatten.compact.map(&:to_s).map(&:strip).reject(&:empty?)
      end
      module_function :coerce_pluck_field_names

      def validate_pluck_fields_allowed!(relation, names)
        state = relation.instance_variable_get(:@state) || {}
        include_base = Array(state[:select]).map(&:to_s)
        exclude_base = Array(state[:exclude]).map(&:to_s)

        missing = if include_base.empty?
                    names & exclude_base
                  else
                    allowed = include_base - exclude_base
                    names - allowed
                  end

        return if missing.empty?

        msg = build_invalid_selection_message_for_pluck(
          missing: missing,
          requested: names,
          include_base: include_base,
          exclude_base: exclude_base
        )
        field = missing.map(&:to_s).min
        hint = exclude_base.include?(field) ? "Remove exclude(:#{field})." : nil
        raise SearchEngine::Errors::InvalidSelection.new(
          msg,
          hint: hint,
          doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/field-selection#guardrails--errors',
          details: { requested: names, include_base: include_base, exclude_base: exclude_base }
        )
      end
      module_function :validate_pluck_fields_allowed!

      def build_invalid_selection_message_for_pluck(missing:, requested:, include_base:, exclude_base:)
        field = missing.map(&:to_s).min
        if exclude_base.include?(field)
          "InvalidSelection: field :#{field} not in effective selection. Remove exclude(:#{field})."
        else
          suggestion_fields = include_base.dup
          requested.each { |f| suggestion_fields << f unless suggestion_fields.include?(f) }
          symbols = suggestion_fields.map { |t| ":#{t}" }.join(',')
          "InvalidSelection: field :#{field} not in effective selection. Use `reselect(#{symbols})`."
        end
      end
      module_function :build_invalid_selection_message_for_pluck

      def build_facets_context_from_state(relation)
        state = relation.instance_variable_get(:@state) || {}
        fields = Array(state[:facet_fields]).map(&:to_s)
        queries = Array(state[:facet_queries]).map do |q|
          h = { field: q[:field].to_s, expr: q[:expr].to_s }
          h[:label] = q[:label].to_s if q[:label]
          h
        end
        return nil if fields.empty? && queries.empty?

        { fields: fields.freeze, queries: queries.freeze }.freeze
      end
      module_function :build_facets_context_from_state

      # Detect a simple AST eq(:id, value) predicate with no other filters.
      def detect_equality_id_predicate_value(relation)
        state = relation.instance_variable_get(:@state) || {}
        ast_nodes = Array(state[:ast]).flatten.compact
        return nil unless ast_nodes.size == 1

        node = ast_nodes.first
        return nil unless node.is_a?(SearchEngine::AST::Eq)
        return nil unless node.field.to_s == 'id'

        node.value
      rescue StandardError
        nil
      end

      # Use the Typesense retrieve-by-id endpoint and hydrate the document.
      def retrieve_document_by_id(relation, id_value)
        collection = relation.send(:collection_name_for_klass)
        client = relation.send(:client)
        raw = client.retrieve_document(collection: collection, id: id_value)
        return nil unless raw.is_a?(Hash)

        SearchEngine::Base::Creation::Helpers.hydrate_from_document(relation.klass, raw)
      rescue StandardError
        nil
      end
    end
  end
end
