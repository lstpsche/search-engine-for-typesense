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

          raw_result = relation.send(:client).search(collection: collection, params: params, url_opts: url_opts)

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

        preview_relation = relation.send(:spawn) do |state|
          state[:page] = 1
          state[:per_page] = limit
        end

        collection = preview_relation.send(:collection_name_for_klass)
        params = SearchEngine::CompiledParams.from(preview_relation.to_typesense_params)
        url_opts = preview_relation.send(:build_url_opts)
        raw_result = preview_relation.send(:client).search(collection: collection, params: params, url_opts: url_opts)

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

      def each(relation, &block)
        arr = to_a(relation)
        block_given? ? arr.each(&block) : arr.each
      end

      def first(relation, n = nil)
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
        result = relation.send(:client).search(collection: collection, params: minimal, url_opts: url_opts)
        count = result.found.to_i
        relation.send(:enforce_hit_validator_if_needed!, count, collection: collection)
        count
      end
      module_function :fetch_found_only

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
          doc: 'docs/field_selection.md#guardrails',
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
    end
  end
end
