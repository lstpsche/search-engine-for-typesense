# frozen_string_literal: true

require 'set'

module SearchEngine
  module Hydration
    # Centralized executors for materialization: to_a/each/count/ids/pluck/pick.
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

          result = search_result(relation)
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

        result = search_result(relation)
        array = result.to_a
        relation.instance_variable_set(:@__preview_memo, array)
        array.first(limit)
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

      def pick(relation, *fields)
        raise ArgumentError, 'pick requires at least one field' if fields.nil? || fields.empty?

        loaded = relation.instance_variable_get(:@__loaded)
        return pluck(relation, *fields).first if loaded

        state = relation.instance_variable_get(:@state) || {}
        relation_for_pick =
          if state[:page] || state[:per_page]
            relation
          else
            relation.limit(1)
          end
        pluck(relation_for_pick, *fields).first
      end

      def pluck(relation, *fields)
        raise ArgumentError, 'pluck requires at least one field' if fields.nil? || fields.empty?

        names = coerce_pluck_field_names(fields)
        validate_pluck_fields_allowed!(relation, names)

        result = execute(relation)
        documents = Result.documents(result.raw)
        result.to_a.each_with_index.map do |object, index|
          doc = documents.fetch(index)
          if result.is_a?(Result)
            result.send(:enforce_strict_missing_if_needed!, doc.keys.map(&:to_s), requested_override: names)
          end
          values = names.map do |field|
            if object.respond_to?(field)
              object.public_send(field)
            else
              doc.fetch(field) { doc[field.to_sym] }
            end
          end
          names.one? ? values.first : values
        end
      end

      def exists?(relation)
        return count(relation).positive? if relation.send(:curation_filter_curated_hits?)

        loaded = relation.instance_variable_get(:@__loaded)
        memo = relation.instance_variable_get(:@__result_memo)
        return memo.found.to_i.positive? if loaded && memo

        fetch_found_only(relation).positive?
      end

      def count(relation)
        if relation.send(:curation_filter_curated_hits?)
          result = execute(relation)
          return curated_total_count(result)
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

      def result_options(relation)
        {
          klass: relation.klass,
          selection: SelectionContext.build(relation),
          facets: build_facets_context_from_state(relation)
        }
      end

      def search_result(relation, client: relation.send(:client))
        collection = relation.send(:collection_name_for_klass)
        params = SearchEngine::CompiledParams.from(relation.to_typesense_params)
        options = result_options(relation)
        built = false
        url_opts = relation.send(:build_url_opts)
        result = client.search(collection: collection, params: params, url_opts: url_opts) do |raw|
          built = true
          Result.new(raw, **options)
        end

        # Legacy clients return an already wrapped result without yielding the response.
        if !built && (options[:selection] || options[:facets])
          result = result.is_a?(Result) ? result.with_context(**options) : Result.new(result.raw, **options)
        end
        relation.send(:enforce_hit_validator_if_needed!, result.found, collection: collection)
        result
      end

      def fetch_found_only(relation)
        collection = relation.send(:collection_name_for_klass)
        base = SearchEngine::CompiledParams.from(relation.to_typesense_params).to_h

        minimal = base.except(:offset, :limit)
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

      def curated_total_count(result)
        raw = result.raw || {}
        base_count = raw['found_docs'] || raw[:found_docs] || result.found

        base_count.to_i + curated_hits_count(result)
      end
      module_function :curated_total_count

      def curated_hits_count(result)
        result.to_a.count do |obj|
          obj.respond_to?(:curated_hit?) && obj.curated_hit?
        end
      end
      module_function :curated_hits_count

      def effective_per_page(relation)
        state = relation.instance_variable_get(:@state) || {}
        per_page = state[:per_page] || (state[:limit] unless state[:page])
        return [per_page, state.dig(:hit_limits, :early_limit)].compact.min if per_page

        loaded = relation.instance_variable_get(:@__loaded)
        memo = relation.instance_variable_get(:@__result_memo)
        if loaded && memo
          request_params = memo.raw['request_params'] || memo.raw[:request_params]
          request_params ||= memo.raw['search_params'] || memo.raw[:search_params]
          if request_params
            per = request_params['per_page'] || request_params[:per_page]
            return per.to_i if per.to_i.positive?
          end
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

        per = [250, relation.hit_limits&.dig(:early_limit)].compact.min
        pages = (total.to_f / per).ceil

        client = relation.send(:client)
        return search_result(relation.page(1).per(per), client: client).to_a if pages == 1

        url_opts = relation.send(:build_url_opts)
        out = []
        (1..pages).each_slice([SearchEngine.config.multi_search_limit, 1].max) do |batch|
          builder = SearchEngine::Multi.new
          batch.each { |page| builder.add("p#{page}", relation.page(page).per(per)) }
          results = builder.execute(client: client, url_opts: url_opts)
          results.each_label { |_, result| out.concat(result.hits) }
        end
        out
      end

      def coerce_pluck_field_names(fields)
        Array(fields).flatten.filter_map do |f|
          s = f.to_s.strip
          s unless s.empty?
        end
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
          doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/field-selection#guardrails--errors',
          details: { requested: names, include_base: include_base, exclude_base: exclude_base }
        )
      end
      module_function :validate_pluck_fields_allowed!

      def build_invalid_selection_message_for_pluck(missing:, requested:, include_base:, exclude_base:)
        field = missing.map(&:to_s).min
        if exclude_base.include?(field)
          "InvalidSelection: field :#{field} not in effective selection. Remove exclude(:#{field})."
        else
          seen = Set.new(include_base)
          suggestion_fields = include_base.dup
          requested.each { |f| suggestion_fields << f if seen.add?(f) }
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
