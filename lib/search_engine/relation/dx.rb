# frozen_string_literal: true

require 'json'
require 'search_engine/relation/dx/friendly_where'
require 'search_engine/relation/dx/dry_run'

module SearchEngine
  # Immutable, chainable query relation bound to a model class.
  # See `lib/search_engine/relation.rb` for full API; DX helpers are mixed in here.
  class Relation
    # DX helpers are mixed into `Relation` to offer redaction‑aware, zero‑I/O explain and preview utilities.
    # These helpers are pure and never mutate relation state.
    module Dx
      # Return the request body JSON after compile, fully redacted.
      # @param pretty [Boolean] pretty-print with stable key ordering when true
      # @return [String]
      # @since M8
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/dx
      def to_params_json(pretty: true)
        params = SearchEngine::CompiledParams.from(to_typesense_params)
        redacted = SearchEngine::Relation::Dx::DryRun.redact_params(params.to_h)
        SearchEngine::Relation::Dx::DryRun.to_json(redacted, pretty: pretty)
      end

      # Return a single-line curl command with redacted API key and JSON body.
      # @return [String]
      # @since M8
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/dx
      def to_curl
        url = compiled_url
        params = SearchEngine::CompiledParams.from(to_typesense_params).to_h
        SearchEngine::Relation::Dx::DryRun.curl(url, params)
      end

      # Compile and validate without performing network I/O.
      # Returns a structured hash with URL and post-redaction body.
      # @return [Hash] { url:, body:, url_opts: }
      # @raise [SearchEngine::Errors::*] same validation errors as runtime path
      # @since M8
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/dx
      def dry_run!
        params = SearchEngine::CompiledParams.from(to_typesense_params).to_h
        SearchEngine::Relation::Dx::DryRun.payload(url: compiled_url, params: params, url_opts: compiled_url_opts)
      end

      # Enhanced explain output with overview, parts, conflicts, and predicted events.
      # Builds a redaction-aware summary without network I/O.
      # @param to [Symbol, nil]
      # @return [String]
      # @since M8
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/dx#helpers--examples
      def explain(to: nil)
        params = SearchEngine::CompiledParams.from(to_typesense_params)
        lines = []

        lines << header_line
        append_preset_explain_line(lines, params)
        append_curation_explain_lines(lines)
        append_where_line(lines, params)
        append_order_line(lines, params)
        append_group_line(lines)
        append_facets_line(lines, params)
        append_selection_explain_lines(lines, params)
        add_effective_selection_tokens!(lines)
        add_pagination_line!(lines, params)
        append_hit_limits_line(lines, params)
        append_ranking_line(lines, params)
        lines << overview_line(params)
        append_conflicts_line(lines, params)
        append_events_line(lines, params)

        out = lines.join("\n")
        puts(out) if to == :stdout
        out
      end

      private

      def redact_body(params)
        hash = params.dup
        preview = SearchEngine::Observability.redact(params)
        hash[:filter_by] = preview[:filter_by] if preview.is_a?(Hash) && preview.key?(:filter_by)
        hash
      end

      def predicted_events_for_plan(params)
        events = []
        events << 'search_engine.compile' if Array(@state[:ast]).any?
        events << 'search_engine.joins.compile' if Array(params[:_join] && params[:_join][:assocs]).any?
        events << 'search_engine.grouping.compile' if params.key?(:group_by)
        if preset_name
          events << 'search_engine.preset.apply'
          events << 'search_engine.preset.conflict' if Array(params[:_preset_conflicts]).any?
        end
        events << 'search_engine.search'
        events
      end

      def compiled_url
        collection = collection_name_for_klass
        cfg = SearchEngine.config
        proto = cfg.protocol.to_s.strip.presence
        base = [proto, "#{cfg.host}:#{cfg.port}"].compact.join('://')
        "#{base}/collections/#{collection}/documents/search"
      end

      def compiled_url_opts
        url_opts = ClientOptions.url_options_from_config(SearchEngine.config)
        overrides = build_url_opts
        url_opts.merge!(overrides) unless overrides.empty?
        url_opts
      end

      def header_line
        "#{klass_name_for_inspect} Relation"
      end

      def append_where_line(lines, params)
        fb = params[:filter_by]
        return unless fb && !fb.to_s.strip.empty?

        # Render friendly operators first, then redact the final string to preserve
        # IN/NOT IN tokens while masking literals.
        friendly = SearchEngine::Relation::Dx::FriendlyWhere.render(fb.to_s)
        masked = SearchEngine::Observability.redact(friendly)
        where_str = masked.is_a?(String) ? masked : friendly
        lines << "  where: #{where_str}" unless where_str.to_s.strip.empty?
      end

      def append_order_line(lines, params)
        sb = params[:sort_by]
        lines << "  order: #{sb}" if sb && !sb.to_s.strip.empty?
      end

      def append_group_line(lines)
        g = @state[:grouping]
        return unless g

        gparts = ["group_by=#{g[:field]}"]
        gparts << "limit=#{g[:limit]}" if g[:limit]
        gparts << 'missing_values=true' if g[:missing_values]
        lines << "  group: #{gparts.join(' ')}"
      end

      def overview_line(params)
        parts = []
        parts << "collection=#{collection_name_for_klass}"
        if (g = grouping)
          seg = [g[:field]].compact
          seg << "limit=#{g[:limit]}" if g[:limit]
          seg << 'missing_values' if g[:missing_values]
          parts << "grouping=#{seg.join(':')}"
        end
        if params.key?(:page) || params.key?(:per_page)
          p = params[:page]
          per = params[:per_page]
          parts << "page/per=#{p || ''}/#{per || ''}"
        end
        parts << (preset_name ? "preset=#{preset_name}(mode=#{preset_mode})" : 'preset=—')
        parts << "joins=#{Array(joins_list).size}"
        cid = SearchEngine::Instrumentation.current_correlation_id
        parts << "cid=#{cid && !cid.to_s.empty? ? cid : '—'}"
        "Overview: #{parts.join(' ')}"
      end

      def append_conflicts_line(lines, params)
        conflicts = Array(params[:_preset_conflicts]).map(&:to_s).sort
        lines << "Conflicts & Warnings: dropped keys in :lock mode => #{conflicts.join(', ')}" unless conflicts.empty?
      end

      def append_events_line(lines, params)
        events = predicted_events_for_plan(params)
        lines << "Events that would fire: #{events.join(' → ')}" unless events.empty?
      end

      def append_facets_line(lines, params)
        fb = params[:facet_by]
        fq = params[:facet_query]
        mv = params[:max_facet_values]
        segs = []
        segs << "facet_by=#{fb}" if fb && !fb.to_s.strip.empty?
        segs << "max=#{mv}" if mv
        segs << "queries=#{fq}" if fq && !fq.to_s.strip.empty?
        lines << "  facets: #{segs.join(' ')}" unless segs.empty?
      end

      def append_ranking_line(lines, params)
        rk = @state[:ranking]
        has_ranking_param = rk ||
                            params.key?(:query_by_weights) || params.key?(:num_typos) ||
                            params.key?(:drop_tokens_threshold) || params.key?(:prioritize_exact_match) ||
                            params.key?(:infix)
        return unless has_ranking_param

        parts = []
        begin
          plan = SearchEngine::RankingPlan.new(relation: self, query_by: params[:query_by], ranking: rk || {})
          fields = plan.effective_query_by_fields
          parts << "query_by=[#{fields.join(', ')}]" unless fields.empty?
        rescue StandardError
          # ignore
        end

        parts << "weights=#{params[:query_by_weights]}" if params[:query_by_weights]
        parts << "num_typos=#{params[:num_typos]}" if params.key?(:num_typos)
        parts << "drop_tokens_threshold=#{params[:drop_tokens_threshold]}" if params.key?(:drop_tokens_threshold)
        parts << "prioritize_exact_match=#{params[:prioritize_exact_match]}" if params.key?(:prioritize_exact_match)
        parts << "prefix=#{params[:infix]}" if params[:infix]

        lines << "  ranking: #{parts.join(' ')}" unless parts.empty?
      end

      def append_hit_limits_line(lines, params)
        hits = params[:_hits]
        return unless hits.is_a?(Hash) && (hits[:early_limit] || hits[:max])

        segs = []
        segs << "early_limit=#{hits[:early_limit]}" if hits[:early_limit]
        if hits[:per_adjusted] == true
          segs << 'per_adjusted=true'
        elsif hits.key?(:per_adjusted)
          segs << 'per_adjusted=false'
        end
        segs << "validator_max=#{hits[:max]}" if hits[:max]
        lines << "  hits: #{segs.join(' ')}" unless segs.empty?
      end
    end

    include Dx
  end
end
