# frozen_string_literal: true

module SearchEngine
  # Pure, deterministic normalizer for ranking/typo/prefix tuning.
  # Accepts relation context, effective query_by, and raw ranking state,
  # validates and emits authoritative Typesense params.
  #
  # Usage: RankingPlan.new(relation: rel, query_by: "name,description", ranking: {...}).params
  class RankingPlan
    # @return [Hash]
    attr_reader :params

    # @param relation [SearchEngine::Relation]
    # @param query_by [String, nil]
    # @param ranking [Hash]
    def initialize(relation:, query_by:, ranking: {})
      @relation = relation
      @raw_query_by = query_by
      @raw = ranking || {}
      @params = compile!
      freeze
    end

    # Return effective query_by fields as an Array<String> (trimmed, non-blank)
    def effective_query_by_fields
      resolve_query_by(@raw_query_by)
    end

    private

    def compile!
      out = {}

      out[:num_typos] = @raw[:num_typos] if @raw.key?(:num_typos) && !@raw[:num_typos].nil?

      if @raw.key?(:drop_tokens_threshold) && !@raw[:drop_tokens_threshold].nil?
        out[:drop_tokens_threshold] = @raw[:drop_tokens_threshold]
      end

      if @raw.key?(:prioritize_exact_match) && !@raw[:prioritize_exact_match].nil?
        out[:prioritize_exact_match] = @raw[:prioritize_exact_match]
      end

      if (weights = @raw[:query_by_weights])
        fields = effective_query_by_fields
        if fields.empty?
          raise SearchEngine::Errors::InvalidOption.new(
            'InvalidOption: query_by is empty; cannot apply query_by_weights',
            hint: 'Set SearchEngine.config.default_query_by or pass options(query_by: ...)',
            doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/ranking#weights'
          )
        end

        normalized_weights = build_weight_vector!(fields, weights)
        if normalized_weights.all? { |w| w.to_i.zero? }
          raise SearchEngine::Errors::InvalidOption.new(
            'InvalidOption: at least one weighted field must have weight > 0',
            doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/ranking#weights'
          )
        end
        out[:query_by_weights] = normalized_weights.join(',')
      end

      out
    end

    def resolve_query_by(query_by)
      query_by.to_s.split(',').map(&:strip).reject(&:empty?)
    end

    def build_weight_vector!(fields, weight_map)
      # Validate that provided keys are subset of effective query_by
      known = fields
      provided = weight_map.keys.map(&:to_s)
      unknown = provided - known
      unless unknown.empty?
        suggestions = suggest_for(unknown.first, known)
        suffix = if suggestions.empty?
                   ''
                 elsif suggestions.length == 1
                   " (did you mean #{suggestions.first.inspect}?)"
                 else
                   others = suggestions[0..-2].map(&:inspect).join(', ')
                   last = suggestions.last.inspect
                   " (did you mean #{others}, or #{last}?)"
                 end
        raise SearchEngine::Errors::InvalidOption.new(
          "InvalidOption: weight specified for unknown field #{unknown.first.inspect}#{suffix}",
          doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/relation-reference#selection',
          details: { unknown: unknown.first, allowed: known }
        )
      end

      fields.map { |f| Integer(weight_map.fetch(f, 1)) }
    rescue ArgumentError, TypeError
      raise SearchEngine::Errors::InvalidOption.new(
        'InvalidOption: query_by_weights must compile to integers',
        doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/ranking#weights'
      )
    end

    def suggest_for(input, candidates)
      return [] if candidates.empty?

      begin
        require 'did_you_mean'
        require 'did_you_mean/levenshtein'
      rescue StandardError
        return []
      end

      distances = candidates.each_with_object({}) do |cand, acc|
        acc[cand] = DidYouMean::Levenshtein.distance(input.to_s, cand.to_s)
      end
      distances.sort_by { |(_c, d)| d }.take(3).select { |(_c, d)| d <= 2 }.map(&:first)
    end
  end
end
