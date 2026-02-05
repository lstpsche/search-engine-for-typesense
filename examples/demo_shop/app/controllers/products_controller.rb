# frozen_string_literal: true

# ProductsController demonstrates basic single-search and pagination.
class ProductsController < ApplicationController
  SORTS = {
    'relevance' => nil,
    'price_asc' => { price_cents: :asc },
    'price_desc' => { price_cents: :desc },
    'updated_desc' => { updated_at: :desc },
    'name_asc' => { name: :asc }
  }.freeze

  def index
    q = params[:q].to_s
    page = int_param(:page, 1)
    per = int_param(:per, 10)

    rel = SearchEngine::Product.all
    rel = rel.search(q) unless q.blank?

    category = params[:category].presence
    brand = params[:brand].presence
    rel = rel.where(category: category) if category
    rel = rel.where(brand_name: brand) if brand

    sort = params[:sort].presence
    rel = apply_sort(rel, sort)

    rel = rel.include_fields(:id, :name, :brand_name, :category, :price_cents)

    if facets_enabled?
      rel = rel.facet_by(:category, max_values: 10)
      rel = rel.facet_by(:brand_name, max_values: 10)
    end

    rel = apply_synonyms(rel)
    rel = apply_stopwords(rel)
    rel = apply_ranking(rel)
    rel = apply_cache(rel)
    rel = apply_preset(rel)
    rel = apply_curation(rel)
    rel = apply_hit_limits(rel)

    rel = rel.page(page).per(per)

    @relation = rel
    @result = rel.execute
    @hits = @result.hits
  end

  private

  def int_param(key, default)
    raw = params[key]
    return default if raw.nil? || raw.to_s.strip.empty?

    Integer(raw)
  rescue ArgumentError, TypeError
    default
  end

  def parse_bool(value)
    case value.to_s.strip.downcase
    when '1', 'true', 'yes', 'on' then true
    when '0', 'false', 'no', 'off' then false
    else nil
    end
  end

  def parse_list(value)
    value.to_s.split(',').map(&:strip).reject(&:empty?)
  end

  def facets_enabled?
    params.fetch(:facet, '1') != '0'
  end

  def apply_sort(rel, sort)
    rules = SORTS[sort.to_s]
    return rel if rules.nil?

    rel.order(rules)
  end

  def apply_synonyms(rel)
    val = parse_bool(params[:synonyms])
    return rel if val.nil?

    rel.use_synonyms(val)
  end

  def apply_stopwords(rel)
    val = parse_bool(params[:stopwords])
    return rel if val.nil?

    rel.use_stopwords(val)
  end

  def apply_cache(rel)
    val = parse_bool(params[:cache])
    return rel if val.nil?

    rel.cache(val)
  end

  def apply_ranking(rel)
    case params[:rank].to_s
    when 'strict'
      rel.ranking(num_typos: 0, drop_tokens_threshold: 0.0, prioritize_exact_match: true)
    when 'fuzzy'
      rel.ranking(num_typos: 2, drop_tokens_threshold: 0.2, prioritize_exact_match: false)
    else
      rel
    end
  end

  def apply_preset(rel)
    name = params[:preset].to_s.strip
    return rel if name.empty?

    mode =
      case params[:preset_mode].to_s
      when 'only' then :only
      when 'lock' then :lock
      else :merge
      end

    rel.preset(name, mode: mode)
  end

  def apply_curation(rel)
    pins = parse_list(params[:pin])
    hides = parse_list(params[:hide])

    rel = rel.pin(*pins) if pins.any?
    rel = rel.hide(*hides) if hides.any?

    fch = parse_bool(params[:filter_curated_hits])
    return rel if fch.nil?

    rel.curate(filter_curated_hits: fch)
  end

  def apply_hit_limits(rel)
    early = int_param(:early_limit, nil)
    max = int_param(:max_hits, nil)

    rel = rel.limit_hits(early) if early && early.positive?
    rel = rel.validate_hits!(max: max) if max && max.positive?
    rel
  end
end
