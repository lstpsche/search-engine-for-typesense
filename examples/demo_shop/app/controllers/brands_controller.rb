# frozen_string_literal: true

# BrandsController lists brands for use in multi-search pairing.
class BrandsController < ApplicationController
  def index
    q = params[:q].to_s
    page = (params[:page].presence || 1).to_i
    per = (params[:per].presence || 10).to_i

    rel = SearchEngine::Brand.all
    rel = rel.search(q) unless q.blank?
    rel = rel.include_fields(:id, :name)
    rel = rel.page(page).per(per)

    @relation = rel
    @result = rel.execute
    @hits = @result.hits
  end
end
