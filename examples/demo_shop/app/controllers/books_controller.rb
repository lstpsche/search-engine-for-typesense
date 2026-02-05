# frozen_string_literal: true

# BooksController shows JOINs with authors and nested selection.
class BooksController < ApplicationController
  def index
    q = params[:q].to_s
    author = params[:author].to_s
    page = (params[:page].presence || 1).to_i
    per = (params[:per].presence || 10).to_i

    rel = SearchEngine::Book.all
    rel = rel.joins(:authors)
    rel = rel.include_fields(:id, :title, authors: %i[first_name last_name])

    rel = rel.search(q) unless q.blank?

    # Example JOIN filter (e.g., last_name = "Rowling")
    rel = rel.where(authors: { last_name: author }) unless author.blank?
    rel = rel.page(page).per(per)

    @relation = rel
    @result = rel.execute
    @hits = @result.hits
  end
end
