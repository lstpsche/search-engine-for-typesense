# frozen_string_literal: true

module SearchEngine
  # Minimal SearchEngine model for demo books
  class Book < SearchEngine::Base
    collection 'books'

    identify_by :id
    attribute :title, :string, sort: true
    attribute :author_id, :string
    attribute :author_name, :string
    attribute :updated_at, :datetime

    belongs_to :authors, collection: 'authors', local_key: :author_id, foreign_key: :id

    query_by %i[title author_name]

    index do
      source :active_record, model: ::Book
      map do |r|
        {
          id: r.id,
          title: r.title.to_s,
          author_id: r.author_id.to_s,
          author_name: r.author&.full_name.to_s,
          updated_at: r.updated_at.to_i
        }
      end
    end
  end
end
