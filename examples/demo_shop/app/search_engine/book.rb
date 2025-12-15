# frozen_string_literal: true

module SearchEngine
  # Minimal SearchEngine model for demo books
  class Book < SearchEngine::Base
    collection 'books'

    attribute :id, :integer
    attribute :title, :string
    attribute :author_id, :integer
    attribute :author_name, :string
    attribute :updated_at, :datetime

    join :authors, collection: 'authors', local_key: :author_id, foreign_key: :id

    index do
      source :active_record, model: ::Book
      map do |r|
        {
          id: r.id,
          title: r.title.to_s,
          author_id: r.author_id,
          author_name: r.author&.full_name.to_s,
          updated_at: r.updated_at&.utc&.iso8601
        }
      end
    end
  end
end
