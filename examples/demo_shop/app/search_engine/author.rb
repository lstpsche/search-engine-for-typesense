# frozen_string_literal: true

module SearchEngine
  # Searchable authors referenced by demo books.
  class Author < SearchEngine::Base
    collection 'authors'

    identify_by :id
    attribute :first_name, :string
    attribute :last_name, :string

    query_by %i[first_name last_name]

    index do
      source :active_record, model: ::Author
      map { |record| { first_name: record.first_name.to_s, last_name: record.last_name.to_s } }
    end
  end
end
