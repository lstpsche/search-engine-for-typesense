# frozen_string_literal: true

module SearchEngine
  # Minimal SearchEngine model for demo brands
  class Brand < SearchEngine::Base
    collection 'brands'

    attribute :id, :integer
    attribute :name, :string
    attribute :updated_at, :datetime

    index do
      source :active_record, model: ::Brand
      map do |r|
        {
          id: r.id,
          name: r.name.to_s,
          updated_at: r.updated_at&.utc&.iso8601
        }
      end
    end
  end
end
