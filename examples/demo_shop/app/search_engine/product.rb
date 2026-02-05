# frozen_string_literal: true

module SearchEngine
  # Minimal SearchEngine model for demo products
  # Provides attributes to support selection, grouping, and basic filters/orders.
  class Product < SearchEngine::Base
    collection 'products'

    attribute :id, :integer
    attribute :name, :string, sort: true
    attribute :description, :string
    attribute :price_cents, :integer, sort: true
    attribute :brand_id, :integer
    attribute :brand_name, :string, facet: true
    attribute :category, :string, facet: true
    attribute :updated_at, :datetime, sort: true

    join :brands, collection: 'brands', local_key: :brand_id, foreign_key: :id

    query_by %i[name description brand_name category]

    # Minimal index mapping; kept simple for demo
    index do
      source :active_record, model: ::Product
      map do |r|
        {
          id: r.id,
          name: r.name.to_s,
          description: r.description.to_s,
          price_cents: r.price_cents.to_i,
          brand_id: r.brand_id,
          brand_name: r.brand&.name.to_s,
          category: r.category.to_s,
          updated_at: r.updated_at&.utc&.iso8601
        }
      end
    end
  end
end
