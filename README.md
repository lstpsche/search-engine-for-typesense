# Search Engine for Typesense [![CI][ci-badge]][ci-url] [![Gem][gem-badge]][gem-url] [![Docs][docs-badge]][docs-url]
[![Typesense](https://img.shields.io/badge/Typesense-Typesense-blue)](https://typesense.org) [![Typesense Ruby gem](https://img.shields.io/badge/Typesense%20Ruby%20gem-TypesenseRubyGem-blue)](https://github.com/typesense/typesense-ruby)

> [!WARNING]
> **⚠️ This project is under maintenance – work in progress. APIs and docs may change. ⚠️**

Mountless Rails::Engine for [Typesense](https://typesense.org). Expressive Relation/DSL with JOINs, grouping, presets/curation — with strong DX and observability.

> [!NOTE]
> This project is not affiliated with [Typesense](https://typesense.org) and is a wrapper for the [`typesense` gem](https://github.com/typesense/typesense-ruby).

## Versioning

The gem version mirrors the Typesense server major/minor it targets. Patch releases are reserved for gem-only fixes and enhancements.

Example: `30.1.x` targets Typesense `30.1`.

## Quickstart

```ruby
# Gemfile
gem "search-engine-for-typesense"
```

```ruby
# config/initializers/search_engine_for_typesense.rb
SearchEngine.configure do |c|
  c.host = ENV.fetch("TYPESENSE_HOST", "localhost")
  c.port = 8108
  c.protocol = "http"
  c.api_key = ENV.fetch("TYPESENSE_API_KEY")
end
```

```ruby
class SearchEngine::Product < SearchEngine::Base
  collection :products

  attribute :id, :integer
  attribute :name, :string

  query_by %i[name brand description]
end

SearchEngine::Product.where(name: "milk").select(:id, :name).limit(5).to_a
```

See [Quickstart](https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/quickstart).

### Host app SearchEngine models

By default, the gem manages a dedicated Zeitwerk loader for your SearchEngine models under `app/search_engine/`. The loader is initialized after Rails so that application models/constants are available, auto-reloads in development, and is eager-loaded in production/test.

Customize or disable via configuration:

```ruby
# config/initializers/search_engine.rb
SearchEngine.configure do |c|
  # Relative to Rails.root or absolute; set to nil/false to disable
  c.search_engine_models = 'app/search_engine'
end
```

## Usage examples

```ruby
# Model
class SearchEngine::Product < SearchEngine::Base
  collection "products"

  attribute :id, :integer
  attribute :name, :string
end

# Basic query
SearchEngine::Product
  .where(name: "milk")
  # Explicit query_by always wins over model/global defaults
  .options(query_by: 'name,brand')
  .select(:id, :name)
  .order(price_cents: :asc)
  .limit(5)
  .to_a

# JOIN + nested selection
SearchEngine::Product
  .joins(:brands)
  .select(:id, :name, brands: %i[id name])
  .where(brands: { name: "Acme" })
  .per(10)
  .to_a

# Faceting + grouping
rel = SearchEngine::Product
        .facet_by(:brand_id, max_values: 5)
        .facet_by(:category)
        .group_by(:brand_id, limit: 3)
params = rel.to_h # compiled Typesense params

# Multi-search
result_set = SearchEngine.multi_search(common: { query_by: SearchEngine.config.default_query_by }) do |m|
  m.add :products, SearchEngine::Product.where("name:~rud").per(10)
  m.add :brands,   SearchEngine::Brand.all.per(5)
end
result_set[:products].found

# Upserting documents
product_record = Product.first
mapped = SearchEngine::Product.mapped_data_for(product_record)

# Map + upsert a single record
SearchEngine::Product.upsert(record: product_record)

# Upsert already-mapped data
SearchEngine::Product.upsert(data: mapped)

# Bulk upsert records (mapper runs internally)
SearchEngine::Product.upsert_bulk(records: Product.limit(2))

# Bulk upsert mapped payloads
SearchEngine::Product.upsert_bulk(data: [mapped])
```

## Documentation

See the [Docs](https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/index)

## Test/offline mode

In test environments (`Rails.env.test?` or `RACK_ENV=test`), SearchEngine defaults to an offline client
(`SearchEngine::Test::OfflineClient`) so no Typesense HTTP calls are made.

You can control this explicitly with:
- `SEARCH_ENGINE_TEST_MODE=1` to force offline mode
- `SEARCH_ENGINE_TEST_MODE=0` to disable offline mode
- `SEARCH_ENGINE_OFFLINE=1` (legacy alias)

If you set `SearchEngine.configure { |c| c.client = ... }`, the custom client is always used.

## Example app

See `examples/demo_shop` — demonstrates single/multi search, JOINs, grouping, presets/curation, and DX/observability. Supports offline mode via the stub client (see [Testing](https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/testing)).

## Contributing

See [Docs Style Guide](https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/docs-style-guide). Follow YARDoc for public APIs, add backlinks on docs landing pages, and redact secrets in examples.

<!-- Badge references (placeholders) -->
[ci-badge]: https://img.shields.io/github/actions/workflow/status/lstpsche/search-engine-for-typesense/ci.yml?branch=main
[ci-url]: #
[gem-badge]: https://badge.fury.io/rb/search-engine-for-typesense.svg?icon=si%3Arubygems
[gem-url]: https://rubygems.org/gems/search-engine-for-typesense
[docs-badge]: https://img.shields.io/badge/docs-index-blue
[docs-url]: https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/index
