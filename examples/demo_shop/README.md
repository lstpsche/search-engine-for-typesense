# Demo Shop — Typesense Search Engine Example

## Quickstart

```bash
bin/setup
bin/rails db:setup
bin/rails runner "Docs::SeedDemo.run"
bin/rails 'search_engine:schema:apply[products]'
```

Set Typesense env vars or run offline. See `.env.example` for placeholders.

## Running

- Start the server: `bin/rails s`
- Visit:
  - `/products` — single search with pagination, sorting, faceting, curation, presets, hit limits
  - `/brands` — simple list for multi‑search pairing
  - `/books` — JOINs with authors + nested `include_fields` + title search
  - `/groups` — `group_by :brand_id`
  - `/search/multi` — federated multi‑search (products + brands)

## Indexing

- Apply schema: `bin/rails 'search_engine:schema:apply[products]'`
- Rebuild: `bin/rails 'search_engine:index:rebuild[products]'`
- Delete stale: `bin/rails 'search_engine:index:delete_stale[products]'`
- Convenience: `bin/rails demo:index:all`

## Offline mode

- Set `SEARCH_ENGINE_OFFLINE=1` to use the stub client with fake results; no network I/O.
- Live mode requires `TYPESENSE_HOST`, `TYPESENSE_PORT`, `TYPESENSE_PROTOCOL`, and `TYPESENSE_API_KEY`.

## Debug & Observability

- Each page shows `rel.explain` and `to_params_json` previews.
- Development enables the logging subscriber for compact unified events.
- If OpenTelemetry is present and enabled in config, spans will be emitted.

## Multi‑search walkthrough

```ruby
# app/controllers/search_controller.rb
@multi = SearchEngine.multi_search_result do |m|
  m.add :products, SearchEngine::Product.all.per(5)
  m.add :brands,   SearchEngine::Brand.all.per(3)
end
```

See docs: [Multi-search Guide](https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/multi-search-guide).

## Presets & Curation

Try adding a curated list in code:

```ruby
SearchEngine::Product.preset(:popular_products, mode: :only).pin("12").hide("99").explain
```

See docs:
- [Presets](https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/presets)
- [Curation](https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/curation)

## Products Advanced Options (Query Params)

The `/products` page accepts additional query params to showcase core DSL:

- `category` / `brand` — filter with `where`
- `sort` — `price_asc`, `price_desc`, `updated_desc`, `name_asc`
- `facet=1` — facet on `category` and `brand_name`
- `pin` / `hide` — curation IDs (`pin=1,2&hide=3`)
- `filter_curated_hits=1|0` — filter curated hits
- `preset` / `preset_mode` — presets (`merge`, `only`, `lock`)
- `early_limit` / `max_hits` — hit limits and validator
- `synonyms=1|0`, `stopwords=1|0`, `cache=1|0`
- `rank=strict|fuzzy` — ranking presets

Note: presets are server-side in Typesense. With the demo namespace enabled,
`preset=popular` resolves to `demo_popular`.

## Environment

- `.env.example` contains all required variables.
- `SEARCH_ENGINE_OFFLINE=1` uses `SearchEngine::Test::StubClient`.

## Routes

- products: `GET /products`
- brands: `GET /brands`
- books: `GET /books`
- groups: `GET /groups`
- multi: `GET /search/multi`

## Smoke script

- Run `examples/demo_shop/bin/smoke` to verify offline flows compile and dry‑run successfully.

## Troubleshooting

- Missing API key or connection errors: run doctor `bin/rails search_engine:doctor`.
- Unknown fields: adjust model attributes or selection; see [Field Selection](https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/field-selection).
- Observability: see [Observability + DX + Testing](https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/observability-dx-testing).

## Cross‑links

- Seeds: `examples/demo_shop/lib/docs/seed_demo.rb`
- Multi‑search controller: `examples/demo_shop/app/controllers/search_controller.rb`
- JOINs demo: `examples/demo_shop/app/controllers/books_controller.rb`
- Grouping demo: `examples/demo_shop/app/controllers/groups_controller.rb`
