# Search Engine for Typesense [![CI][ci-badge]][ci-url] [![Gem][gem-badge]][gem-url] [![Docs][docs-badge]][docs-url]
[![Typesense](https://img.shields.io/badge/Typesense-Typesense-blue)](https://typesense.org) [![Typesense Ruby gem](https://img.shields.io/badge/Typesense%20Ruby%20gem-TypesenseRubyGem-blue)](https://github.com/typesense/typesense-ruby)

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

See [Quickstart](https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/quickstart).

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

# Geo search
class SearchEngine::Venue < SearchEngine::Base
  collection :venues
  identify_by :id

  attribute :name, :string
  attribute :location, :geopoint
end

# Filter by radius
SearchEngine::Venue
  .where_geo(:location, within_radius: { lat: 54.69, lng: 25.28, radius: "10 km" })
  .order_geo(:location, from: { lat: 54.69, lng: 25.28 })
  .to_a

# Filter by polygon (viewport)
SearchEngine::Venue
  .where_geo(:location, within_polygon: [[54.72, 25.35], [54.72, 25.22], [54.67, 25.22], [54.67, 25.35]])
  .to_a

# Viewport boost with _eval() + distance tiebreaker
SearchEngine::Venue
  .order_eval("location:(54.72,25.35, 54.72,25.22, 54.67,25.22, 54.67,25.35)", direction: :desc)
  .order_geo(:location, from: { lat: 54.69, lng: 25.28 })
  .to_a

# Access geo distance on results (present when order_geo is used)
result = SearchEngine::Venue.all.order_geo(:location, from: { lat: 54.69, lng: 25.28 }).execute
result.hits.first.geo_distance_meters # => { "location" => 1234 }
```

## Documentation

See the [Docs](https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/index)

## Test/offline mode

In test environments (`Rails.env.test?` or `RACK_ENV=test`), SearchEngine defaults to an offline client
(`SearchEngine::Test::OfflineClient`) so no Typesense HTTP calls are made.

You can control this explicitly with:
- `SEARCH_ENGINE_TEST_MODE=1` to force offline mode
- `SEARCH_ENGINE_TEST_MODE=0` to disable offline mode
- `SEARCH_ENGINE_OFFLINE=1` (legacy alias)

If you set `SearchEngine.configure { |c| c.client = ... }`, the custom client is always used.

## Async partition indexing

Async partition indexing is an advanced opt-in mode for partitioned full indexing. The default remains
inline execution. Apps with a real ActiveJob backend can use queue-backed partition execution so each
search/index partition imports into the same blue/green physical collection before the alias is swapped.
This mode does not require Sidekiq; use any ActiveJob backend that can run the partition jobs.

```ruby
SearchEngine.configure do |c|
  c.indexer.partition_execution = :active_job
  c.indexer.partition_queue_name = "search_index_partitions"
  c.indexer.partition_timeout_s = 7_200
end
```

Async mode is partition-based, not app-domain based. The gem enqueues one
`SearchEngine::IndexPartitionJob` per configured partition, waits for every partition to finish, and
only then lets the schema lifecycle swap the alias. If any partition fails or times out, the previous
alias target remains active.

Use a shared `Rails.cache` backend, or provide `c.indexer.partition_run_store`, so worker processes and
the parent indexing process can see the same run metadata. Size the queue carefully: worker concurrency
multiplies with any per-partition `max_parallel` setting.

## PostgreSQL outbox sync

Rails callbacks are convenient for ordinary `create`, `update`, and `destroy` flows, but they do not see
every database write. Bulk SQL imports, database triggers, background functions, and direct maintenance
scripts can change source tables without instantiating Active Record models. PostgreSQL outbox sync captures
those writes at the database layer and lets the gem process them through ActiveJob.

The flow is:

1. A row-level PostgreSQL trigger writes a durable outbox row in the same transaction as the source table
   change.
2. The trigger calls `pg_notify` as a low-latency nudge after commit.
3. A host-managed listener receives notifications, or falls back to polling, and enqueues
   `SearchEngine::PostgresOutbox::DrainJob`.
4. The drainer claims pending rows, coalesces older rows for the same collection/document pair, orders
   collection groups with the dependency planner, and processes the resulting upserts/deletes.

`pg_notify` is not durable. Treat notifications only as a wakeup signal; the outbox table is the source of
truth. Run the listener in a process lifecycle you control, and keep fallback polling enabled so missed
notifications are drained later.

PostgreSQL outbox sync is disabled by default:

```ruby
# config/initializers/search_engine.rb
SearchEngine.configure do |c|
  c.postgres_outbox.enabled = true
  c.postgres_outbox.listener_enabled = -> { Rails.env.production? }
  c.postgres_outbox.table_name = "search_engine_outbox_events"
  c.postgres_outbox.delivery_table_name = "search_engine_outbox_deliveries"
  c.postgres_outbox.channel = "search_engine_outbox"
  c.postgres_outbox.queue_name = "search_engine"
  c.postgres_outbox.batch_size = 1000
  c.postgres_outbox.poll_interval_s = 5
  c.postgres_outbox.retention_s = 7.days.to_i

  # Optional. Leave off when your deployment already guarantees one listener.
  c.postgres_outbox.advisory_lock = false

  # Optional. Leave empty for the default single-target flow.
  c.postgres_outbox.delivery_targets = lambda do
    [
      { key: :mirror_a, queue_name: :search_engine_mirror_a },
      { key: :mirror_b, queue_name: :search_engine_mirror_b }
    ]
  end
end
```

Generate and edit the migrations:

```bash
bin/rails generate search_engine:postgres_outbox:install
```

The events table migration should include the gem helper:

```ruby
class CreateSearchEngineOutboxEvents < ActiveRecord::Migration[7.1]
  include SearchEngine::PostgresOutbox::MigrationHelpers

  def change
    create_search_engine_outbox_events
    # Required only when c.postgres_outbox.delivery_targets is configured.
    create_search_engine_outbox_deliveries
  end
end
```

Add one trigger per source table that should write outbox events:

```ruby
class AddSearchEngineOutboxTriggers < ActiveRecord::Migration[7.1]
  include SearchEngine::PostgresOutbox::MigrationHelpers

  def up
    create_search_engine_outbox_trigger(
      :products,
      source_model: "Product",
      collection: "products"
    )

    create_search_engine_outbox_trigger(
      :product_variants,
      source_model: "ProductVariant",
      collection: "product_variants",
      record_id_sql: "record_data.id::text",
      document_id_sql: "record_data.product_id::text || '-' || record_data.id::text"
    )
  end

  def down
    drop_search_engine_outbox_trigger(:product_variants)
    drop_search_engine_outbox_trigger(:products)
  end
end
```

`record_id_sql` and `document_id_sql` are trusted migration SQL expressions. They may refer to the
PL/pgSQL `record_data` variable, which is `NEW` for inserts/updates and `OLD` for deletes.

The event table stores logical changes. When `delivery_targets` is empty, drain jobs claim those event rows
directly and existing single-target setups do not need to create or use delivery rows. When you configure
delivery targets, `search_engine_outbox_deliveries` stores target-specific status, retry, lock, and queue
state for each logical event. The listener uses the drain enqueuer to materialize missing delivery rows and
enqueue one drain job per target queue. Processors still receive event objects and return event IDs; the
parent event status is refreshed from the aggregate delivery states.

Pair triggered source models with `sync_strategy: :postgres_outbox` so Active Record callbacks do not also
write to Typesense for the same changes:

```ruby
class Product < ApplicationRecord
  include SearchEngine::ActiveRecordSyncable

  search_engine_syncable collection: :products, sync_strategy: :postgres_outbox
end
```

The listener lifecycle belongs to the host app. This Sidekiq initializer is one example; any process manager
or ActiveJob backend can start and stop a listener as long as it can enqueue jobs:

```ruby
# config/initializers/search_engine_outbox_listener.rb
if defined?(Sidekiq)
  Sidekiq.configure_server do |config|
    listener = nil

    config.on(:startup) do
      outbox = SearchEngine.config.postgres_outbox
      next unless outbox.enabled && outbox.listener_enabled.call

      listener = SearchEngine::PostgresOutbox::Listener.new.start
    end

    config.on(:quiet) { listener&.stop(timeout: 5) }
    config.on(:shutdown) { listener&.stop(timeout: 5) }
  end
end
```

Custom processors can override the default collection handling. Register processors by collection name and
return `SearchEngine::PostgresOutbox::ProcessorResult`:

```ruby
SearchEngine.configure do |c|
  c.postgres_outbox.collection_processors["products"] = lambda do |events:, context:|
    document_ids = events.map(&:document_id)
    ProductSearchSync.call(document_ids: document_ids, worker_id: context[:worker_id])

    SearchEngine::PostgresOutbox::ProcessorResult.success(events.map(&:id))
  rescue StandardError => error
    SearchEngine::PostgresOutbox::ProcessorResult.failure(events.map(&:id), error: error)
  end
end
```

When one collection references another, declare those references on the SearchEngine models. The outbox
drainer uses the same dependency planner direction as bulk cascade planning, so parent/source collections
are processed before dependent collections in the same drain pass. If a collection group fails, later
dependent groups are left retryable instead of being processed against stale data.

Enable `c.postgres_outbox.advisory_lock = true` when multiple processes may start listeners and your host
deployment cannot guarantee exactly one listener. The listener uses `pg_try_advisory_lock` with
`c.postgres_outbox.advisory_lock_key`, or a stable key derived from the notification channel. If the lock is
not acquired, that listener sleeps and retries.

Processed and superseded rows are safe to delete after your retention window. Failed rows should be
inspected before deletion because they contain the last error and retry state. A typical cleanup job deletes
only rows with `status IN ('processed', 'superseded')` and `processed_at` older than
`c.postgres_outbox.retention_s`.

## Example app

See `examples/demo_shop` — demonstrates single/multi search, JOINs, grouping, presets/curation, and DX/observability. Supports offline mode via the stub client (see [Testing](https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/testing)).

## Contributing

See [Docs Style Guide](https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/docs-style-guide). Follow YARDoc for public APIs, add backlinks on docs landing pages, and redact secrets in examples.

<!-- Badge references (placeholders) -->
[ci-badge]: https://img.shields.io/github/actions/workflow/status/lstpsche/search-engine-for-typesense/ci.yml?branch=main
[ci-url]: #
[gem-badge]: https://badge.fury.io/rb/search-engine-for-typesense.svg?icon=si%3Arubygems
[gem-url]: https://rubygems.org/gems/search-engine-for-typesense
[docs-badge]: https://img.shields.io/badge/docs-index-blue
[docs-url]: https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/index
