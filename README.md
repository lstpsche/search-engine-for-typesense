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

# Inspect row-level failures without repeating the import request
result = SearchEngine::Product.upsert_bulk(data: [mapped], on_failure: :return)
failed_rows = result[:row_results].reject { |row| row[:success] }
failed_rows.first # => { index: 0, success: false, status: 404, error: "..." }

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

`upsert_bulk` defaults to `on_failure: :raise`. Use the exact symbol `:return` when the caller needs to
handle valid Typesense row failures itself. Malformed responses and response/document count mismatches always
raise in either mode; the gem never guesses which submitted document a missing response row belongs to.

Since `30.1.8.21`, bulk results intentionally do not expose the raw Typesense `response` because import
responses may contain submitted documents. Use the ordered, frozen `row_results` entries (`index`, `success`,
`status`, `error`) and aggregate counters instead. This is a safety-related result-shape change for callers
that previously read `result[:response]`.

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

A custom partition run store must implement:

```ruby
create_run(run_id:, collection:, collection_class_name:, into:, partitions:, ttl_s:)
mark_started(run_id:, partition_key:, job_id: nil)
record_attempt(run_id:, partition_key:, summary:, error:)
mark_succeeded(run_id:, partition_key:, summary:)
mark_failed(run_id:, partition_key:, error:)
snapshot(run_id:)
expire(run_id:)
```

`record_attempt` is non-terminal: it must persist that partition's partial summary and representative error
while leaving the partition `running`, without changing sibling partitions. Row-level import failures raise
from `IndexPartitionJob`, so ActiveJob retries the partition. Replaying successful upserts is expected and
safe. Only retry exhaustion terminalizes the partition as failed.

Only `:inline` and `:active_job` (or their exact String equivalents) are supported for
`c.indexer.dispatch` and `c.indexer.partition_execution`. Unknown values such as `:sidekiq` fail configuration
validation instead of silently running inline. Explicit `:active_job` dispatch also fails if ActiveJob is not
available.

## Guarded blue/green cutovers

`SearchEngine::Schema.apply!` can run an optional guard around the complete full-rebuild lifecycle: physical
collection creation, indexing, alias swap, and retention. `SearchEngine::Schema.rollback` uses the same guard
around target resolution, validation, and its alias swap. In-place schema updates intentionally do not invoke
the guard. The guard must yield exactly once; not yielding or yielding repeatedly raises `ArgumentError`.
Guard errors and indexing errors propagate, and an interrupted pre-swap physical collection is still cleaned
up.

In delivery-target outbox deployments, use the guard to stop new target claims, wait for already-claimed
deliveries to acknowledge, and prevent cooperating direct writers from crossing the alias swap:

```ruby
# config/initializers/search_engine.rb
typesense_target_key = ENV.fetch("TYPESENSE_DELIVERY_TARGET_KEY")

SearchEngine.configure do |c|
  c.schema.around_rebuild = lambda do |collection:, &rebuild|
    Rails.logger.info("Guarding Typesense rebuild for #{collection} on #{typesense_target_key}")
    SearchEngine::PostgresOutbox::Repository.new.with_delivery_target_claims_paused(
      target_key: typesense_target_key,
      timeout_s: SearchEngine.config.postgres_outbox.processing_timeout_s + 60,
      poll_interval_s: 0.1,
      &rebuild
    )
  end
end
```

The target key is the exact delivery destination key, not the logical collection name. Claim transactions
automatically take the matching transaction-scoped shared PostgreSQL advisory lock. The rebuild guard takes
an exclusive session lock, then waits until that target has no `processing` deliveries. It deliberately does
not reset timed-out leases: lease age cannot prove that an old worker's external Typesense request has
stopped. A stuck processing delivery therefore times out the rebuild; recover it operationally before
retrying. PostgreSQL releases the session lock if the owning connection or process dies.
Set the guard timeout above `processing_timeout_s` with enough margin for the slowest legitimate external
write and scheduling delay; a shorter fixed timeout causes avoidable rebuild failures.

Every direct Typesense writer outside the delivery drainer must cooperate with the same exact target key:

```ruby
SearchEngine::PostgresOutbox::Repository.new.with_delivery_target_writes_allowed(
  target_key: typesense_target_key,
  timeout_s: 5
) do
  SearchEngine::Product.upsert(record: product)
end
```

Claimed delivery requests must resolve the logical alias once and write to that pinned physical collection until
the delivery is acknowledged. The built-in `EventProcessor` does this for both upserts and deletes. Custom
collection processors must follow the same rule: a stale worker whose HTTP request completes after lease reclaim
must only be able to mutate the retired physical collection, never whichever physical collection the logical alias
points to after a later cutover.

Rollback is conservative and retry-safe. The backward-compatible form rolls back one generation only when the
alias points at the newest retained physical; once it no longer does, repeating the call returns
`action: :already_rolled_back` without another swap. When newer orphaned physicals exist, or when an operator
needs an auditable destination, pass the retained target explicitly. Add `expected_current` for compare-and-swap
protection:

```ruby
SearchEngine::Schema.rollback(
  SearchEngine::Product,
  to: "products_20250101_000000_001",
  expected_current: "products_20250102_000000_001"
)
```

The equivalent task remains backward compatible and also accepts both safety arguments:

```sh
rails 'search_engine:schema:rollback[products]'
rails 'search_engine:schema:rollback[products,products_20250101_000000_001,products_20250102_000000_001]'
```

An explicit destination must be a retained physical older than the current/expected source. Retrying after the
alias already reached that destination is a successful no-op. Any other expected-current mismatch raises
`SearchEngine::Schema::RollbackConflict` without changing the alias.

This protocol only gates delivery-target claims. Legacy single-target event claims have no target identity and
must be stopped before a rebuild. Likewise, an unguarded direct writer can still cross the swap. In
delivery-target mode, `reset_stale_processing!` rejects standalone use because reset without same-transaction
reclaim creates a false-quiescence gap. Ordinary claims directly reclaim only the bounded timed-out rows they
actually select; unselected stale rows remain `processing`, so a cutover continues to fail closed.

Forced cascade rebuilds use the guarded blue/green path and propagate any rebuild/guard failure. They never
downgrade to a live partition import. The maintenance tasks `search_engine:index:rebuild` and
`search_engine:index:rebuild_partition` are intentionally different: they write directly into the resolved
live collection and do not invoke `schema.around_rebuild`. When a guard is configured, these tasks refuse to
run unless `ALLOW_LIVE_INDEX_MAINTENANCE=true` is explicitly set. Pause all outbox consumers and direct writers
before using that override. For a guarded forced blue/green data rebuild, run
`FORCE_REBUILD=true rails 'search_engine:schema:apply[collection]'`. Without `FORCE_REBUILD`, schema apply may
complete as an in-place schema update and intentionally skip document reindexing. Hosts with no configured
guard retain the legacy live-task behavior.

## PostgreSQL outbox sync

Rails callbacks are convenient for ordinary `create`, `update`, and `destroy` flows, but they do not see
every database write. Bulk SQL imports, database triggers, background functions, and direct maintenance
scripts can change source tables without instantiating Active Record models. PostgreSQL outbox sync captures
those writes at the database layer and lets the gem process them through ActiveJob.

The flow is:

1. A row-level PostgreSQL trigger writes a durable outbox row in the same transaction as the source table
   change.
2. The trigger calls `pg_notify` as a low-latency nudge after commit. Its payload is constant for a given
   source table and collection, so PostgreSQL can fold repeated row-level notifications from one transaction
   into a single wakeup.
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
  c.postgres_outbox.drain_slot_table_name = "search_engine_outbox_drain_slots"
  c.postgres_outbox.channel = "search_engine_outbox"
  c.postgres_outbox.queue_name = "search_engine"
  c.postgres_outbox.batch_size = 1000
  c.postgres_outbox.batch_sizes = {
    product_balances: 10_000,
    calculated_products: 1_000,
    products: 2_000
  }
  c.postgres_outbox.drain_target_parallelism = 1
  c.postgres_outbox.drain_job_max_batches = 1
  c.postgres_outbox.drain_job_max_runtime_s = nil
  c.postgres_outbox.clear_cache_after_write = false
  c.postgres_outbox.poll_interval_s = 5
  c.postgres_outbox.retention_s = 7.days.to_i

  # Optional. Leave off when your deployment already guarantees one listener.
  c.postgres_outbox.advisory_lock = false

  # Optional. Leave empty for the default single-target flow.
  c.postgres_outbox.delivery_targets = lambda do
    [
      { key: :mirror_a, queue_name: :search_engine_mirror_a, parallelism: 2 },
      { key: :mirror_b, queue_name: :search_engine_mirror_b }
    ]
  end
end
```

When an upgrade changes only generated trigger-function behavior, migrations can call
`replace_search_engine_outbox_trigger_function` with the same arguments used to create the trigger. It runs
`CREATE OR REPLACE FUNCTION` without dropping and reattaching the existing trigger, avoiding unnecessary
table-level trigger DDL locks. The trigger must already exist.

`batch_size` is the global fallback for all collections. Use `batch_sizes` when some collections are much
lighter or heavier than others. Omitted drain limits use the per-collection values; explicit `limit:`
arguments still override the map and use one global cap for that drain.

When searches set `use_cache`, enable `clear_cache_after_write` if incremental
writes must be visible before their outbox rows are acknowledged. The drainer
clears Typesense's server-side search cache once after each processed
collection group and before database acknowledgement. If cache clearing fails,
the group remains retryable and its idempotent document writes are replayed.
The option defaults to `false` so hosts that do not use server-side search
caching do not add an unnecessary API call.

In delivery-target mode, a claim can contain several dependency-ordered collection groups. The drainer renews
all unstarted fenced leases before each group and removes any delivery it no longer owns before calling the
processor. Hosts must still size each individual collection group so its worst-case Typesense retry sequence
fits below `processing_timeout_s`; renewal protects later groups, not an unbounded current group.

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
    # Required only when c.postgres_outbox.delivery_targets is configured:
    create_search_engine_outbox_deliveries
    create_search_engine_outbox_drain_slots
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
directly and existing single-target setups do not need to create or use delivery rows, drain slots, or the
drain slot table. When you configure delivery targets, `search_engine_outbox_deliveries` stores
target-specific status, retry, lock, and queue state for each logical event. Add
`create_search_engine_outbox_drain_slots` in that delivery-target migration so the drain enqueuer can cap
queued and running drain jobs per target.

Drain slots are a generic backpressure mechanism for delivery-target mode. On each listener or polling
wakeup, the enqueuer materializes missing delivery rows, acquires idle slots from
`c.postgres_outbox.drain_slot_table_name`, and enqueues one drain job per acquired slot. If all slots for a
target are already queued or processing, that wakeup does not enqueue another job for the target.

`c.postgres_outbox.drain_target_parallelism` controls the default maximum concurrent drain jobs per delivery
target. Individual targets can override it with `parallelism:`:

```ruby
SearchEngine.configure do |c|
  c.postgres_outbox.drain_target_parallelism = 2
  c.postgres_outbox.delivery_targets = [
    { key: :mirror_a, queue_name: :search_engine_mirror_a },
    { key: :mirror_b, queue_name: :search_engine_mirror_b, parallelism: 4 }
  ]
end
```

Slot-aware drain jobs do finite work. `c.postgres_outbox.drain_job_max_batches` limits how many batches a
job may drain before yielding, and `c.postgres_outbox.drain_job_max_runtime_s` optionally adds a runtime
budget. When a slot-aware job reaches either bound while more work remains, it requeues the same slot instead
of acquiring a new one. When no more work is indicated, it releases the slot back to `idle`.

Already-enqueued target jobs without a `drain_slot` argument remain compatible. If the drain slot table
exists, those old jobs route through the slot-aware enqueuer for their continuations; if the table does not
exist, the gem falls back to the previous one-job-per-target behavior. This keeps staged rollouts safe while
hosts add the optional drain slot table.

Processors still receive event objects and return event IDs; the parent event status is refreshed from the
aggregate delivery states.

### Retiring a delivery target

Removing a target from `delivery_targets` does not infer permission to discard its persisted backlog. Remove
the target from configuration first, keep process configuration stable, then explicitly dry-run and apply
retirement:

```ruby
repository = SearchEngine::PostgresOutbox::Repository.new

preview = repository.retire_delivery_target!(
  target_key: "mirror_a",
  dry_run: true,
  reason: "mirror_a was decommissioned"
)

result = repository.retire_delivery_target!(
  target_key: "mirror_a",
  dry_run: false,
  reason: "mirror_a was decommissioned",
  operator: "deploy-2026-07-11"
)
```

Both calls reject a target that is still configured. Apply supersedes only that exact target's `pending`,
`processing`, and `failed` deliveries, clears leases, stores a complete JSON audit record in `last_error`, and
refreshes parent event status under parent-first locks. It is idempotent; a second apply reports zero rows.
Drain slots are scheduler bookkeeping and are deliberately not changed. Configuration is rechecked directly
before each database mutation, but Ruby configuration and PostgreSQL cannot share an atomic lock, so do not
change delivery-target configuration concurrently with retirement.

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

In delivery-target mode, cleanup can first call
`SearchEngine::PostgresOutbox::Repository#refresh_terminal_delivery_event_statuses!`. This bounded helper
repairs parent events whose delivery rows are all terminal but whose parent status is still non-terminal,
then normal retention cleanup can delete the repaired parent rows and cascade their deliveries.

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
