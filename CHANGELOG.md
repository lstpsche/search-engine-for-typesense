# Changelog

## 30.1.8.22

- Add `schema.around_rebuild`, an exact-once guard around full physical create/index/alias-swap/retention
  lifecycles. In-place schema updates remain outside the guard, while guard and indexing failures propagate
  without leaking interrupted physical collections.
- Add PostgreSQL delivery-target cutover locks. Full rebuilds can pause new claims, wait for acknowledged
  processing deliveries, and exclude cooperating direct Typesense writers with one deterministic target key.
  Locks are session-safe, timeout-bounded, and automatically released when the database session dies.
- Gate every delivery claim before target row work. Timed-out leases are reclaimed only for the bounded rows
  actually selected; unselected rows remain `processing`, and stale older operations are settled before newer
  siblings so delete/recreation ordering cannot reverse.
- Pin built-in outbox upserts and deletes to one resolved physical collection for the claimed operation, so a
  delayed stale HTTP request cannot follow a logical alias onto a replacement collection after cutover.
- Reject standalone `reset_stale_processing!` in delivery mode because reset without same-transaction reclaim
  can create false quiescence. Single-target event mode is unchanged.
- Make forced cascade rebuild failures propagate instead of downgrading to live partition imports.
- Mark `search_engine:index:rebuild` and `search_engine:index:rebuild_partition` as live-maintenance paths. When
  `schema.around_rebuild` is configured, they now require explicit `ALLOW_LIVE_INDEX_MAINTENANCE=true` after
  operators pause outbox consumers and direct writers. `search_engine:schema:apply` accepts
  `FORCE_REBUILD=true` as the explicit guarded full-data rebuild path.

No database migration is added by this release. Existing PostgreSQL outbox delivery/slot migrations remain
compatible.

## 30.1.8.21

- Bound PostgreSQL outbox claim ranking and harden delivery coalescing with parent-first locking, fenced
  delivery leases, lock-order-safe stale resets, and per-event processing failure isolation.
- Add explicit, audited, idempotent delivery-target retirement with a required dry-run/apply decision. Target
  retirement never infers intent from configuration removal and does not alter drain slots.
- Add strict ordered Typesense import response parsing and `upsert_bulk(..., on_failure: :return)` for stable
  row-level outcomes. Malformed, ambiguous, short, and long responses now fail closed.
- Remove raw Typesense `response` data from bulk upsert result/error metadata because it can contain submitted
  documents. Callers that used `result[:response]` must migrate to `row_results` and aggregate counters.
- Preserve safe structured error metadata while recursively redacting secrets and payload-bearing fields.
- Reject unsupported dispatch configuration/overrides instead of silently falling back to inline execution.
- Make partial async partition imports retryable and observable. Custom partition run stores must add
  `record_attempt(run_id:, partition_key:, summary:, error:)` as a non-terminal transition.

No database migration is added by this release. Existing PostgreSQL outbox delivery/slot migrations remain
compatible.
