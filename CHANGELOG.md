# Changelog

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
