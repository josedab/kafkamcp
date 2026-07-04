# Persistence in KafkaMCP

> **Status: pluggable as of [ADR-006](./adr/006-persistent-engine-store.md).**
> An embedded SQLite backend is now available behind `server.persistence`.
> The historical in-memory behaviour remains the default, so existing
> deployments are unchanged unless they explicitly opt in.

This file is referenced by the startup persistence warning emitted by
`kafkamcp serve`.

## TL;DR — choosing a backend

```yaml
server:
  persistence:
    backend: sqlite                       # or "inmem" (default)
    path: /var/lib/kafkamcp/state.db
```

- `inmem` (default) — historical behaviour. State lives in process
  memory and is lost on restart.
- `sqlite` — embedded pure-Go SQLite (`modernc.org/sqlite`), WAL mode.
  State persists across restarts; use SQLite's online backup command described
  in [`OPERATIONS.md`](OPERATIONS.md), not a live raw file copy. Use this for
  single-node KafkaMCP deployments where durability matters.

## What persists on disk

| Item | Where |
|---|---|
| Audit log | If `audit.log_file` is set, structured-JSON audit lines are appended to that file. Without `log_file`, audit records live only in an in-memory ring buffer (`audit.max_entries`). |
| Telemetry / traces | Exported live to the configured OTLP endpoint. The server keeps no local copy. |
| Prometheus metrics | Scraped over HTTP. Counters reset on restart by design. |
| **Audit ring-buffer** (when `backend: sqlite`) | The in-memory tail of recent audit entries is mirrored to namespace `audit.entries` so the recent history survives a restart. The `audit.log_file` append stream is still the long-term durable copy. |
| **Rate-limit token buckets** (when `backend: sqlite`) | Global and per-agent token-bucket state is persisted under namespace `ratelimit.buckets`. Restarting no longer hands every agent a free fresh quota. |
| **Approval grants and token indexes** (when `backend: sqlite`) | Requests, signed decision chains, current grant state, and token-to-grant indexes persist under `approval_grants` and `approval_token_index`. Single-use consumption uses atomic compare-and-swap. |
| **Idempotency receipts** (when `backend: sqlite`) | Pending reservations and successful retry receipts persist under `idempotency_receipts`. Reservation, completion, and abort use atomic compare-and-swap. |

## What is volatile (lost on restart)

The supported product intentionally keeps these caches/process metrics
transient:

- Schema Registry cache entries.
- Prometheus counters and histograms.
- OIDC/JWKS and transport connection caches.
- Cluster-discovery results and lag-analysis calculations.

They are reconstructed from configuration and external systems after restart;
no supported workflow/session engine depends on hidden process state.

## No simulated tools

KafkaMCP ships **no simulated or placeholder tools**. There is no
`experimental` config block and no metadata-only simulation gated behind a
feature flag. Future incomplete work stays out of runtime packs until it has a
real implementation and integration tests.

> The previously shipped simulated tools and their engines were deleted from
> the binary: chaos injection, the WASM runtime, the Kafka proxy, and migration
> planning (`kafka_chaos_*`, `kafka_wasm_*`, `kafka_proxy`,
> `kafka_migration_plan`), along with the control-plane, marketplace/plugin,
> agent-store, and cloud-deploy surfaces. None can be re-enabled by any config
> flag.

## Recommended mitigations

1. **Audit** — set `audit.log_file` and ship it to your log pipeline.
2. **Guarded writes** — use SQLite so approval and idempotency state survives
   restart.
3. **Rate limiting** — SQLite preserves buckets; with `inmem`, enforce any
   cross-replica/global quota at an upstream proxy.

## Concurrency boundary

The backend's compare-and-swap primitive is a single atomic SQL statement, so
separate managers that briefly overlap during failover cannot both consume one
approval token or reserve one idempotency key. This hardens transitions, but it
does **not** make shared-file multi-replica SQLite supported: WAL locking,
background engines, and operational ownership still require one active writer.
