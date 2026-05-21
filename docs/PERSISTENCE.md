# Persistence in KafkaMCP

> **Status: pluggable as of [ADR-006](./adr/006-persistent-engine-store.md).**
> An embedded SQLite backend is now available behind `server.persistence`.
> The historical in-memory behaviour remains the default, so existing
> deployments are unchanged unless they explicitly opt in.

This file is referenced by the startup banner emitted by `kafkamcp serve`:

```
VOLATILE STATE: keys, audit ring-buffer, rate-limit buckets, sessions, ...
```

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
  State persists across restarts; back it up by copying the file. Use
  this for single-node KafkaMCP deployments where durability matters.

## What persists on disk

| Item | Where |
|---|---|
| Audit log | If `audit.log_file` is set, structured-JSON audit lines are appended to that file. Without `log_file`, audit records live only in an in-memory ring buffer (`audit.max_entries`). |
| Telemetry / traces | Exported live to the configured OTLP endpoint. The server keeps no local copy. |
| Prometheus metrics | Scraped over HTTP. Counters reset on restart by design. |
| **Materialised views** (when `backend: sqlite`) | View definitions and view records are persisted in the SQLite `kv` table under namespaces `viewstore.definitions` and `viewstore.records`. Restored on next start. |

## What is volatile (lost on restart)

The following subsystems still use process-local maps and channels with no
persistence layer. Their data is destroyed when the server exits and must be
re-created by clients on the next boot. They are scheduled for migration to
the engine backend, one PR at a time, following the pattern documented in
[ADR-006](./adr/006-persistent-engine-store.md).

- **Encryption** — keys, agent policies, field-encryption configs, key
  versions. Restart → all encrypted data becomes undecryptable.
- **Audit (ring buffer)** — the in-memory tail of the audit log. The `log_file`
  on disk is the only durable copy.
- **Rate limiting** — token-bucket state per agent and globally. Restart =
  fresh quota.
- **Streaming sessions** — open consumer sessions (`kafka_stream`). Restart
  drops all of them.
- **Chaos experiments** — recorded in `chaos.Engine` and never written out.
  (Experimental; see below.)
- **WASM plugin runtime** — uploaded modules, their sandbox configs, run
  counters. (Experimental.)
- **Agent memory** — per-agent memory entries (`internal/agentmemory`).
  Restoration after migration is straightforward; tracked as a follow-up.
- **Digital twin / replay / replay-studio / dag-workflow / task queue /
  state machine / persona / tenant / collaboration / config-VCS /
  cost-attribution / sandbox / catalog / etc.**
  — every "engine" under `internal/` keeps state in `sync.RWMutex`-guarded
  maps.
- **Plugin registry, agent profiles, observability sampling counters** —
  also in-memory only.

## What "experimental" means

`config.experimental.*` flags gate features whose implementation is a
metadata-only simulation. Even with the flag enabled the tool runs locally
and does not affect a real Kafka cluster:

- `experimental.chaos` — chaos injection records experiments but does **not**
  partition brokers, slow consumers, or break schemas.
- `experimental.wasm_runtime` — `kafka_wasm_execute` does **not** execute
  WebAssembly; it returns a canned `{"status":"ok"}` payload.
- `experimental.kafka_proxy` — the proxy accepts Kafka client connections
  but, in the absence of a wired `OnTranslated` MCP callback, responds with
  hardcoded "ok" / "unknown server error" frames. Do **not** point real
  clients at it.
- `experimental.migration` — `kafka_autopilot_execute` and
  `kafka_autopilot_rollback` are gated because the underlying
  `migration.Planner.ExecutePlan` does **not** actually mutate the target
  cluster; it only updates in-process status counters. Dry-runs, assessment,
  and plan generation remain available without the flag.
- `experimental.control_plane` — `kafka_controlplane_*` tools fabricate
  org/instance/billing data; there is no SaaS layer behind them.
- `experimental.marketplace` — `kafka_plugin_install` only flips an
  in-memory boolean. It does **not** download, verify, or sandbox a real
  plugin artifact.

Each flag, when enabled, also emits a loud `WARN` log at startup so the
behaviour is obvious in operational dashboards.

## Recommended mitigations

Treat the still-volatile subsystems as request-scoped:

1. **Audit** — set `audit.log_file` and ship the file to your log pipeline.
2. **Encryption keys** — manage them externally (Vault/KMS) and load on
   startup; do not rely on the in-memory key store for anything you cannot
   recreate.
3. **Streaming sessions / pipelines / dag-workflows** — treat as
   request-scoped. Agents must be prepared to re-create them after a restart.
4. **Rate limiting** — accept that buckets reset on restart. If you need
   durable quota, enforce it at an upstream proxy.
5. **Disable experimental flags in production** — they are off by default
   and should stay off.

## Roadmap

The pluggable backend introduced by [ADR-006](./adr/006-persistent-engine-store.md)
covers the **materialised view store** in this release. The remaining
engines (`agentmemory`, `audit`, `ratelimit`, ...) keep their legacy
in-process maps; they are scheduled for incremental migration using the
same `NewEngineWithBackend(ctx, logger, be)` pattern. See the ADR for
the migration template.

