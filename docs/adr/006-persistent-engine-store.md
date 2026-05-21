# ADR-006: Persistent Engine Store with Pluggable Backend

## Status

Accepted

## Context

[ADR-005](./005-engine-pattern.md) introduced the engine pattern that
roughly 100 KafkaMCP subsystems use to hold their state. Every engine
keeps a `sync.RWMutex` + `map[string]*T` in process memory. That works
for short-lived demos but means **every restart wipes every piece of
engine state**: agent memory, materialised views, rate-limit budgets,
in-flight workflows, the audit ring buffer, and so on.

The pre-existing `docs/PERSISTENCE.md` enumerates what was volatile.
This ADR records the decision to make persistence pluggable so operators
can opt into durability without forking the codebase.

### Requirements

- **Zero-config default** — existing deployments must keep working
  unchanged. The historical in-memory behaviour stays the default.
- **Single-binary deploy story** — KafkaMCP is shipped as one Go binary.
  Adding "you must run Postgres" would be a regression for the
  laptop / edge / single-node use case.
- **No cgo** — KafkaMCP cross-compiles to several targets. A pure-Go
  SQLite is the only embedded option that meets this constraint.
- **Type safety** — engines should not have to hand-roll
  marshalling/unmarshalling.
- **Concurrency safety** — backends must serialise writes correctly under
  load.

### Options considered

1. **Postgres-only** — Reject. Forces an external dependency on every
   deployment.
2. **BoltDB / bbolt** — Pure Go, embedded, but writer-locked. No good
   migration story to a real database.
3. **modernc.org/sqlite** — Pure Go SQLite. WAL mode. Single file on
   disk. Trivial backup. Battle-tested. Easy to swap out for Postgres or
   etcd later if needed. Chosen.

## Decision

Introduce a narrow `backend.Backend` interface in
`internal/engine/backend/` with two implementations:

- `inmem` — historical sync-map behaviour (default).
- `sqlite` — embedded SQLite via `modernc.org/sqlite`, WAL mode,
  `synchronous=NORMAL`, busy-timeout 5s, max-open-conns 1 (writers
  serialise on a single connection to avoid `SQLITE_BUSY`).

The interface is intentionally minimal:

```go
type Backend interface {
    Get(ctx context.Context, namespace, key string) ([]byte, error)
    Set(ctx context.Context, namespace, key string, value []byte) error
    Delete(ctx context.Context, namespace, key string) error
    List(ctx context.Context, namespace, prefix string) ([]Entry, error)
    Close() error
}
```

A generic typed helper, `engine.Store[T]`, lives at
`internal/engine/store.go`. It JSON-marshals values and partitions them
by namespace so multiple engines can safely share one backend.

Configuration:

```yaml
server:
  persistence:
    backend: sqlite          # or "inmem" (default)
    path: /var/lib/kafkamcp/state.db
```

The server constructs the backend in `internal/server/server.go::New()`
and threads it into engine constructors. Engines opt into persistence by
adding a `NewEngineWithBackend(ctx, logger, backend)` constructor; the
legacy `NewEngine(logger)` keeps working unchanged.

## Migration path

Engine migration is incremental. This change migrates the **viewstore**
engine end-to-end as the worked example, because it is a pure
key-value store (view definitions + view records) and is the most
visible to operators (materialised views routinely contain hours of
ingested state).

The remaining state-bearing engines (`agentmemory`, `audit`,
`ratelimit`, `cdc`, `dagworkflow`, ...) keep their legacy in-memory
behaviour for now. They can be migrated one PR at a time using the same
pattern:

1. Add `NewEngineWithBackend(ctx, logger, be)` returning the existing
   engine wired to two `engine.Store[T]` (one per logical collection).
2. In `load()`, populate the in-memory maps from `List("")`.
3. In each mutating method, call `store.Set(...)` after the in-memory
   update while still holding the engine lock.
4. In `init_platform.go`, swap the `NewEngine(logger)` call for
   `NewEngineWithBackend(ctx, logger, be)` and tolerate the (rare) load
   error by falling back to a fresh in-memory engine.

## Consequences

**Positive**

- Operators can run KafkaMCP as a stateful single-node service with
  trivial backup story (`cp state.db state.db.bak`).
- The new interface lets us add a Postgres or etcd backend later
  without touching engines.
- The `Store[T]` helper continues the direction set by ADR-005:
  engines stop reinventing storage plumbing.

**Negative**

- One more dependency (`modernc.org/sqlite` and its libc shim) pulled
  into the binary. The binary grows by a few MB.
- WAL mode writes a `-wal` and `-shm` sidecar file next to the db.
  Operators that mount a read-only filesystem need to point
  `server.persistence.path` at a writable directory.
- Single-connection writes are a deliberate trade-off: it serialises
  writers but eliminates `SQLITE_BUSY` retries.

**Out of scope**

- Cross-process replication. Operators that need HA should front
  KafkaMCP with a single durable instance and treat replicas as
  caches.
- Schema migrations beyond the initial `kv` table. The table is
  intentionally narrow so future schema work can be additive.
