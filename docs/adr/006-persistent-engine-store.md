# ADR-006: Embedded Persistence for Guarded Operations

## Status

Accepted, revised after the supported-surface cleanup.

## Context

KafkaMCP needs durable approval, idempotency, audit-tail, and rate-limit state
without requiring an external database for a single-node deployment. Guarded
state transitions must also remain atomic when two processes briefly overlap
during controlled failover.

## Decision

Provide a narrow `internal/engine/backend.Backend` with:

- `inmem` as the zero-config default.
- pure-Go SQLite (`modernc.org/sqlite`) in WAL mode for durable single-node
  operation.
- atomic byte-level compare-and-swap implemented as one SQL statement.

`engine.Store[T]` adds typed JSON storage while retaining exact raw bytes for
CAS versions.

Persisted namespaces are:

- `approval_grants`
- `approval_token_index`
- `idempotency_receipts`
- `audit.entries`
- `ratelimit.buckets`

Configuration:

```yaml
server:
  persistence:
    backend: sqlite
    path: /var/lib/kafkamcp/state.db
```

## Constraints

- SQLite is one active writer only.
- Do not place the database on NFS, EFS, SMB, or another shared network
  filesystem.
- Guarded/SQLite Kubernetes deployments use one replica and `Recreate`
  rollout.
- Atomic CAS hardens failover overlap but does not create multi-writer HA.

## Consequences

Positive:

- Approval grants, single-use consumption, idempotency receipts, audit tail,
  and rate budgets survive restart.
- No cgo or external service is required.
- The backend remains replaceable behind a small interface.

Negative:

- Horizontal guarded-write HA requires a future transactional remote backend
  and leader/ownership design.
- Operators must back up and integrity-check the SQLite file.

Operational procedures are documented in
[`docs/OPERATIONS.md`](../OPERATIONS.md) and
[`docs/PERSISTENCE.md`](../PERSISTENCE.md).
