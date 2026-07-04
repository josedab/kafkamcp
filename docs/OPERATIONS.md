# Production Operations

Operational guidance for running KafkaMCP as a self-hosted service. KafkaMCP is a
stateless-by-default gateway; the only durable state is optional (approval grants
and persistence-aware engines when the sqlite backend is enabled).

## Guarded-pack prerequisites

`ops-write` and `governance` are **not usable on stdio**. The config validator
enforces all five conditions at startup; any failure is a hard error:

| Requirement | Config / env | Validation error if absent |
| --- | --- | --- |
| Network transport | `server.transport: sse` or `streamable-http` | `"stdio is core-read only"` |
| Cryptographic identity | `policies.identity.provider: static\|oidc\|mtls` | `"policies.identity.provider must be one of static, oidc, mtls"` |
| Header fallback disabled | `policies.identity.allow_header_fallback: false` (default for cryptographic providers) | `"allow_header_fallback must be false"` |
| Default-deny authorization | `policies.default_deny: true` | `"policies.default_deny must be true"` |
| Signing key | `KAFKAMCP_APPROVAL_SIGNING_KEY` ≥32 bytes | `"must contain at least 32 bytes"` |

Two-person approval (`allow_self_approval: false`) is the structural default and
does not require explicit configuration, but set it explicitly in production to
make the intent clear in the config file:

```yaml
policies:
  approval:
    allow_self_approval: false  # default; explicit for auditability
```

**Migration note:** any deployment that previously enabled `ops-write` or
`governance` on stdio now fails startup. Switch to a secure network transport
or use `core-read` only.

## Service level objectives (starting targets)

These are **suggested starting SLOs** to calibrate against your environment, not
guarantees. Measure with the Prometheus metrics exposed on the metrics port.

| SLI | Suggested SLO |
| --- | --- |
| Tool call availability (non-5xx) | 99.9% monthly |
| Read tool p99 latency (excluding broker time) | < 250 ms |
| Approval request/approve p99 latency | < 100 ms |
| Kafka connectivity error budget | < 0.1% of calls |

## Capacity guidance

- KafkaMCP is CPU- and network-light; most cost is broker round-trips.
- Bounded consume/search cap the work per call; keep client-supplied limits sane
  via policy.
- `core-read` with the in-memory backend may run multiple replicas.
- `ops-write`, `governance`, or SQLite persistence require **exactly one active
  replica**. The Helm chart rejects unsafe replica/HPA settings for these
  configurations.
- Use active/passive failover for guarded deployments. Never run two active
  SQLite writers or rely on load-balancer stickiness as a correctness boundary.

## Health, readiness, and metrics

The auxiliary HTTP listener is configured independently from the MCP listener:

```yaml
server:
  metrics_host: 127.0.0.1  # default; use 0.0.0.0 in a protected container network
  metrics_port: 3101       # default: server.port + 1
```

| Endpoint | Meaning | Kubernetes use |
| --- | --- | --- |
| `/livez` | Process and HTTP listener are alive; no external dependency call | Liveness/startup |
| `/healthz` | Default Kafka cluster responds | Diagnostic health |
| `/ready` | Kafka and the persistence backend are ready | Readiness |
| `/metrics` | Prometheus metrics | ServiceMonitor/scrape |

Do not use `/healthz` or `/ready` as liveness probes: a Kafka outage must remove
the pod from service, not trigger restart storms. Binding metrics to
`0.0.0.0` exposes these endpoints to the container network, so restrict the
metrics port with NetworkPolicy/firewall rules.

## Persistence, backup & restore (SQLite)

When `server.persistence.backend: sqlite` is set, approval grants and
persistence-aware engine state live in the configured `path`.

### Single-node persistence

The SQLite backend is designed for **single-node, single-writer** deployments
only. It uses WAL mode for read concurrency but does **not** support concurrent
writes from multiple processes. Do **not** share a SQLite file across replicas
via network storage (NFS, SMB, EFS, etc.) — SQLite's file-locking primitives are
unreliable over most network filesystems, which can cause database corruption.

### Backup

Use SQLite's online backup while the process is running:

```bash
sqlite3 /var/lib/kafkamcp/state.db ".backup '/backups/state-$(date +%F).db'"
```

For container environments, also copy the WAL file (`state.db-wal`) before or
immediately after the backup to ensure a consistent snapshot. Schedule periodic
backups (e.g. hourly via cron or Kubernetes CronJob) and verify integrity:

```bash
sqlite3 /backups/state-$(date +%F).db "PRAGMA integrity_check;"
```

### Restore

1. Stop the KafkaMCP process.
2. Replace `state.db` (and remove any stale `.db-wal` / `.db-shm` sidecar files).
3. Restart the process.

Approval grants are short-lived; expired grants are harmless after restore. On
restore, reissue any in-flight grants that were reverted.

### HA limitations

The SQLite backend does **not** support multi-replica high availability.
Approval-grant state is local to the single writer. For availability:

- **Active/passive guarded deployment:** keep one active process and fail over
  by stopping it before promoting the passive. Reissue in-flight grants.
- **Stateless read replicas:** `core-read` plus `backend: inmem` can scale
  horizontally; process-local rate-limit and metrics state is expected.
- **Sticky sessions:** available in the Helm service/Ingress for volatile
  read-side sessions, but they do not make guarded writes safe across replicas.

There is no built-in leader election or cross-node replication. Do not expose
the SQLite file over a network volume for concurrent access.

## Disaster recovery

- **Config** is the source of truth; keep `kafkamcp.yaml` in version control.
- **Secrets** (masking key, Kafka/SR credentials, OIDC secrets) come from the
  environment — restore them from your secrets manager, not from backups.
- **Grants** are ephemeral; DR does not need to preserve them.
- RTO is dominated by restarting the process and re-establishing broker
  connectivity; run `kafkamcp doctor` post-recovery to confirm readiness.

## Upgrade & rollback

- Binaries are versioned and released with checksums, SBOMs, and provenance
  (see [`RELEASE.md`](RELEASE.md)).
- **Upgrade:** guarded/SQLite deployments use a stop-then-start (`Recreate`)
  rollout. Read-only in-memory deployments may use a rolling update.
- **Rollback:** redeploy the previous version. Config and tool schemas are
  covered by the stable compatibility snapshot (see the readiness report); a
  rollback within a minor series is config-compatible.

## Credential rotation

- **Approval signing key** (`KAFKAMCP_APPROVAL_SIGNING_KEY`): required for
  `ops-write` or `governance` (≥32 bytes). For rotation, put the old key in a
  second environment variable and list that name under
  `policies.approval.previous_signing_key_envs`, switch the current key, then
  restart. Historical records remain verifiable; in-flight grants signed by the
  old key must be requested again. Remove the previous key after the configured
  `terminal_retention_seconds` window (seven days by default).
- **Masking HMAC key** (`KAFKAMCP_MASKING_KEY`): rotating the key changes all
  tokenization outputs. Rotate during a maintenance window if downstream systems
  join on tokens; otherwise rotate freely (fail-closed if unset).
- **Kafka / Schema Registry credentials:** update the environment and restart;
  use `kafkamcp doctor` to confirm connectivity.
- **OIDC / mTLS material:** rotate at the identity provider / CA; KafkaMCP picks
  up new JWKS automatically for OIDC and re-reads the CA bundle on restart for
  mTLS.

## Incident response

1. Run `kafkamcp doctor --json` to snapshot config/identity/policy/connectivity.
2. Check the metrics endpoint for error spikes and denied-call counters.
3. Read `kafka://audit/log` and review approval lifecycle events for privileged
   actions in the incident window.
4. To contain an agent, revoke its outstanding grants (`kafka_approval`
   action=revoke) and/or tighten `policies` and restart.

## Audit export

- Privileged actions and approval lifecycle events are emitted to the structured
  logger and audit ring buffer. The ring buffer survives restart when SQLite
  persistence is enabled.
- Query recent entries through the `kafka://audit/log` resource.
- For long-term, exportable audit trails, set `audit.log_file` and ship the
  structured JSON stream to your SIEM/log store; the ring buffer remains
  bounded even when persisted.

## Readiness

Run the objective readiness report before promoting a build:

```bash
make readiness
```

It summarizes tests, vulnerabilities, generated-doc freshness, and (where
available) the stable tool snapshot and integration matrix. It does **not**
assert time-based reliability or customer-adoption gates.

Before a Kubernetes promotion also run:

```bash
make helm-check
make security
```
