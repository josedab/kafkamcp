# API Stability Contract

> **Last reviewed:** 2026-07
>
> **Authoritative source:** this file plus [`docs/tool-reference.md`](tool-reference.md)
> (generated) and [`docs/kafkamcp.schema.json`](kafkamcp.schema.json).
>
> See [`docs/VERSIONING.md`](VERSIONING.md) for the versioning policy and
> [`docs/COMPATIBILITY.md`](COMPATIBILITY.md) for the cross-version matrix.

This document is the explicit, operator-facing contract. It names every stable
surface by its exact identifier, states what is guaranteed not to change without
a MAJOR version bump and deprecation window, and defines the policy for every
tier.

---

## 1. Stable tools (core-read, v0.1.0 baseline)

The following 15 tools are **stable**. Their names, required input parameters,
and documented output field names will not change without first issuing a
deprecation notice in at least one MINOR release and incrementing MAJOR.

| Tool name | Description (brief) |
| --- | --- |
| `kafka_analyze_dlq` | Consume and categorize dead-letter queue messages |
| `kafka_check_schema_compatibility` | Validate a proposed schema against a subject's compatibility policy |
| `kafka_consume` | Read messages with flexible offset/timestamp positioning |
| `kafka_describe_consumer_group` | State, members, assignments, and per-partition lag |
| `kafka_describe_topic` | Partition details, replicas, ISR, offsets, configuration |
| `kafka_diagnose_lag` | Root-cause analysis for consumer group lag |
| `kafka_discover_clusters` | Auto-discover clusters by scanning ports and environment variables |
| `kafka_discover_tools` | Enumerate active packs, registered tools, and their tiers |
| `kafka_get_schema` | Full schema definition, field summary, compatibility, and references |
| `kafka_health` | Read-only cluster health check using live metadata |
| `kafka_list_consumer_groups` | Summary state, member count, and total lag for all groups |
| `kafka_list_schemas` | All Schema Registry subjects with version, type, and compatibility |
| `kafka_list_topics` | All topics with partition count, replication factor, and message count |
| `kafka_metrics` | Aggregated local Prometheus metrics (no raw agent identifiers) |
| `kafka_search` | Filter messages by key, header, value content, or time range |

> **Source:** generated from `internal/server/register_*.go` by `cmd/gen-tool-docs`;
> the tier column in [`docs/tool-reference.md`](tool-reference.md) is authoritative.
> If a tool appears here but not in the generated reference as `stable | core-read`,
> this file is stale — the generated reference takes precedence.

### What "stable" guarantees for a tool

| Surface | Guarantee |
| --- | --- |
| **Tool name** | Unchanged. An alias will exist for ≥1 MINOR release if renamed. |
| **Required input parameters** | No new required parameter without MAJOR bump. |
| **Optional input parameters** | May be added in MINOR. Removed only after MAJOR bump + deprecation. |
| **Output field names** (documented fields) | Unchanged. Fields may be added in MINOR. Removed only after MAJOR bump. |
| **Output field types** | Unchanged without MAJOR bump. |
| **Error codes / error message keys** | Programmatic error keys unchanged; human-readable text may improve. |
| **Pack membership** | A stable tool may be *added* to additional packs in MINOR; never silently *removed* from a pack it already belongs to. |

### What "stable" does NOT guarantee

- The *presence* of a schema registry, Kafka Connect, or specific broker feature.
  Some tools return a no-op or descriptive error when optional infrastructure is absent.
- Response latency, throughput, or memory usage.
- Log output format.
- Metric label cardinality or exact label values.
- Internal Go package APIs (`internal/`).

---

## 2. Beta surfaces (ops-write, governance)

The following surfaces are **beta**: real and functional, but still hardening.
They may change in MINOR versions; breaking changes are documented in CHANGELOG
with migration notes. No MAJOR bump is required for a beta break, but a
CHANGELOG entry and at least one PATCH window are required.

**ops-write pack:** `kafka_produce`, `kafka_create_topic`,
`kafka_alter_topic`, `kafka_delete_topic`, `kafka_reset_offsets`,
`kafka_create_connector`, `kafka_update_connector`,
`kafka_delete_connector`, `kafka_restart_connector`, `kafka_acl_manage`, and
`kafka_approval`.

**governance pack:** `kafka_approval`, `kafka_governance_decision`,
`kafka_acl_manage`, `kafka_policy_evaluate`, and `kafka_policy_simulate`.

### Approval-grant interface stability

The approval grant lifecycle (`request → approve → use / revoke / expire`) and
the `Scope` fields (`agent_id`, `identity_method`, `tool`, `cluster`,
`resource`, `args_digest`) are treated as **beta** and documented explicitly.
Changes will be called out in CHANGELOG. HMAC-SHA-256 protects both the linked
decision chain and the current grant state; the exact serialization format is
beta.

---

## 3. No experimental or simulated surfaces

KafkaMCP ships **no simulated or placeholder tools**. Every registered tool
performs real behavior. Future incomplete work stays out of runtime packs until
it has a real implementation and integration tests, so there is no
"experimental" tool pack and no stability-exempt simulated surface.

> The previously shipped simulated tools (`kafka_chaos_*`, `kafka_wasm_*`,
> `kafka_proxy`, `kafka_migration_plan`) and the removed migration/autopilot
> tools (`kafka_migrate_*`, `kafka_autopilot_*`) are no longer registered in any
> pack and their engines were deleted from the binary.

---

## 4. Config key stability

Config keys are annotated with an `"x-stability"` field in
[`docs/kafkamcp.schema.json`](kafkamcp.schema.json). Any key lacking an
explicit annotation should be treated as **beta** until annotated.

**Stable config keys (v0.1.0 baseline):**

| Key path | Type | Default |
| --- | --- | --- |
| `server.transport` | string | `stdio` |
| `server.host` | string | `0.0.0.0` |
| `server.port` | integer | `3100` |
| `server.log_level` | string | `info` |
| `clusters[].name` | string | required |
| `clusters[].bootstrap_servers` | string | required |
| `clusters[].security_protocol` | string | `PLAINTEXT` |
| `clusters[].sasl_mechanism` | string | — |
| `clusters[].sasl_username` | string | — |
| `clusters[].sasl_password` | string | — |
| `clusters[].ssl_ca_location` | string | — |
| `clusters[].schema_registry.url` | string | — |
| `clusters[].default` | boolean | `false` |
| `policies.default_deny` | boolean | `false` |
| `audit.enabled` | boolean | `false` |
| `audit.log_file` | string | — |

The JSON Schema annotations and the table above are protected by the committed
config-contract snapshot in `internal/config/testdata/schema_contract.json`.

**Beta config keys (ops-write, governance, identity):**
`server.tool_packs`, `policies.approval.*`, `policies.identity.*`,
`server.persistence.*`, `server.metrics_host`,
`server.metrics_port`, `clusters[].tls_min_version`,
`clusters[].connect_tls.min_version`, `masking.*`, `telemetry.*`.

---

## 5. Resource URI stability

The following MCP resource URIs are **stable**:

| URI | Description |
| --- | --- |
| `kafka://cluster/metadata` | Cluster brokers, controller, topic/partition counts |
| `kafka://clusters` | All configured clusters |
| `kafka://topics` | Topic catalog |
| `kafka://topics/{name}` | Individual topic detail |
| `kafka://schemas` | Schema Registry subject catalog |
| `kafka://schemas/{subject}` | Individual schema definition |
| `kafka://groups` | Consumer group catalog |
| `kafka://groups/{id}` | Individual consumer group detail |
| `kafka://audit/log` | Agent audit trail (recent entries) |

No additional resource URIs are registered by the supported packs. Legacy
resource handlers outside this table are archived implementation code and are
not callable.

---

## 6. Deprecation window

| Tier | Minimum window | Process |
| --- | --- | --- |
| **Stable** | ≥1 MINOR release | Deprecation notice in `docs/tool-reference.md` + CHANGELOG; removal only in next MAJOR. |
| **Beta** | ≥1 PATCH release | CHANGELOG entry; removal may occur in MINOR with migration note. |

A deprecated stable tool continues to function and returns correct results
during the deprecation window. It is never silently degraded, rate-throttled,
or made unreachable before removal.

---

## 7. Semver guarantees per tier

| Situation | Version bump required |
| --- | --- |
| Remove or rename a stable tool | MAJOR |
| Add a required parameter to a stable tool | MAJOR |
| Remove a documented output field from a stable tool | MAJOR |
| Remove or rename a stable config key | MAJOR |
| Remove or rename a stable resource URI | MAJOR |
| Add a new stable tool | MINOR |
| Add an optional parameter to a stable tool | MINOR |
| Add a new output field to a stable tool | MINOR |
| Change a stable tool's behavior in a non-breaking way | MINOR |
| Change beta tool name, parameters, or output | MINOR (with CHANGELOG note) |
| Security patch (no interface change) | PATCH |
| Bug fix (no interface change) | PATCH |

Pre-1.0 exception: while the version is `0.x`, MINOR bumps may include
breaking changes to beta surfaces. Every such break is called out in CHANGELOG
under `### Breaking Changes`.

---

## 8. LTS and support lifecycle

| Phase | Policy |
| --- | --- |
| **Pre-1.0 (current)** | Supported version is `latest`. Security patches backported to the previous MINOR series for **90 days** after a new MINOR ships, then support ends for the older series. |
| **Post-1.0** | The `v1.x` series receives security patches for **12 months** after `v2.0` is tagged. Breaking changes deferred to MAJOR with a **6-month** minimum deprecation window for stable APIs. |

There is no LTS release designation at this time. When a release is designated
LTS, it will be marked in GitHub Releases and in CHANGELOG.

---

## 9. Breaking-change process

1. Open a GitHub issue labelled `breaking-change` with motivation and proposed
   new interface.
2. Document the before/after YAML or tool-call change in the issue.
3. Land behind a deprecation notice in a MINOR (for stable: at least one MINOR
   must ship the deprecation notice *before* removal in a MAJOR).
4. Update `CHANGELOG.md`, `docs/tool-reference.md`, and `docs/kafkamcp.schema.json`.
5. Tag the release; push notes to GitHub Releases.
6. `kafkamcp doctor` (when the check is implemented) warns on any deprecated
   config key at startup.

---

## 10. Machine-checkable snapshot

The CI suite includes a snapshot test at `internal/server/stable_snapshot_test.go`
that registers the full server in a test harness and verifies:

- Every tool listed in section 1 is present and registered under `core-read`.
- No tool listed as stable has been removed.
- No required parameter has been silently dropped.

The snapshot lives at `internal/server/testdata/` and is regenerated by
`make generate`. A PR that inadvertently removes or renames a stable tool will
fail CI. Intentional breaks require updating the snapshot *and* bumping MAJOR.
