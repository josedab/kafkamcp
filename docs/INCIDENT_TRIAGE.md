# Incident Triage Playbook

> **Status:** Manual steps only. Automated scripts described below **do not
> exist yet**; this document specifies what they should do so that a contributor
> can implement them. Sections marked ⚙️ indicate where automation is planned.
>
> **Last reviewed:** 2026-07

This playbook is the reference for using KafkaMCP as part of an AI-assisted
incident response workflow. It doubles as a **design-partner demo specification**:
if you are evaluating KafkaMCP for incident-triage use, this document defines
the expected flow and outcomes so you can assess fit against your environment.

## Overview

KafkaMCP's `core-read` pack gives any MCP-compatible agent read-only, governed
access to Kafka during an incident — without granting it write access by default.
A typical triage session uses:

1. **Discovery** — understand what topics/consumer groups are involved.
2. **Lag diagnosis** — find which consumer groups are behind and by how much.
3. **DLQ analysis** — categorize and sample dead-letter queue messages.
4. **Schema check** — confirm the schema versions in play.
5. **Audit review** — confirm what (if anything) an agent did before the incident.

Writes (`ops-write` pack) — offset resets, topic changes — require a separate
approval grant and are out of scope for the initial triage phase.

## Prerequisites

- KafkaMCP running with `core-read` pack (default).
- Audit logging enabled (`audit.enabled: true`).
- `kafkamcp doctor` passes (see [OPERATIONS.md](OPERATIONS.md#disaster-recovery)).

## Phase 1: Initial triage

### Step 1.1 — Snapshot cluster state

```bash
# Run with any MCP client or directly via the doctor command
kafkamcp doctor --json > /tmp/triage-snapshot-$(date +%s).json
```

Expected output: JSON object with `config_valid`, `identity_ok`, `policy_ok`,
`cluster_connectivity`, and `tools` sections. Any `false` value indicates a
configuration or connectivity problem.

⚙️ **Planned automation:** A `make triage-snapshot` target that captures the
doctor output, Prometheus metrics, and the audit ring buffer into a single
timestamped bundle.

### Step 1.2 — Identify lagging consumer groups

**Agent prompt:**
```
List all consumer groups with lag above 1000 messages. Sort by total lag descending.
```

**Tool called:** `kafka_list_consumer_groups` followed by
`kafka_describe_consumer_group` for each group.

**Expected outcome:** A table of consumer groups showing total lag and the most
lagged partitions. Groups with state `Dead` or `Empty` are likely to be the
primary suspects.

### Step 1.3 — Sample the DLQ

**Agent prompt:**
```
Analyze the dead-letter queue for the orders service.
Categorize the errors and show me the 5 most recent messages.
```

**Tool called:** `kafka_analyze_dlq`

**Expected outcome:** An error-category breakdown (deserialization errors vs.
downstream timeouts vs. business logic failures) and sample payloads. The
categorization is heuristic-based and should be treated as a starting point.

### Step 1.4 — Check schema versions

**Agent prompt:**
```
What schema version is currently registered for orders.created-value?
Is there a schema change in the last 24 hours?
```

**Tool called:** `kafka_get_schema`, `kafka_list_schemas`

**Expected outcome:** Current schema version and a list of all registered
versions with registration timestamps.

## Phase 2: Targeted investigation

### Step 2.1 — Trace a specific message

If a specific `correlation_id` or message key is known:

```
Trace the message with key "ord-9842" in the orders.created topic.
Show me the last 3 offsets for that key.
```

**Tool called:** `kafka_search` with a key filter.

### Step 2.2 — Check the audit trail

```
Show me all tool calls made by agent "incident-agent" in the last 30 minutes.
```

**Resource read:** `kafka://audit/log`

**Expected outcome:** A list of audit log entries showing which tools were
called, their parameters, and outcomes. This answers "did the agent do anything
that could have caused this?"

### Step 2.3 — Consumer group detail

```
Describe the order-processor consumer group. Show me the per-partition lag and
the last committed offset timestamps.
```

**Tool called:** `kafka_describe_consumer_group`

## Phase 3: Remediation (requires `ops-write` approval)

> Remediation steps are **out of scope** for `core-read` and require an
> `ops-write` approval grant. Do not attempt these steps without explicit
> authorization.

### Step 3.1 — Request an approval grant

```yaml
# In kafkamcp.yaml, ensure the agent is listed in policies with ops-write enabled.
# Then request a short-lived approval grant:
```

**Tool called:** `kafka_approval` action=`request`

### Step 3.2 — Reset consumer group offsets (dry-run first)

```
Reset the order-processor consumer group to the earliest offset
on the orders.created topic. Dry-run first.
```

**Tool called:** `kafka_reset_offsets` with `dry_run: true` first, then
`dry_run: false` after reviewing.

## Local reproducible demo

The incident-demo environment (`examples/incident-demo/`) provides a
fully automated, reproducible Kafka cluster seeded with the scenario
described in this playbook. Use it to practise triage steps or to run
a design-partner evaluation without needing a production cluster.

```bash
# 1. Start the environment (Apache Kafka profile — explicit, not default)
make incident-demo-up

# 2. Seed topics, backlog, DLQ records, and the lagging consumer group
make incident-demo-setup

# 3. Verify: exits 0 only when all topics present AND total_lag > 0
make incident-demo-verify

# 4. Run KafkaMCP against the demo
bin/kafkamcp --config examples/incident-demo/kafkamcp-demo.yaml

# 5. (Optional) machine-readable verify output
bin/incident-demo -brokers localhost:9092 -prefix incident-demo. -json verify

# 6. Clean up — deletes incident-demo-lagging-consumer group then prefixed topics
make incident-demo-cleanup

# 7. Tear down containers and volumes
make incident-demo-down
```

For Redpanda, substitute `make incident-demo-up-redpanda` / `make incident-demo-down-redpanda`.

The seeded scenario maps directly onto Phases 1–2 of this playbook:

| Demo element | Playbook step |
| --- | --- |
| `incident-demo-lagging-consumer` with `total_lag > 0` | Phase 1.2 — lag identification |
| `incident-demo.orders.dlq` with error headers | Phase 1.3 — DLQ analysis |
| Schema-drift messages in `incident-demo.orders` | Phase 1.4 — schema check |

See [`examples/incident-demo/README.md`](../examples/incident-demo/README.md)
for the full seeding specification.

## Expected outcomes (design-partner evaluation)

When evaluating KafkaMCP for incident triage, the expected outcomes are:

| Step | Expected outcome | Pass criteria |
| --- | --- | --- |
| Doctor check | All health checks pass | No `false` in `cluster_connectivity` |
| Lag identification | Lagging groups returned within 5s | `incident-demo-lagging-consumer` listed with `total_lag > 0` |
| DLQ analysis | Error categories returned | At least one category with sample messages from `incident-demo.orders.dlq` |
| Schema check | Current schema version returned | Version number and field list shown |
| Audit query | Recent agent actions returned | Entries show tool name, timestamp, agent ID |

## Supported remediation boundary

KafkaMCP diagnoses lag, DLQ failure categories, and schema compatibility. It
does not claim an autonomous replay/remediation feature. Operators can inspect
records with `kafka_consume` and deliberately republish corrected records with
the approval-gated `kafka_produce` tool, preserving an explicit audit trail.
The local `make incident-demo-*` environment is the reproducible validation
path for this workflow.

## Related documents

- [OPERATIONS.md](OPERATIONS.md) — production runbook for credential rotation,
  audit export, and incident containment.
- [docs/COMMUNITY.md](COMMUNITY.md) — design-partner feedback template.
- [docs/ROADMAP.md](ROADMAP.md) — Phase 2 validation assets (incident demo) on
  the roadmap.
