# Incident-Demo Environment

This directory contains the Docker Compose assets and Go binary for the
KafkaMCP incident-triage demonstration scenario. It seeds a deterministic,
fully reproducible Kafka environment: five prefixed topics, realistic DLQ
records, schema-drift messages, and a committed-but-lagging consumer group
that makes `kafka_diagnose_lag` immediately useful.

## Services

| Service | Port | Profile | Purpose |
|---------|------|---------|---------|
| Apache Kafka (KRaft) | 9092 | `kafka` | Kafka broker |
| Schema Registry | 8081 | `kafka` | Schema Registry wired to Kafka |
| Redpanda | 9092 | `redpanda` | Vendor-neutral compatibility profile |
| Schema Registry (Redpanda) | 8081 | `redpanda` | Schema Registry wired to Redpanda |

> **Both profiles are explicit.** There is no default profile; you must pass
> `--profile kafka` or `--profile redpanda`. Never start both at the same time —
> they bind the same ports.

## Quick start (Apache Kafka)

```bash
# 1. Start the environment — explicit --profile kafka required
make incident-demo-up
# Equivalent direct command:
#   docker compose -f examples/incident-demo/docker-compose.yaml \
#     --profile kafka up -d

# 2. Build the demo binary and seed the scenario
make incident-demo-setup

# 3. Verify the scenario (must exit 0 and report total_lag > 0)
make incident-demo-verify

# 4. Run KafkaMCP against the demo cluster
bin/kafkamcp --config examples/incident-demo/kafkamcp-demo.yaml

# 5. Clean up — deletes the lagging consumer group then the prefixed topics
make incident-demo-cleanup

# 6. Stop and remove containers and volumes
make incident-demo-down
# Equivalent:
#   docker compose -f examples/incident-demo/docker-compose.yaml \
#     --profile kafka down -v
```

## Quick start (Redpanda compatibility profile)

```bash
# Start with Redpanda instead of Apache Kafka
make incident-demo-up-redpanda
# Equivalent:
#   docker compose -f examples/incident-demo/docker-compose.yaml \
#     --profile redpanda up -d

# The same setup / verify / cleanup targets work unchanged:
make incident-demo-setup
make incident-demo-verify
make incident-demo-cleanup

# Stop and remove
make incident-demo-down-redpanda
# Equivalent:
#   docker compose -f examples/incident-demo/docker-compose.yaml \
#     --profile redpanda down -v
```

## Demo scenario: what gets seeded

`make incident-demo-setup` runs `incident-demo setup` which:

1. Creates the five prefixed topics (idempotent — safe to re-run if already present).
2. Produces the following message counts into those topics.
3. Seeds the **lagging consumer group** (see below).

| Topic | Messages | Content |
|-------|----------|---------|
| `incident-demo.orders` | 54 | 2 healthy messages + 50-message backlog + 2 schema-drift records |
| `incident-demo.orders.dlq` | 2 | DLQ records with `x-failure-reason`, `x-retry-count`, `x-original-topic` headers |
| `incident-demo.payments` | 1 | Healthy payment event |
| `incident-demo.payments.dlq` | 1 | Validation failure (negative amount) |
| `incident-demo.notifications` | 1 | Notification event |

### Lagging consumer group

`setup` creates a consumer group named **`incident-demo-lagging-consumer`** that:

- Reads from `incident-demo.orders` starting at the earliest offset
- **Commits offsets for only 5 messages**, leaving the rest unconsumed
- Exits without closing the consumer — the committed lag is persistent

This means `kafka_diagnose_lag` will report a non-zero `total_lag` against
`incident-demo-lagging-consumer`, making the group immediately discoverable by
an agent with a prompt like *"which consumer groups are lagging?"*.

### Verify success criterion

`make incident-demo-verify` runs `incident-demo verify` and exits non-zero
unless **both** of the following are true:

- All five expected topics are present
- `incident-demo-lagging-consumer.total_lag > 0`

Use `-json` for machine-readable output:

```bash
bin/incident-demo -brokers localhost:9092 -prefix incident-demo. -json verify
```

Example successful output:

```json
{
  "prefix": "incident-demo.",
  "ok": true,
  "present": [
    {"name": "incident-demo.orders", "partitions": 3, "message_count": 54},
    {"name": "incident-demo.orders.dlq", "partitions": 1, "message_count": 2},
    {"name": "incident-demo.payments", "partitions": 2, "message_count": 1},
    {"name": "incident-demo.payments.dlq", "partitions": 1, "message_count": 1},
    {"name": "incident-demo.notifications", "partitions": 1, "message_count": 1}
  ],
  "message_counts": [...],
  "lag_group": "incident-demo-lagging-consumer",
  "total_lag": 49
}
```

## Cleanup details

`make incident-demo-cleanup` runs `incident-demo cleanup` which:

1. **Deletes the `incident-demo-lagging-consumer` consumer group first** using
   the Kafka Admin API (`DeleteGroups`). `GROUP_ID_NOT_FOUND` is treated as
   success so the command is idempotent.
2. Deletes each of the five prefixed topics. A safety check ensures every
   topic name starts with `incident-demo.` before deletion; topics with any
   other prefix are never touched.
3. Exits non-zero only on unexpected errors; missing topics are skipped silently.

> **Safe prefix guarantee:** the binary refuses to run cleanup if the prefix is
> shorter than 3 characters or matches reserved Kafka prefixes (`__`, `connect-`,
> `schema-`, `_schemas`).

## Binary reference

```
incident-demo [flags] <command>

Commands:
  setup    Create demo topics and populate with scenario data
  verify   Check that topics and messages exist; exit 1 if not
  cleanup  Delete consumer group then all topics matching the prefix

Flags:
  -brokers string   Kafka bootstrap servers (default "localhost:9092")
  -prefix  string   Topic prefix; cleanup is scoped to this prefix (default "incident-demo.")
  -json             Emit machine-readable JSON output
  -v                Verbose logging
```

## Running the compatibility contract tests

After `make incident-demo-up-redpanda` and `make incident-demo-setup`:

```bash
go test -tags=integration -run TestKafkaProviderContract ./internal/kafka/ \
  -kafka-brokers=localhost:9092 -kafka-provider=redpanda
```

