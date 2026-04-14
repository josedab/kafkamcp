# Connect Claude to Your Kafka Cluster in 5 Minutes

*By Jose D. · July 2025*

---

I'm going to show you how to go from "Claude has no idea my Kafka cluster exists" to "Claude lists topics, consumes messages, and inspects consumer groups" in under five minutes. No Python scripts. No REST wrappers. No Docker Compose stack. One binary, one YAML file.

Here's what we're building:

```
Claude Desktop ←→ MCP (stdio) ←→ KafkaMCP ←→ Your Kafka Cluster
```

## Prerequisites

You need two things:
- A running Kafka cluster (local `localhost:9092` is fine)
- Go 1.22+ installed (for `go install`)

If you're running Kafka locally and don't have one handy, the fastest path:

```bash
docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_CFG_NODE_ID=0 \
  -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@localhost:9093 \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  bitnami/kafka:3.7
```

## Minute 1: Install KafkaMCP

```bash
go install github.com/kafkamcp/kafkamcp/cmd/kafkamcp@latest
```

Verify it's working:

```bash
kafkamcp version
# kafkamcp 0.1.0
```

That's it. Single binary, no dependencies.

## Minute 2: Write the config

Create a file called `kafkamcp.yaml` in a location you'll remember (I use `~/.config/kafkamcp/kafkamcp.yaml`):

```yaml
server:
  transport: stdio
  log_level: info

clusters:
  - name: local
    bootstrap_servers: "localhost:9092"
    security_protocol: PLAINTEXT
    default: true

audit:
  enabled: true
  max_entries: 10000
```

Eight lines of meaningful config. That's the entire setup.

Validate it:

```bash
kafkamcp validate --config ~/.config/kafkamcp/kafkamcp.yaml
# Configuration is valid.
```

### Using Confluent Cloud?

Replace the cluster block:

```yaml
clusters:
  - name: confluent
    bootstrap_servers: "${CONFLUENT_BOOTSTRAP_SERVERS}"
    security_protocol: SASL_SSL
    sasl_mechanism: PLAIN
    sasl_username: "${CONFLUENT_API_KEY}"
    sasl_password: "${CONFLUENT_API_SECRET}"
    schema_registry:
      url: "${CONFLUENT_SR_URL}"
      auth:
        username: "${CONFLUENT_SR_API_KEY}"
        password: "${CONFLUENT_SR_API_SECRET}"
    default: true
```

KafkaMCP expands `${VAR}` and `${VAR:-default}` patterns from your environment. Set the variables, run `kafkamcp validate`, and you're done.

## Minute 3: Connect to Claude Desktop

Open your Claude Desktop configuration file:

- **macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

Add the KafkaMCP entry:

```json
{
  "mcpServers": {
    "kafkamcp": {
      "command": "kafkamcp",
      "args": ["--config", "/Users/you/.config/kafkamcp/kafkamcp.yaml"]
    }
  }
}
```

Use the full path to your config file. Restart Claude Desktop.

## Minute 4: Verify the connection

In Claude, you should see the MCP server indicator showing KafkaMCP is connected. If it doesn't appear, check Claude's MCP logs (Help → MCP Logs) for connection errors.

Now ask Claude:

> List all Kafka topics in my cluster

Claude calls `kafka_list_topics` and returns something like:

```json
{
  "topics": [
    {
      "name": "orders.created",
      "partitions": 6,
      "replication_factor": 1,
      "is_internal": false,
      "message_count_estimate": 14523
    },
    {
      "name": "payments.completed",
      "partitions": 3,
      "replication_factor": 1,
      "is_internal": false,
      "message_count_estimate": 8291
    }
  ],
  "total_count": 2,
  "cluster": "local"
}
```

If you see your topics, it's working.

## Minute 5: Do something useful

Now the fun part. Try these:

**Read recent messages:**
> Show me the last 5 messages from orders.created

Claude calls `kafka_consume` with `offset: "latest"` and `limit: 5`. You get deserialized messages with keys, values, headers, and timestamps.

**Inspect a consumer group:**
> Describe the consumer group order-processor and show me the lag per partition

Claude calls `kafka_describe_consumer_group`, showing you members, assignments, and per-partition lag.

**Produce a message:**
> Publish a test message to the test-events topic with key "test-1" and value {"event": "hello", "source": "claude"}

Claude calls `kafka_produce` and confirms the delivery with partition, offset, and timestamp.

**Search messages:**
> Search for messages in orders.created where the value contains "refund"

Claude calls `kafka_search` and returns matching messages from a bounded scan.

**Get cluster metadata:**
> What does my Kafka cluster look like? How many brokers, topics, partitions?

Claude reads the `kafka://cluster/metadata` resource and gives you a summary.

That's five minutes. Your AI agent now has native, authenticated, schema-aware access to your Kafka cluster — and you didn't write a single line of integration code.

## What you get

The full tool set available to your agent:

| Category | Tools |
|----------|-------|
| **Discovery** | `kafka_list_topics`, `kafka_describe_topic`, `kafka_list_schemas`, `kafka_get_schema`, `kafka_list_consumer_groups`, `kafka_describe_consumer_group` |
| **Data Access** | `kafka_consume`, `kafka_produce`, `kafka_subscribe`, `kafka_search` |
| **Operations** | `kafka_create_topic`, `kafka_alter_topic`, `kafka_delete_topic`, `kafka_reset_offsets` |
| **Analysis** | `kafka_analyze_dlq`, `kafka_aggregate`, `kafka_join`, `kafka_query` |
| **Schema** | `kafka_check_schema_compatibility`, `kafka_suggest_schema_evolution` |
| **Connect** | `kafka_list_connectors`, `kafka_describe_connector` |

22 tools. All available to Claude instantly.

## Adding access control

If you don't want Claude (or other agents) to have full access, add a policy:

```yaml
policies:
  default_deny: true
  agents:
    - id: "anonymous"
      topics:
        - pattern: "orders.*"
          permissions: [read]
        - pattern: "test-*"
          permissions: [read, write]
      consumer_groups:
        permissions: [describe]
      schemas:
        permissions: [read]
      rate_limit:
        requests_per_minute: 60
```

Now Claude can read from `orders.*` topics and read/write `test-*` topics, but nothing else. Rate-limited to 60 requests per minute. Every interaction is logged to the audit trail.

## Troubleshooting

**Claude doesn't show KafkaMCP in MCP servers:**
- Check that the `command` path in `claude_desktop_config.json` is correct. Use `which kafkamcp` to find the full path.
- Check Claude's MCP logs for error messages.

**"Configuration validation failed":**
- Run `kafkamcp validate --config your-config.yaml` to see the specific error.
- Check that environment variables referenced in `${VAR}` are set.

**"topic not found":**
- Verify the topic exists: `kafkamcp --config your-config.yaml` and ask to list topics first.
- Check that the Kafka cluster is reachable from your machine.

**Connection timeout:**
- Ensure `bootstrap_servers` is reachable. For Docker, use `localhost:9092` (not `kafka:9092` which is the container-internal hostname).

## Next steps

- [Access Control Guide](https://kafkamcp.dev/docs/guides/access-control) — restrict what agents can do
- [Multi-Cluster Setup](https://kafkamcp.dev/docs/guides/multi-cluster) — connect to multiple Kafka clusters
- [API Reference](https://kafkamcp.dev/docs/api-reference/tools) — all 22 tools documented
- [Docker Compose Guide](https://kafkamcp.dev/docs/guides/docker-compose) — full local environment with Kafka + Schema Registry + KafkaMCP

---

*KafkaMCP is open source under the Apache 2.0 license. [GitHub](https://github.com/kafkamcp/kafkamcp) · [Docs](https://kafkamcp.dev) · [Discord](https://discord.gg/kafkamcp)*
