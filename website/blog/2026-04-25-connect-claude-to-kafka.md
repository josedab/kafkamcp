---
slug: connect-claude-to-kafka
title: "Connect Claude to Your Kafka Cluster in 5 Minutes"
authors:
  - name: Jose D.
tags: [kafka, claude, mcp, tutorial, getting-started]
date: 2025-07-15
---

I'm going to show you how to go from "Claude has no idea my Kafka cluster exists" to "Claude lists topics, consumes messages, and inspects consumer groups" in under five minutes. No Python scripts. No REST wrappers. No Docker Compose stack. One binary, one YAML file.

Here's what we're building:

```
Claude Desktop ←→ MCP (stdio) ←→ KafkaMCP ←→ Your Kafka Cluster
```

{/* truncate */}

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
go install github.com/josedab/kafkamcp/cmd/kafkamcp@latest
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

Claude calls `kafka_list_topics` and returns your topics with partition counts, replication factors, and estimated message counts.

## Minute 5: Do something useful

**Read recent messages:**
> Show me the last 5 messages from orders.created

**Inspect a consumer group:**
> Describe the consumer group order-processor and show me the lag per partition

**Produce a message:**
> Publish a test message to the test-events topic with key "test-1" and value `{"event": "hello", "source": "claude"}`

**Search messages:**
> Search for messages in orders.created where the value contains "refund"

That's five minutes. Your AI agent now has native, authenticated, schema-aware access to your Kafka cluster — and you didn't write a single line of integration code.

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

Now Claude can read from `orders.*` topics and read/write `test-*` topics, but nothing else. Rate-limited to 60 requests per minute.

## Next steps

- [Access Control Guide](https://josedab.github.io/kafkamcp/docs/guides/access-control) — restrict what agents can do
- [Multi-Cluster Setup](https://josedab.github.io/kafkamcp/docs/guides/multi-cluster) — connect to multiple Kafka clusters
- [API Reference](https://josedab.github.io/kafkamcp/docs/api-reference/tools) — full tool catalog

---

*KafkaMCP is open source under the Apache 2.0 license. [GitHub](https://github.com/josedab/kafkamcp) · [Docs](https://josedab.github.io/kafkamcp/) · [Discussions](https://github.com/josedab/kafkamcp/discussions)*
