---
sidebar_position: 2
title: Getting Started
---

# Getting Started

Get KafkaMCP running and connected to your Kafka cluster in under 5 minutes.

## Prerequisites

- A running Kafka cluster (local or remote)
- **One** of the following (choose your preferred install method):
  - Go 1.25+ (for source install)
  - Docker
  - Pre-built binary from [GitHub Releases](https://github.com/kafkamcp/kafkamcp/releases)

## Step 1: Install

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
<TabItem value="go" label="Go Install" default>

```bash
go install github.com/kafkamcp/kafkamcp/cmd/kafkamcp@latest
```

</TabItem>
<TabItem value="binary" label="Binary">

```bash
# Download the latest release for your platform
# Browse all releases: https://github.com/kafkamcp/kafkamcp/releases
curl -sSL https://github.com/kafkamcp/kafkamcp/releases/latest/download/kafkamcp_$(uname -s)_$(uname -m).tar.gz | tar xz
sudo mv kafkamcp /usr/local/bin/
```

</TabItem>
<TabItem value="docker" label="Docker">

```bash
docker pull ghcr.io/kafkamcp/kafkamcp:latest
```

</TabItem>
<TabItem value="source" label="From Source">

```bash
git clone https://github.com/kafkamcp/kafkamcp.git
cd kafkamcp
go build -o bin/kafkamcp ./cmd/kafkamcp
```

</TabItem>
</Tabs>

## Step 2: Configure

Create a `kafkamcp.yaml` file:

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

:::tip
For Confluent Cloud, AWS MSK, or other managed Kafka services, see the [configuration reference](./configuration) for SASL/TLS settings.
:::

## Step 3: Validate

Verify your configuration is correct:

```bash
kafkamcp validate --config kafkamcp.yaml
```

You should see:

```
Configuration is valid.
```

## Step 4: Connect to an AI Agent

### Claude Desktop

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "kafkamcp": {
      "command": "kafkamcp",
      "args": ["--config", "/path/to/kafkamcp.yaml"]
    }
  }
}
```

Restart Claude Desktop. You should see "KafkaMCP" in the available MCP servers.

### Other MCP Clients

For any MCP-compatible client, start KafkaMCP in stdio mode:

```bash
kafkamcp --config kafkamcp.yaml
```

For remote agents, use SSE transport:

```bash
# In kafkamcp.yaml, set:
# server:
#   transport: sse
#   host: 0.0.0.0
#   port: 3100

kafkamcp --config kafkamcp.yaml
# Agents connect to http://localhost:3100
```

## Step 5: Try It

Once connected, ask your agent:

> "List all Kafka topics in my cluster"

The agent will call `kafka_list_topics` and return a list of all topics with their partition counts, replication factors, and message count estimates.

Try more:

> "Show me the last 5 messages from my-topic"

> "Describe the consumer group my-consumer-group"

> "What schemas are registered in the Schema Registry?"

## What's Next?

- [**Core Concepts**](./core-concepts) — Understand tools, resources, and the auth model
- [**Claude Desktop Guide**](./guides/claude-desktop) — Detailed Claude Desktop setup
- [**Docker Compose Guide**](./guides/docker-compose) — Full local environment with Kafka + KafkaMCP
- [**API Reference**](./api-reference/tools) — All 22 tools with examples
