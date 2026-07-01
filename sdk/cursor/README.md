# KafkaMCP Cursor Integration

Cursor has built-in MCP support, so KafkaMCP works with minimal configuration.

## Setup

Add KafkaMCP to your Cursor MCP configuration (`.cursor/mcp.json`):

### Option 1: stdio transport (recommended)

```json
{
  "mcpServers": {
    "kafkamcp": {
      "command": "kafkamcp",
      "args": ["--transport", "stdio"],
      "env": {
        "KAFKAMCP_CONFIG": "/path/to/kafkamcp.yaml"
      }
    }
  }
}
```

### Option 2: Connect to a running SSE server

```json
{
  "mcpServers": {
    "kafkamcp": {
      "url": "http://localhost:8080/sse"
    }
  }
}
```

### Option 3: Connect to a running streamable-http server

```json
{
  "mcpServers": {
    "kafkamcp": {
      "url": "http://localhost:8080/mcp"
    }
  }
}
```

## Usage

Once connected, Cursor can use the enabled KafkaMCP tools natively:

- "List all Kafka topics"
- "Show me failed orders in the last hour"
- "Diagnose lag for the payments consumer group"
- "Analyze error-topic as a DLQ"
- "Check whether this schema change is compatible"

## Available Tools

Cursor discovers the configured subset of the 29-tool supported surface:

- `core-read`: 15 read-only topic, message, schema, group, lag, DLQ, health,
  metrics, and discovery tools
- `ops-write`: guarded Kafka/topic/connector/ACL mutations
- `governance`: approvals, policy evaluation/simulation, and ACL reconciliation

See the generated [tool reference](../../docs/tool-reference.md) for exact
parameters and pack membership.
