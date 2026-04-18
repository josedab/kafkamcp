# KafkaMCP Cursor Integration

Cursor has built-in MCP support, so KafkaMCP works with minimal configuration.

## Setup

Add KafkaMCP to your Cursor MCP configuration (`.cursor/mcp.json`):

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

Or connect to a running server:

```json
{
  "mcpServers": {
    "kafkamcp": {
      "url": "http://localhost:8080/sse"
    }
  }
}
```

## Usage

Once connected, Cursor can use all KafkaMCP tools natively:

- "List all Kafka topics"
- "Show me failed orders in the last hour"
- "Analyze the cost of the user-events topic"
- "Run the DLQ investigation workflow on error-topic"
- "What depends on the orders topic?"

## Available Tools

All 30+ KafkaMCP tools are available including:
- Topic management (list, describe, create, alter, delete)
- Message operations (consume, produce, search, aggregate)
- Schema Registry (list, get, diff, compatibility check)
- Anomaly detection (`kafka_anomalies`)
- Natural language queries (`kafka_nl_query`)
- Cost analysis (`kafka_cost_analyze`, `kafka_cost_waste`, `kafka_cost_whatif`)
- Workflow templates (`kafka_workflows`)
- Knowledge graph (`kafka_knowledge_graph`)
- GitOps (`kafka_gitops_diff`)
- Platform info (`kafka_platform_info`)
