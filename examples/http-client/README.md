# HTTP Client Examples for KafkaMCP

These examples show how to interact with KafkaMCP using the **SSE** and **Streamable-HTTP** transports via raw HTTP calls. Useful when you don't have an MCP SDK available or want to script interactions.

## Prerequisites

Start KafkaMCP with an HTTP-based transport:

```bash
# SSE transport (default port 3100)
kafkamcp --config kafkamcp.yaml --transport sse

# Or streamable-http transport
kafkamcp --config kafkamcp.yaml --transport streamable-http
```

## curl Examples

### Initialize an MCP session

```bash
curl -s http://localhost:3100/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
      "protocolVersion": "2025-03-26",
      "capabilities": {},
      "clientInfo": { "name": "curl-example", "version": "1.0.0" }
    }
  }'
```

### List available tools

```bash
curl -s http://localhost:3100/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 2,
    "method": "tools/list",
    "params": {}
  }'
```

### List Kafka topics

```bash
curl -s http://localhost:3100/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 3,
    "method": "tools/call",
    "params": {
      "name": "kafka_list_topics",
      "arguments": {}
    }
  }'
```

### Consume messages from a topic

```bash
curl -s http://localhost:3100/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 4,
    "method": "tools/call",
    "params": {
      "name": "kafka_consume",
      "arguments": {
        "topic": "my-topic",
        "count": 5,
        "position": "earliest"
      }
    }
  }'
```

### Produce a message

```bash
curl -s http://localhost:3100/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 5,
    "method": "tools/call",
    "params": {
      "name": "kafka_produce",
      "arguments": {
        "topic": "my-topic",
        "value": "{\"hello\": \"world\"}"
      }
    }
  }'
```

## Python Example

See [mcp_http_client.py](mcp_http_client.py) for a complete Python script that demonstrates session initialization, tool listing, and tool calling over HTTP.
