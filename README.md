# KafkaMCP

> **Open Source MCP Server That Makes Kafka Topics Fully Readable, Writable, and Discoverable by AI Agents**

[![Go](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)](https://go.dev)
[![CI](https://github.com/kafkamcp/kafkamcp/actions/workflows/ci.yml/badge.svg)](https://github.com/kafkamcp/kafkamcp/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

KafkaMCP is an MCP (Model Context Protocol) server that transforms Apache Kafka into a first-class, agent-accessible data layer. It exposes Kafka topics, consumer groups, schemas, and cluster metadata as MCP tools and resources — enabling any MCP-compatible AI agent to discover, read from, write to, and reason about Kafka data with zero custom integration code.

## Features

- **🔍 Topic Discovery** — List and describe topics, partitions, offsets, and configurations
- **📨 Consume Messages** — Read messages with flexible positioning (earliest, latest, offset, timestamp)
- **📤 Produce Messages** — Publish messages with delivery confirmation
- **📋 Schema Registry** — List and retrieve Avro/Protobuf/JSON schemas with human-readable field summaries
- **👥 Consumer Groups** — Inspect group state, members, partition assignments, and per-partition lag
- **🏗️ Cluster Metadata** — Browse brokers, controller, and cluster topology
- **🔒 Access Control** — YAML-based per-agent policies with default-deny semantics
- **📝 Audit Logging** — Structured JSON audit trail of all agent interactions
- **🔌 Dual Transport** — stdio (local/Claude Desktop) and SSE/HTTP (remote/multi-agent)
- **🏢 Multi-Cluster** — Connect to multiple Kafka clusters with per-cluster configuration

## Quick Start

### 1. Install

**From source:**
```bash
go install github.com/kafkamcp/kafkamcp/cmd/kafkamcp@latest
```

**From binary:**
```bash
# Download from GitHub Releases
curl -sSL https://github.com/kafkamcp/kafkamcp/releases/latest/download/kafkamcp_$(uname -s)_$(uname -m).tar.gz | tar xz
sudo mv kafkamcp /usr/local/bin/
```

**Docker:**
```bash
docker run --rm -v $(pwd)/kafkamcp.yaml:/etc/kafkamcp/kafkamcp.yaml \
  ghcr.io/kafkamcp/kafkamcp:latest --config /etc/kafkamcp/kafkamcp.yaml
```

### 2. Configure

Create `kafkamcp.yaml`:

```yaml
server:
  transport: stdio        # stdio | sse | streamable-http
  log_level: info

clusters:
  - name: local
    bootstrap_servers: "localhost:9092"
    security_protocol: PLAINTEXT
    default: true

# Optional: Schema Registry
#   schema_registry:
#     url: "http://localhost:8081"

# Optional: Access Control
# policies:
#   default_deny: true
#   agents:
#     - id: "my-agent"
#       topics:
#         - pattern: "*"
#           permissions: [read, write]
#       consumer_groups:
#         permissions: [describe]
#       schemas:
#         permissions: [read]

audit:
  enabled: true
  max_entries: 10000
```

### 3. Connect to Claude Desktop

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

### 4. Use

Once connected, your AI agent can:

```
"List all Kafka topics in my cluster"
"Show me the last 10 messages from the orders.created topic"
"Describe the consumer group order-processor and show me the lag"
"What schema does the payments.completed topic use?"
"Publish a test message to the test-events topic"
```

## MCP Tools

### P0 — Core Tools
| Tool | Description |
|------|-------------|
| `kafka_list_topics` | List all topics with metadata |
| `kafka_describe_topic` | Get detailed topic metadata |
| `kafka_consume` | Read messages (earliest/latest/offset/timestamp) |
| `kafka_produce` | Publish messages with delivery confirmation |
| `kafka_list_schemas` | List Schema Registry subjects |
| `kafka_get_schema` | Get full schema definition |
| `kafka_describe_consumer_group` | Get consumer group state and lag |
| `kafka_list_consumer_groups` | List all consumer groups |

### P1 — Advanced Tools
| Tool | Description |
|------|-------------|
| `kafka_subscribe` | Subscribe with consumer group semantics (batch polling) |
| `kafka_create_topic` | Create a topic with partitions, replication, configs |
| `kafka_alter_topic` | Modify topic configuration |
| `kafka_delete_topic` | Delete a topic |
| `kafka_search` | Search messages by key, header, value, time range |
| `kafka_reset_offsets` | Reset consumer group offsets (with dry-run) |
| `kafka_check_schema_compatibility` | Validate schema against compatibility policy |
| `kafka_analyze_dlq` | Analyze dead-letter queue: categorize errors, samples |

### P2 — Stream Processing & Intelligence
| Tool | Description |
|------|-------------|
| `kafka_aggregate` | Windowed aggregation (count/sum/avg/min/max) on a field |
| `kafka_join` | Time-windowed join across two topics on a key field |
| `kafka_query` | Natural language query translated to topic + filter |
| `kafka_suggest_schema_evolution` | Schema change advisor with compatible strategies |
| `kafka_list_connectors` | List Kafka Connect connectors with status |
| `kafka_describe_connector` | Full connector config, tasks, and errors |

> **Note:** Schema tools (`kafka_list_schemas`, `kafka_get_schema`, `kafka_check_schema_compatibility`) require `schema_registry.url` in cluster config.
> Kafka Connect tools require `connect_url` in cluster config.

## MCP Resources

| Resource URI | Description |
|-------------|-------------|
| `kafka://cluster/metadata` | Cluster brokers, controller, topic/partition counts |
| `kafka://clusters` | All configured clusters for multi-cluster discovery |
| `kafka://topics` | Topic catalog with partition counts |
| `kafka://topics/{name}` | Individual topic detail |
| `kafka://schemas` | Schema Registry subject catalog |
| `kafka://schemas/{subject}` | Individual schema definition |
| `kafka://groups` | Consumer group catalog with state and lag |
| `kafka://groups/{id}` | Individual consumer group detail |
| `kafka://audit/log` | Agent audit trail (recent 1000 entries) |
| `kafka://topic_graph` | Producer → topic → consumer group topology graph |

## Security & Access Control

- **Per-agent authorization** — YAML policy engine with default-deny, topic glob patterns
- **Rate limiting** — Token bucket per agent, configurable via policy
- **Agent ID extraction** — From `X-Agent-ID` or `X-Client-ID` headers
- **TLS support** — HTTPS for SSE/HTTP transports via `tls_cert`/`tls_key` config
- **Audit logging** — Structured JSON audit trail (file + ring buffer)
- **Auth-filtered list results** — Agents only see resources they're authorized to access

## Observability

- **Prometheus metrics** at `/metrics` (port+1) — tool call counts, latencies, message throughput, auth denials, rate limit denials
- **Health check** at `/healthz`
- **Structured JSON logging** via `slog`

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                     KafkaMCP                              │
│                                                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │
│  │ AI Agent A  │  │ AI Agent B  │  │ AI Agent C  │      │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘      │
│         │ stdio          │ SSE            │ SSE          │
│         ▼                ▼                ▼              │
│  ┌──────────────────────────────────────────────┐        │
│  │              MCP Server Core                  │        │
│  │  Tools │ Resources │ AuthZ │ Audit │ Rate Lim │        │
│  └──────────────────────┬───────────────────────┘        │
│                         │                                │
│  ┌──────────────────────▼───────────────────────┐        │
│  │         Kafka Client (franz-go)               │        │
│  │  Admin │ Consumer │ Producer │ Schema Client  │        │
│  └──────────────────────┬───────────────────────┘        │
│                         │                                │
│         ┌───────────────┼───────────────┐                │
│         ▼               ▼               ▼                │
│  ┌──────────┐   ┌──────────────┐  ┌──────────┐          │
│  │  Kafka   │   │   Schema     │  │  Kafka   │          │
│  │  Brokers │   │   Registry   │  │  Admin   │          │
│  └──────────┘   └──────────────┘  └──────────┘          │
└──────────────────────────────────────────────────────────┘
```

## Configuration Reference

### Server
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `transport` | string | `stdio` | MCP transport: `stdio`, `sse`, `streamable-http` |
| `host` | string | `0.0.0.0` | Bind host for SSE/HTTP |
| `port` | int | `3100` | Bind port for SSE/HTTP |
| `log_level` | string | `info` | Log level: `debug`, `info`, `warn`, `error` |
| `tls_cert` | string | — | Path to TLS certificate for HTTPS |
| `tls_key` | string | — | Path to TLS private key for HTTPS |

### Clusters
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Cluster name |
| `bootstrap_servers` | string | Yes | Kafka broker addresses |
| `security_protocol` | string | No | `PLAINTEXT`, `SASL_PLAINTEXT`, `SASL_SSL`, `SSL` |
| `sasl_mechanism` | string | No | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `sasl_username` | string | No | SASL username (supports `${ENV_VAR}`) |
| `sasl_password` | string | No | SASL password (supports `${ENV_VAR}`) |
| `schema_registry.url` | string | No | Schema Registry URL |
| `default` | bool | No | Mark as default cluster |

### Policies
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `default_deny` | bool | `false` | Deny all access by default |
| `agents[].id` | string | — | Agent identifier |
| `agents[].topics[].pattern` | string | — | Glob pattern (e.g., `orders.*`) |
| `agents[].topics[].permissions` | []string | — | `read`, `write`, `create`, `delete` |

### Environment Variables

Config values support `${VAR}` and `${VAR:-default}` syntax:

```yaml
sasl_username: "${KAFKA_USERNAME}"
sasl_password: "${KAFKA_PASSWORD:-changeme}"
```

## Development

### Prerequisites

- Go 1.22+
- Docker (for integration tests)
- A Kafka cluster (local or remote)

### Build

```bash
go build -o bin/kafkamcp ./cmd/kafkamcp
```

### Test

```bash
go test ./...
```

### Validate Config

```bash
./bin/kafkamcp validate --config kafkamcp.yaml
```

## Project Structure

```
kafkamcp/
├── cmd/kafkamcp/          # CLI entrypoint
│   └── main.go
├── internal/
│   ├── config/            # YAML config loader with env var expansion
│   ├── kafka/             # Kafka client manager (admin, consume, produce)
│   ├── schema/            # Schema Registry client with LRU cache
│   ├── auth/              # Policy engine (per-agent access control)
│   ├── audit/             # Structured JSON audit logger
│   └── server/            # MCP server, tool/resource handlers
├── examples/              # Example configurations
├── .github/workflows/     # CI/CD
├── Dockerfile
├── kafkamcp.yaml          # Example configuration
├── go.mod
└── go.sum
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Roadmap

- [x] **v0.1**: Core P0 tools (list, describe, consume, produce, schemas, groups)
- [x] **v0.2**: Auth enforcement, rate limiting, Prometheus metrics
- [x] **v0.3**: P1 tools (subscribe, topic CRUD, search, offset reset, DLQ analysis, schema compat)
- [x] **v0.4**: P2 tools (aggregate, join, NL query, schema evolution, Kafka Connect, topic graph)
- [x] **v0.4**: All MCP resources (10 static + 3 templates)
- [ ] **v1.0**: Cloud managed service, enterprise features (SSO, data masking)

See the [PRD](PRD.md) for the complete roadmap.
