---
sidebar_position: 13
title: FAQ
---

# Frequently Asked Questions

## General

### What is MCP?

MCP (Model Context Protocol) is an open standard that lets AI agents discover and call tools exposed by external servers. KafkaMCP implements the MCP server side, exposing Kafka operations as tools that any MCP-compatible agent can use. Learn more at [modelcontextprotocol.io](https://modelcontextprotocol.io).

### What Kafka versions does KafkaMCP support?

KafkaMCP uses the [franz-go](https://github.com/twmb/franz-go) client library, which supports the Kafka protocol from 0.8+ through the latest versions. It works with:

- Apache Kafka 2.x, 3.x
- Confluent Platform
- Amazon MSK
- Azure Event Hubs (Kafka protocol)
- Redpanda
- Aiven for Apache Kafka

### Does KafkaMCP require a JVM?

No. KafkaMCP is written in pure Go and compiles to a single static binary. No JVM, no native dependencies, no runtime to install.

### How many tools does KafkaMCP expose?

KafkaMCP exposes **251 MCP tools** and **18 MCP resources** as of v0.1.0. These range from basic consume/produce to advanced features like windowed aggregations, natural language queries, anomaly detection, and schema evolution workflows.

### Is KafkaMCP production-ready?

KafkaMCP v0.1.0 is suitable for production use with appropriate security configuration (TLS, access control, rate limiting, audit logging). See the [Security](./security) page for the production checklist.

## Installation

### Can I run KafkaMCP without Go installed?

Yes. You can use:

- **Pre-built binaries** from [GitHub Releases](https://github.com/kafkamcp/kafkamcp/releases) (Linux, macOS, Windows on amd64/arm64)
- **Docker** — `docker run ghcr.io/kafkamcp/kafkamcp:latest`

### How do I update KafkaMCP?

```bash
# Go install (always fetches latest)
go install github.com/kafkamcp/kafkamcp/cmd/kafkamcp@latest

# Docker
docker pull ghcr.io/kafkamcp/kafkamcp:latest

# Binary — download the latest release
curl -sSL https://github.com/kafkamcp/kafkamcp/releases/latest/download/kafkamcp_$(uname -s)_$(uname -m).tar.gz | tar xz
```

## Configuration

### How do I keep credentials out of config files?

Use environment variable expansion:

```yaml
clusters:
  - name: production
    sasl_username: "${KAFKA_USERNAME}"
    sasl_password: "${KAFKA_PASSWORD}"
```

KafkaMCP expands `${VAR}` and `${VAR:-default}` before parsing the YAML. See [Configuration](./configuration#environment-variable-expansion) for details.

### Can I connect to multiple Kafka clusters?

Yes. Define multiple entries under `clusters:` in your config. Each tool accepts an optional `cluster` parameter. See the [Multi-Cluster Guide](./guides/multi-cluster).

### Which transport should I use?

| Transport | When to use |
|-----------|-------------|
| `stdio` | Local agents — Claude Desktop, Cursor, VS Code |
| `sse` | Remote agents, shared server, multi-agent setups |
| `streamable-http` | Modern HTTP streaming, load-balanced deployments |

### How do I validate my config without starting the server?

```bash
kafkamcp validate --config kafkamcp.yaml
```

This checks for missing clusters, duplicate names, invalid transports, and incomplete SASL settings.

## Security

### What happens if I don't configure access control?

Without `policies.default_deny: true`, all agents have unrestricted access to all Kafka operations. This is fine for local development but not recommended for production.

### How does agent identity work with Claude Desktop?

Claude Desktop uses `stdio` transport, which does not send HTTP headers. Agents are identified as `anonymous`. If you need per-agent policies with Claude Desktop, configure a custom MCP client that sends `X-Agent-ID` headers.

### Can I use KafkaMCP with SSO or OAuth?

KafkaMCP does not have built-in SSO/OAuth support for agent authentication. Agent identity is resolved from HTTP headers (`X-Agent-ID`). For SSO integration, place an authentication proxy (e.g., OAuth2 Proxy) in front of KafkaMCP's HTTP endpoint.

## Agents & SDKs

### Which AI agents work with KafkaMCP?

Any MCP-compatible agent, including:

- **Claude Desktop** (Anthropic)
- **LangChain** agents (via the Python SDK)
- **CrewAI** agents (via the Python SDK)
- **AutoGen** agents
- **Custom agents** using the Go, TypeScript, or Python SDKs
- **VS Code** and **Cursor** via MCP extensions
- **IntelliJ/JetBrains** via the plugin

### Can I use KafkaMCP from a non-MCP application?

Yes. Use the `openapi-spec` tool (`kafka_openapi_spec`) to generate an OpenAPI 3.0 specification, then call KafkaMCP over HTTP like any REST API. You can also use the SDKs directly.

## Operations

### Where are Prometheus metrics exposed?

Metrics are served at `/metrics` on `server.port + 1` (default: `3101`), bound to `127.0.0.1`. Metrics are only available when using HTTP transports (`sse` or `streamable-http`).

### How do I monitor KafkaMCP in Grafana?

Import the ready-made dashboard from `deploy/grafana/kafkamcp-overview.json`. It includes 23 panels covering tool call rates, latency percentiles, message throughput, schema cache performance, auth/rate-limit denials, and active connections. See the [Monitoring Guide](./guides/monitoring).

### How do I debug connection issues?

1. Run `kafkamcp validate --config kafkamcp.yaml`
2. Start with `--log-level debug` for verbose output
3. Check broker connectivity from the KafkaMCP host
4. Verify `security_protocol` and SASL settings match your cluster

See [Troubleshooting](./troubleshooting) for common issues and fixes.

## Still have questions?

- [GitHub Discussions](https://github.com/kafkamcp/kafkamcp/discussions) — ask the community
- [Discord](https://discord.gg/kafkamcp) — real-time chat
- [Troubleshooting](./troubleshooting) — common issues and fixes
