---
sidebar_position: 13
title: FAQ
---

# Frequently Asked Questions

## General

### What is MCP?

MCP (Model Context Protocol) is an open standard that lets AI agents discover and call tools exposed by external servers. KafkaMCP implements the MCP server side, exposing Kafka operations as tools that any MCP-compatible agent can use. Learn more at [modelcontextprotocol.io](https://modelcontextprotocol.io).

### What Kafka versions does KafkaMCP support?

KafkaMCP uses [franz-go](https://github.com/twmb/franz-go) and negotiates the
Kafka protocol with the broker. The repository includes a reusable compatibility
harness for Apache Kafka and Redpanda, plus configuration guidance (not
certification) for Amazon MSK and Confluent Cloud. See the
[compatibility contract](https://github.com/josedab/kafkamcp/blob/main/docs/COMPATIBILITY.md)
for the current verified matrix.

### Does KafkaMCP require a JVM?

No. KafkaMCP is written in pure Go and compiles to a single static binary. No JVM, no native dependencies, no runtime to install.

### How many tools does KafkaMCP expose?

KafkaMCP exposes **29 supported tools** across explicit packs and **nine stable
read-only resources**. The default `core-read` pack contains only 15 tools.
Archived feature experiments outside the supported packs are not registered or
callable.

### Is KafkaMCP production-ready?

KafkaMCP is currently pre-release. The repository includes production-oriented
security controls, but no GA/LTS claim is made until the objective and external
gates in the [roadmap](https://github.com/josedab/kafkamcp/blob/main/docs/ROADMAP.md)
are met.

## Installation

### Can I run KafkaMCP without Go installed?

Not before the first tagged release. Today, build/install with Go. Signed
pre-built binaries and GHCR images will be published by the release workflow.

### How do I update KafkaMCP?

```bash
# Go install (always fetches latest)
go install github.com/josedab/kafkamcp/cmd/kafkamcp@latest

# Docker (after the first tagged release)
docker pull ghcr.io/josedab/kafkamcp:VERSION

# Binary (after the first tagged release)
curl -sSL https://github.com/josedab/kafkamcp/releases/latest/download/kafkamcp_$(uname -s)_$(uname -m).tar.gz | tar xz
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
- **Cursor** and other MCP clients via `sdk/cursor/mcp.json`

### Can I use KafkaMCP from a non-MCP application?

KafkaMCP exposes MCP, not a parallel REST/OpenAPI facade. Use an MCP transport
directly or one of the SDK wrappers with a real MCP `Transport`
implementation.

## Operations

### Where are Prometheus metrics exposed?

Metrics are served at `/metrics` on `server.metrics_port` (default:
`server.port + 1`, normally `3101`) and bind to `server.metrics_host` (default
`127.0.0.1`). Containers and pods must set `metrics_host: 0.0.0.0` and protect
the port with firewall/NetworkPolicy rules. `/livez` is process liveness,
`/healthz` checks Kafka, and `/ready` checks Kafka plus persistence.

### How do I monitor KafkaMCP in Grafana?

Build a dashboard from the PromQL examples in the
[Monitoring Guide](./guides/monitoring). KafkaMCP exposes standard Prometheus
metrics but does not bundle a versioned Grafana dashboard.

### How do I debug connection issues?

1. Run `kafkamcp validate --config kafkamcp.yaml`
2. Start with `--log-level debug` for verbose output
3. Check broker connectivity from the KafkaMCP host
4. Verify `security_protocol` and SASL settings match your cluster

See [Troubleshooting](./troubleshooting) for common issues and fixes.

## Still have questions?

- [GitHub Discussions](https://github.com/josedab/kafkamcp/discussions) — ask the community
- [Troubleshooting](./troubleshooting) — common issues and fixes
