# KafkaMCP Architecture Guide

This document describes the high-level architecture of KafkaMCP to help new contributors navigate the codebase and understand where new code belongs.

## Directory Structure

```
kafkamcp/
├── cmd/kafkamcp/          # Application entrypoint (main.go)
├── internal/              # Private application packages
│   ├── config/            # YAML config loading & validation
│   ├── kafka/             # Kafka client abstraction (KafkaManager)
│   ├── schema/            # Schema Registry client
│   ├── server/            # MCP server: tool registration, handlers, resources
│   └── <domain>/          # ~60 domain packages (see "Package Groupings" below)
├── sdk/                   # Client SDKs (Go, Python, TypeScript, VS Code, Cursor)
├── ide/                   # IDE plugin sources (IntelliJ)
├── deploy/                # Helm charts & Kubernetes manifests
├── examples/              # Docker Compose files & example configs
├── docs/                  # ADRs, internal docs
└── website/               # Docusaurus documentation site
```

## Core Concepts

### MCP Server Lifecycle

1. **`cmd/kafkamcp/main.go`** — Parses CLI flags, loads config, creates a `kafka.KafkaManager`, and calls `server.New()`.
2. **`internal/server/server.go`** — `New()` constructs the `Server` struct, initialises all subsystems (Analytics, Governance, Streaming, Platform), then calls `registerTools()` and `registerResources()`.
3. **Transport** — The server starts in one of three modes based on config: `stdio` (default for MCP clients), `sse`, or `streamable-http`.
4. **Shutdown** — Graceful shutdown via OS signal (SIGINT/SIGTERM) tears down connections.

### Tool Registration Pattern

Every MCP tool follows the same three-file pattern:

| File | Purpose |
|------|---------|
| `tools_<domain>.go` | Handler methods on `*Server` (e.g., `handleListTopics`) |
| `register_<domain>.go` | Registration function that calls `s.mcpServer.AddTool(...)` |
| `internal/<domain>/` | Domain logic package (pure business logic, no MCP dependency) |

**Registration flow:**

```
server.New()
  └─> registerTools()            # register_tools.go — orchestrator
        ├─> registerP0Tools()    # register_p0.go — core Kafka ops
        ├─> registerP1Tools()    # register_p1.go — consumer groups, ACLs
        ├─> registerP2Tools()    # register_p2.go — connect, migrations
        ├─> registerP3Tools()    # register_p3.go — advanced analytics
        └─> register<Domain>Tools()  # one per feature domain
```

Each `register_<domain>.go` file calls `s.mcpServer.AddTool()` with:
- A `mcp.NewTool(name, ...params)` definition
- A handler method reference (e.g., `s.handleListTopics`)

### Handler Lifecycle

When an MCP client invokes a tool:

1. **MCP framework** deserialises the request and calls the registered handler.
2. **Handler** (`tools_<domain>.go`) extracts arguments, resolves the cluster, and checks policies:
   - `s.resolveCluster(args)` — determines which Kafka cluster to use
   - `s.policyEngine.Check(...)` — enforces access control
   - `s.auditLogger.Log(...)` — records the operation
   - `s.rateLimiter.Allow(...)` — enforces rate limits
3. **Domain logic** — handler delegates to the domain package (e.g., `s.kafkaManager.ListTopics()`).
4. **Response** — handler returns `mcp.NewToolResultText(json)` or an error.

### Resource Registration

Resources follow a similar pattern in `resources.go` / `resources_features.go`:
- `s.mcpServer.AddResource(...)` registers static and templated resources.
- Resources provide read-only discovery of topics, schemas, and cluster metadata.

#### Static vs Templated Resources

- **Static resources** have a fixed URI (e.g., `kafka://cluster/topics`) and return a snapshot of cluster state when read. They are registered once at startup and always available.
- **Templated resources** use URI templates with placeholders (e.g., `kafka://cluster/topics/{topicName}`). The MCP client fills in the template parameters at read time, allowing dynamic lookup of specific topics, schemas, or consumer groups without registering a resource per entity.

### Shutdown Order

When the server receives SIGINT/SIGTERM, `Close()` tears down resources in reverse initialization order:

1. **Cancel context** — signals all in-flight handlers and background goroutines to stop.
2. **HTTP server shutdown** — gracefully drains active HTTP connections (10-second timeout).
3. **Metrics server shutdown** — stops the Prometheus endpoint.
4. **Server.Close()** — closes subsystems (Analytics, Governance, Streaming, Platform) and their internal resources.
5. **KafkaManager.Close()** — closes all Kafka client connections and consumer groups.

This ordering ensures that no new requests are accepted while in-flight operations complete, and that Kafka connections are the last resources released.

## Package Groupings

The `Server` struct embeds four composite subsystem groups:

### Analytics
AI-powered analysis: anomaly detection, NL query, cost analysis, forecasting, knowledge graph, lag diagnostics, schema evolution, time-series, advisor.

### Governance
Policy, audit, security: policy engine, audit logger, rate limiter, PII masking, data contracts, personas, multi-tenancy, compliance.

### Streaming
Event-driven subsystems: watch subscriptions, DLQ remediation, replication monitoring, self-healing, lineage tracking, message tracing, flow debugging.

### Platform
Infrastructure & extensibility: plugin registry, agent registry, OpenTelemetry, GitOps, migration planner, workflows, gateway, collaboration, semantic search, onboarding, OPA, K8s operator, pipelines, marketplace, observability, chaos engineering, replay, federation, profiler, DAG workflows, view store, agent memory, task queue, streaming SQL, catalog, adaptive rate limiting, config VCS, cost attribution, sandbox.

## Adding a New Tool

1. **Create the domain package** at `internal/<name>/` with your business logic.
2. **Create `tools_<name>.go`** in `internal/server/` with handler methods on `*Server`.
3. **Create `register_<name>.go`** in `internal/server/` with a `register<Name>Tools()` method.
4. **Wire it in** by adding `s.register<Name>Tools()` to `registerTools()` in `register_tools.go`.
5. **Add the subsystem field** to the appropriate group struct in `server.go` (Analytics, Governance, Streaming, or Platform) and initialise it in `New()`.

## Configuration

Configuration is loaded from YAML (`internal/config/`) with environment variable interpolation. The default config file is `kafkamcp.yaml`. Key sections:

- `server` — transport mode, host, port, log level
- `clusters` — Kafka cluster definitions (bootstrap servers, auth, Schema Registry)
- `policies` — per-agent access control rules
- `audit` — audit logging settings
- `masking` — PII redaction rules
- `telemetry` — OpenTelemetry tracing

Environment variables `KAFKAMCP_TRANSPORT` and `KAFKAMCP_LOG_LEVEL` override config file values at runtime.

## Testing

- **Unit tests** live alongside source files (`*_test.go`).
- **Mock tests** use `*_mock_test.go` for isolated handler testing.
- **Integration tests** are tagged with `//go:build integration` and require a running Kafka cluster.
- Run `make test` for unit tests, `make integration-test` for integration tests.
