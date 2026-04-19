# Contributing to KafkaMCP

Thank you for your interest in contributing to KafkaMCP! This guide will help you get started.

## Prerequisites

- **Go 1.25+** — [install instructions](https://go.dev/doc/install)
- **Docker** (optional) — for running Kafka locally
- **golangci-lint** — `go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest`

## Development Workflow

### 1. Fork & Clone

```bash
git clone https://github.com/<your-username>/kafkamcp.git
cd kafkamcp
```

### 2. Build

```bash
make build
```

### 3. Setup VSCode (optional)

```bash
make setup-vscode
```

### 4. Run Tests

```bash
make test
```

### 4. Lint

```bash
make lint
```

### 5. Format Code

```bash
make fmt
```

### 6. Run All Checks

```bash
make validate
```

## Local Kafka Environment

Start a local Kafka cluster for integration testing:

```bash
docker compose -f examples/docker-compose.yaml up -d
```

Then run KafkaMCP against it:

```bash
./bin/kafkamcp --config kafkamcp.yaml
```

## Testing Expectations

- All new code should include unit tests.
- Run `make test` before submitting a PR — tests must pass with `-race` enabled.
- Aim for meaningful coverage on new functionality; don't test trivial getters/setters.

## Code Style

- Follow standard Go conventions (`gofmt`, `goimports`).
- Use tabs for indentation (enforced by `gofmt`).
- Keep exported types and functions documented with GoDoc comments.
- Avoid unnecessary comments — let clear naming speak for itself.
- Error messages should be lowercase and not end with punctuation.
- Wrap errors with context: `fmt.Errorf("doing thing: %w", err)`.

## Commit Conventions

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add consumer group lag monitoring
fix: handle nil partition in offset reset
docs: update README with SSE transport example
refactor: split server.go into per-tier handler files
test: add policy engine edge case tests
chore: update golangci-lint to v1.57
```

Keep commits focused — one logical change per commit.

## Pull Request Process

1. Create a feature branch from `main`:
   ```bash
   git checkout -b feat/my-feature
   ```
2. Make your changes and ensure `make validate` passes.
3. Push and open a PR against `main`.
4. Fill in the PR template with a description of changes.
5. Respond to review feedback promptly.

## Project Structure

```
cmd/kafkamcp/       — CLI entrypoint
internal/
  server/           — MCP server, tool handlers, resource handlers
    tools_p0.go     — P0 handlers: list topics, describe, consume, produce, schemas, groups
    tools_p1.go     — P1 handlers: create/alter/delete topic, search, offset reset, DLQ, subscribe
    tools_p2.go     — P2 handlers: aggregate, schema evolution, connectors, join, NL query
    tools_p3.go     — P3 handlers: watch, DLQ remediation, schema diff, streaming, agent registry
    tools_advanced.go    — Advanced handlers: connector CRUD, time travel, recommend, trace, ACLs, config drift, playground
    tools_features.go    — Feature handlers: anomalies, NL query, cost analysis, workflows, GitOps, knowledge graph, data contracts
    tools_streaming.go   — Streaming consumer session management
    tools_new_features.go — New feature handlers: lag diagnosis, migration, lineage, group health, personas, forecasting
    register_tools.go    — Tool registration (all tiers)
    resources.go         — Resource handlers
  kafka/            — Kafka client operations (consume, produce, admin)
  auth/             — Policy-based authorization engine
  config/           — YAML configuration parsing
  schema/           — Schema Registry client and cache
  audit/            — Structured audit logging
  match/            — Topic pattern matching utilities
  metrics/          — Prometheus metrics
  ratelimit/        — Per-agent rate limiting
  anomaly/          — AI-powered anomaly detection
  cost/             — Kafka cost analysis
  dlq/              — Dead-letter queue remediation
  forecast/         — Capacity forecasting
  gitops/           — GitOps configuration management
  graph/            — Embedded knowledge graph
  lagdiag/          — Consumer lag root cause analysis
  lineage/          — Data lineage tracking
  masking/          — PII redaction engine
  migration/        — Migration planning
  nlquery/          — Natural language query engine
  persona/          — Declarative agent personas
  platform/         — Multi-platform abstraction
  plugin/           — Plugin/extension registry
  selfheal/         — Self-healing consumer groups
  telemetry/        — OpenTelemetry tracing
  watch/            — Event-driven watch subscriptions
  workflow/         — Agent workflow templates
sdk/                — Client SDKs (Go, Python, VS Code, Cursor, IntelliJ)
examples/           — Example configs (docker-compose, Confluent Cloud)
deploy/             — Deployment (Helm chart)
```

## Questions?

Open a [GitHub Discussion](https://github.com/kafkamcp/kafkamcp/discussions) or file an issue.
