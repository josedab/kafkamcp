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

### 2. Setup

```bash
make setup
```

This installs dev dependencies and a **git pre-commit hook** that runs `make validate` (fmt → vet → lint → test → build) before each commit. You can also install the hook separately:

```bash
make setup-hooks
```

### 3. Build

```bash
make build
```

### 4. Setup VSCode (optional)

```bash
make setup-vscode
```

### 5. Run Tests

```bash
make test
```

### 6. Lint

```bash
make lint
```

### 7. Format Code

```bash
make fmt
```

### 8. Run All Checks

```bash
make validate
```

### 9. Hot-Reload Development (recommended)

Use `make dev` for a fast edit-save-test loop with [air](https://github.com/air-verse/air). Air watches for file changes and automatically rebuilds and restarts the server:

```bash
make dev          # stdio transport (default)
make dev-sse      # SSE transport
make dev-http     # streamable-http transport
```

Run `make doctor` to verify your environment has all required tools installed.

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

## Troubleshooting

If you run into build failures or unexpected behavior, start with:

```bash
make doctor
```

This checks your Go version, linting tools, Docker status, Kafka reachability, and pre-commit hook installation. It will tell you exactly what's missing or misconfigured.

### Common Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| `make doctor` reports missing tools | Dev dependencies not installed | Run `make setup` to install all required tools |
| `golangci-lint` reports different findings locally vs CI | Version mismatch between local and CI `golangci-lint` | Install the pinned version: check `GOLANGCI_LINT_VERSION` in the Makefile and run `go install github.com/golangci/golangci-lint/cmd/golangci-lint@<version>` |
| `make docker-dev` fails with "port 9092 already in use" | Another process (Kafka, Docker container) is bound to port 9092 | Stop the conflicting process: `lsof -i :9092` to identify it, then `kill <PID>` or `docker compose down` |
| `make dev` fails with "air: command not found" | `air` hot-reload tool is not installed | Run `go install github.com/air-verse/air@latest` or `make setup` |
| Tests fail with `-race` on macOS | The Go race detector requires CGO on macOS; `CGO_ENABLED=0` or missing Xcode CLI tools | Install Xcode CLI tools: `xcode-select --install` and ensure `CGO_ENABLED=1` (the default) |

> **Note:** `make check` is an alias for `make validate` — both run the full fmt → lint → test → build pipeline.
