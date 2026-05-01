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

This installs dev dependencies and a **git pre-commit hook** that runs `make fmt-check`, `make vet`, and `make lint` before each commit. You can also install the hook separately:

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
3. Run `make pre-push` before pushing (runs fmt-check, vet, lint, test, build).
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
  adaptiverate/     — Adaptive rate limiting
  advisor/          — Cluster health advisor
  agentmemory/      — Agent memory persistence
  anomaly/          — AI-powered anomaly detection
  catalog/          — Data catalog discovery
  chaos/            — Chaos testing toolkit
  cloud/            — Cloud provider integrations
  collaboration/    — Multi-agent collaboration
  compliance/       — Compliance policy enforcement
  configvcs/        — Configuration version control
  contracts/        — Schema-aware data contracts
  cost/             — Kafka cost analysis
  costattribution/  — Cost attribution per team/topic
  dagworkflow/      — DAG-based workflow engine
  dashboard/        — Dashboard data aggregation
  dlq/              — Dead-letter queue remediation
  evolution/        — Schema evolution advisor
  federation/       — Multi-cluster federation
  flowdebug/        — Message flow debugging
  forecast/         — Capacity forecasting
  gateway/          — API gateway integration
  gitops/           — GitOps configuration management
  graph/            — Embedded knowledge graph
  ide/              — IDE integration utilities
  lagdiag/          — Consumer lag root cause analysis
  lineage/          — Data lineage tracking
  masking/          — PII redaction engine
  migration/        — Migration planning
  nlquery/          — Natural language query engine
  observability/    — Observability pipeline
  onboarding/       — Guided onboarding flows
  opa/              — Open Policy Agent integration
  operator/         — Kubernetes operator support
  persona/          — Declarative agent personas
  pipeline/         — Stream processing pipelines
  platform/         — Multi-platform abstraction
  plugin/           — Plugin/extension registry
  profiler/         — Performance profiling
  replay/           — Message replay engine
  replication/      — Cross-cluster replication monitoring
  sandbox/          — Sandboxed execution environment
  selfheal/         — Self-healing consumer groups
  semantic/         — Semantic search over messages
  streamsql/        — SQL over Kafka streams
  taskqueue/        — Background task queue
  telemetry/        — OpenTelemetry tracing
  tenant/           — Multi-tenant isolation
  urlutil/          — URL validation utilities
  viewstore/        — Materialized view store
  watch/            — Event-driven watch subscriptions
  workflow/         — Agent workflow templates
sdk/                — Client SDKs (Go, Python, TypeScript, VS Code, Cursor, IntelliJ)
examples/           — Example configs (docker-compose, Confluent Cloud)
deploy/             — Deployment (Helm chart, Grafana dashboard)
```

## Package Tiers

Internal packages are grouped into three tiers. Higher tiers may depend on lower tiers but not the reverse.

```
┌──────────────────────────────────────────────────────────────────┐
│  Feature (optional modules — safe to disable)                    │
│  adaptiverate, advisor, agentmemory, anomaly, catalog, chaos,    │
│  collaboration, compliance, configvcs, contracts, cost,          │
│  costattribution, dagworkflow, dashboard, dlq, evolution,        │
│  federation, flowdebug, forecast, gateway, gitops, graph, ide,   │
│  lagdiag, lineage, migration, nlquery, observability, onboarding,│
│  opa, operator, persona, pipeline, plugin, profiler, replay,     │
│  replication, sandbox, selfheal, semantic, streamsql, taskqueue, │
│  viewstore, watch, workflow                                      │
├──────────────────────────────────────────────────────────────────┤
│  Infrastructure (cross-cutting concerns)                         │
│  audit, auth, cloud, masking, match, metrics, ratelimit,         │
│  telemetry, tenant, platform, urlutil                            │
├──────────────────────────────────────────────────────────────────┤
│  Core (always needed)                                            │
│  kafka, server, config, schema                                   │
└──────────────────────────────────────────────────────────────────┘
```

- **Core** packages provide the Kafka client, MCP server, configuration parsing, and schema registry integration. Every deployment uses them.
- **Infrastructure** packages supply cross-cutting capabilities (auth, audit, metrics, masking) used by both Core and Feature packages.
- **Feature** packages are self-contained modules that add optional functionality. They depend on Core and Infrastructure but never on each other.

### Package Reference

#### Core

| Package | Description |
|---------|-------------|
| `config` | YAML configuration loading, validation, and env-var expansion |
| `kafka` | Kafka client abstraction (admin, consumer, producer via franz-go) |
| `schema` | Schema Registry client with caching (Avro/Protobuf/JSON) |
| `server` | MCP server core: tool registration, handlers, resources, transport |

#### Infrastructure

| Package | Description |
|---------|-------------|
| `aclmgmt` | Kafka ACL management and enforcement |
| `audit` | Structured JSON audit logging of agent interactions |
| `auth` | Policy-based authorization engine (default-deny) |
| `cloud` | Cloud provider integrations (AWS, GCP, Azure) |
| `masking` | PII redaction engine with field-level masking rules |
| `match` | Topic name glob/pattern matching utilities |
| `metrics` | Prometheus metrics collection and export |
| `platform` | Multi-platform abstraction layer |
| `ratelimit` | Per-agent token-bucket rate limiting |
| `telemetry` | OpenTelemetry distributed tracing |
| `tenant` | Multi-tenant isolation and routing |
| `urlutil` | URL validation and normalization utilities |

#### Feature — Kafka Operations

| Package | Description |
|---------|-------------|
| `cdc` | Change data capture stream processing |
| `chaos` | Chaos testing toolkit for Kafka resilience |
| `codec` | Message encoding/decoding utilities |
| `dlq` | Dead-letter queue inspection and remediation |
| `eventsourcing` | Event sourcing pattern support |
| `lagdiag` | Consumer lag root-cause analysis |
| `migration` | Topic and consumer group migration planning |
| `msgsampling` | Statistical message sampling from topics |
| `replay` | Message replay engine for topic reprocessing |
| `replication` | Cross-cluster replication monitoring |
| `retention` | Topic retention policy management |
| `selfheal` | Self-healing consumer group remediation |
| `smartroute` | Intelligent message routing across topics |
| `streamsql` | SQL query engine over Kafka streams |
| `topicportal` | Topic portal UI data provider |
| `topictemplate` | Topic creation templates and standards |
| `viewstore` | Materialized view store from Kafka topics |
| `watch` | Event-driven watch subscriptions |

#### Feature — Schema & Data Quality

| Package | Description |
|---------|-------------|
| `contracts` | Schema-aware data contracts |
| `dataquality` | Data quality scoring and validation |
| `evolution` | Schema evolution advisor and compatibility checks |
| `schemaadvisor` | Schema design recommendations |
| `schemaplayground` | Interactive schema testing sandbox |

#### Feature — Observability & Monitoring

| Package | Description |
|---------|-------------|
| `anomaly` | AI-powered anomaly detection on metrics |
| `correlation` | Cross-signal event correlation |
| `dashboard` | Dashboard data aggregation |
| `flowdebug` | Message flow debugging and tracing |
| `forecast` | Capacity and growth forecasting |
| `healthscore` | Cluster and topic health scoring |
| `incident` | Incident detection and management |
| `observability` | Observability pipeline integration |
| `predictalert` | Predictive alerting based on trends |
| `profiler` | Performance profiling utilities |
| `sessionreplay` | Agent session replay and debugging |
| `timeseries` | Time-series metric storage and queries |

#### Feature — AI & Analytics

| Package | Description |
|---------|-------------|
| `advisor` | Cluster health advisor with recommendations |
| `catalog` | Data catalog discovery and metadata |
| `chat` | Conversational agent interface |
| `crosslineage` | Cross-cluster data lineage |
| `digitaltwin` | Digital twin simulation of Kafka clusters |
| `graph` | Embedded knowledge graph of cluster topology |
| `lineage` | Data lineage tracking across topics |
| `llmpipeline` | LLM-powered data processing pipelines |
| `nlpipeline` | Natural language processing pipelines |
| `nlquery` | Natural language query engine for Kafka |
| `semantic` | Semantic search over message content |
| `vectorsync` | Vector embedding synchronization |

#### Feature — Agent & Workflow

| Package | Description |
|---------|-------------|
| `agentmemory` | Agent memory persistence across sessions |
| `agentprofile` | Agent profile and preference management |
| `capability` | Dynamic capability discovery |
| `collaboration` | Multi-agent collaboration primitives |
| `dagworkflow` | DAG-based workflow execution engine |
| `onboarding` | Guided onboarding flows for new agents |
| `orchestration` | Multi-step task orchestration |
| `persona` | Declarative agent persona definitions |
| `pipeline` | Stream processing pipeline builder |
| `playbook` | Operational playbook execution |
| `runbook` | Automated runbook procedures |
| `taskqueue` | Background task queue with retries |
| `workflow` | Agent workflow templates |

#### Feature — Infrastructure & Deployment

| Package | Description |
|---------|-------------|
| `clouddeploy` | Cloud deployment automation |
| `controlplane` | Control plane for multi-cluster management |
| `federation` | Multi-cluster federation and routing |
| `gateway` | API gateway integration |
| `operator` | Kubernetes operator support |
| `sidecar` | Sidecar proxy pattern support |

#### Feature — Cost & Governance

| Package | Description |
|---------|-------------|
| `compliance` | Compliance policy enforcement |
| `configvcs` | Configuration version control |
| `cost` | Kafka infrastructure cost analysis |
| `costattribution` | Per-team/topic cost attribution |
| `finops` | Financial operations and optimization |
| `gitops` | GitOps configuration management |
| `opa` | Open Policy Agent integration |

#### Feature — Misc

| Package | Description |
|---------|-------------|
| `adaptiverate` | Adaptive rate limiting based on load |
| `benchmark` | Benchmark utilities and harnesses |
| `devmode` | Development mode helpers |
| `ide` | IDE integration utilities (LSP, completions) |
| `plugin` | Plugin/extension registry |
| `sandbox` | Sandboxed execution environment |

## Questions?

Open a [GitHub Discussion](https://github.com/kafkamcp/kafkamcp/discussions) or file an issue.

## Adding a New Tool

Use the scaffolding command to create a new tool with all the required boilerplate:

```bash
make new-tool NAME=mytool
```

This generates three files:

- `internal/mytool/engine.go` — Business logic package with an `Engine` struct
- `internal/server/tools_mytool.go` — MCP tool handler with a placeholder implementation
- `internal/server/register_mytool.go` — Tool registration function

After scaffolding, complete these manual steps:

1. Add the engine field to the appropriate group struct in `internal/server/server.go`
2. Initialise it in `New()` in `internal/server/server.go`
3. Call `s.registerMyToolTools()` from `registerTools()` in `internal/server/register_tools.go`
4. Implement the handler logic in `internal/server/tools_mytool.go`
5. Add tests in `internal/mytool/engine_test.go`
6. Run `make generate` to regenerate tool reference docs

## Code Generation

After modifying tool definitions in `register_*.go` files, or any code with `//go:generate` directives, run:

```bash
make generate
```

This runs all code generation steps including tool reference docs (`cmd/gen-tool-docs/`) and `go generate ./...`. Always run this before submitting a PR that modifies tools.

## SDK Development

KafkaMCP ships with client SDKs for multiple languages and editors. All SDKs live under the `sdk/` directory:

| SDK | Path | Language |
|-----|------|----------|
| Go SDK | `sdk/go/` | Go |
| Python LangChain | `sdk/python/kafkamcp_langchain/` | Python |
| Python CrewAI | `sdk/python/kafkamcp_crewai/` | Python |
| TypeScript SDK | `sdk/typescript/` | TypeScript |
| VS Code Extension | `sdk/vscode/` | TypeScript |
| IntelliJ Plugin | `sdk/intellij/` | Kotlin/Java |
| Cursor Integration | `sdk/cursor/` | JSON config |

### Setup

Install all SDK test dependencies (requires Python 3.8+ and Node.js 18+):

```bash
make sdk-setup
```

### Running SDK Tests

Run tests across all SDKs:

```bash
make sdk-test
```

### Per-SDK Workflows

**Go SDK** — standard Go workflow:
```bash
go test ./sdk/go/...
```

**Python SDKs** (LangChain / CrewAI):
```bash
cd sdk/python/kafkamcp_langchain
pip install -e ".[test]"
python -m pytest -q
```

**VS Code Extension**:
```bash
cd sdk/vscode
npm install
npm test
```

**TypeScript SDK**:
```bash
cd sdk/typescript
npm install
npm test
```

When adding or modifying SDK functionality, ensure `make sdk-test` passes before submitting a PR.

## Documentation Standards

When contributing new tools, features, or configuration options, follow these guidelines:

### Documenting New Tools

- Add a description to the tool's `mcp.NewTool()` definition — this is the primary documentation surface for MCP clients.
- Include parameter descriptions with types, defaults, and constraints in the tool registration.
- Add an entry to the API reference in `website/docs/api-reference/tools.md` with at least one usage example.

### Documenting New Features

- Update `docs/ARCHITECTURE.md` if the feature introduces a new package, subsystem, or changes the server lifecycle.
- Add a new ADR in `docs/adr/` for significant architectural decisions, and update the ADR index in `docs/adr/README.md`.
- Update `website/docs/` if the feature is user-facing (configuration options, new CLI flags, new resources).

### Windows Development

KafkaMCP is developed primarily on macOS and Linux. If developing on Windows:

- Use Git Bash, WSL2, or a similar Unix-compatible shell — the Makefile targets assume a Unix environment.
- Ensure `CGO_ENABLED=1` if running tests with `-race` (requires a C compiler, e.g., via [tdm-gcc](https://jmeubank.github.io/tdm-gcc/)).
- File paths in configuration should use forward slashes or raw strings to avoid escaping issues.

### Dependency Management

- Run `go mod tidy` after adding or removing dependencies.
- Do not commit vendored dependencies — the project uses module mode.
- Pin indirect dependencies only when necessary to resolve version conflicts.
- When upgrading a dependency, verify that `make validate` still passes before submitting.

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

> **Note:** `make check` is an alias for `make validate` — both run the full fmt-check → vet → lint → test → build pipeline. `make validate-ci` and `make pre-push` run the same checks but signal CI/pre-push intent. If formatting issues are reported, run `make fmt` first, then re-run validation.

> **CI vs Local:** In CI (`.github/workflows/ci.yml`), `lint` runs as a **separate parallel job** alongside `test` for faster feedback. Locally, `make validate` runs fmt-check → vet → lint → test → build as a single sequential pipeline. Both must pass before merging — CI simply parallelises them. If you pass `make validate` locally, you will pass CI.
