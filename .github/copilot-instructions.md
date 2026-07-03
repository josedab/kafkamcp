# KafkaMCP — Copilot Instructions

KafkaMCP is a Go 1.25 MCP (Model Context Protocol) server exposing Apache Kafka (topics, schemas, consumer groups, cluster metadata) as MCP tools and resources. It is built on `github.com/mark3labs/mcp-go` and `github.com/twmb/franz-go`.

## Build, test, lint

All workflows are driven by the `Makefile` (`make help` lists everything). Assume Unix shell; Windows users should use Git Bash/WSL2.

| Task | Command |
|------|---------|
| Full local gate (CI parity) | `make validate` (fmt-check → vet → lint → test → build) |
| Build binary to `bin/kafkamcp` | `make build` |
| Unit tests with `-race` + coverage | `make test` |
| Fast iteration (no race) | `make test-short` |
| Single test (by name + package) | `make test-one TEST=TestName PKG=./internal/kafka` |
| Single test (auto-locate package) | `make test-func FUNC=TestName` |
| Single package | `make test-pkg PKG=./internal/kafka` |
| Integration tests (need Kafka on `localhost:9092`) | `make test-integration-setup && make integration-test` |
| Lint (golangci-lint, version pinned in Makefile — currently `v1.62.0`) | `make lint` |
| Format (gofmt + goimports, local prefix `github.com/josedab/kafkamcp`) | `make fmt` |
| Regenerate tool reference docs (`docs/tool-reference.md`) | `make generate` |
| Coverage gate (CI fails below 70%) | `make test-coverage-check MIN_COVERAGE=70` |
| Hot-reload dev server (uses `air`) | `make dev` / `make dev-sse` / `make dev-http` |
| Local Kafka via Docker Compose | `make docker-dev` / `make docker-dev-down` |
| Diagnose env (Go, lint, Docker, Kafka, hook) | `make doctor` |
| Install dev deps + pre-commit hook | `make setup` |

Integration tests are guarded by `//go:build integration`. The pre-commit hook (installed via `make setup-hooks`) runs `fmt-check`, `vet`, `lint`. The pinned `GOLANGCI_LINT_VERSION` in the Makefile must match CI — install with `go install github.com/golangci/golangci-lint/cmd/golangci-lint@<pinned>` if `make doctor` warns about a version mismatch.

CI (`.github/workflows/ci.yml`) additionally runs: `go mod tidy` diff check, regenerated `docs/tool-reference.md` diff check, a tool-count consistency check between `docs/tool-reference.md` and `website/docs/api-reference/tools.md`, gosec, govulncheck, helm lint (`deploy/helm/kafkamcp`), cross-platform build matrix, and benchmarks on `./internal/kafka/`.

## Architecture

Read these five files (in order) to grasp the request lifecycle before changing anything non-trivial — they are also called out in `docs/ARCHITECTURE.md`:

1. `cmd/kafkamcp/main.go` — CLI, config load, KafkaManager wiring, transport startup.
2. `internal/config/config.go` — YAML loading, validation, env-var expansion (`${VAR}`).
3. `internal/server/server.go` — `Server` struct + `New()` constructor + functional options.
4. `internal/server/register_tools.go` — orchestrator that calls every `registerXxxTools()`.
5. `internal/kafka/manager.go` — sole Kafka client abstraction; all broker I/O goes through it.

### Runtime packages (`internal/`)

The production tree is intentionally small. Core/runtime packages are:
`app`, `config`, `server`, `kafka`, `schema`, `auth`, `approval`, `audit`,
`idempotency`, `engine`, `masking`, `ratelimit`, `metrics`, and `telemetry`.
Supported domain packages are `lagdiag`, `onboarding`, `opa`, and `aclmgmt`;
`doctor`, `readiness`, and `tooleval` provide validation tooling.

The `Server` embeds three small groups: **Governance** (policy/identity/audit/
rate-limit/masking), **Analytics** (lag diagnosis), and **Platform**
(discovery/OPA/ACL/OTel). Do not add or initialize a subsystem until a
registered, fully implemented tool needs it.

### Adding a new tool (three-file pattern)

Run `make new-tool NAME=mytool` — it scaffolds the canonical layout. You then manually wire the result:

1. `internal/<name>/engine.go` — real domain/provider logic, no MCP dependency.
2. `internal/server/tools_<name>.go` — handler methods on `*Server` (one per tool, named `handleXxx`).
3. `internal/server/register_<name>.go` — `register<Name>Tools()` that calls `s.mcpServer.AddTool(mcp.NewTool(...), s.handleXxx)`.
4. Add an engine field only if the registered tool needs persistent runtime state; initialise it in the appropriate existing group.
5. Add `s.register<Name>Tools()` to `registerTools()` in `internal/server/register_tools.go`.
6. Run `make generate` so `docs/tool-reference.md` is updated (CI enforces this), and update the tool count in `website/docs/api-reference/tools.md` to match (CI also enforces this).
7. Add the new `/internal/<name>/` path to `.github/CODEOWNERS` (`make check-codeowners` enforces this).

There is no experimental pack. Do not register placeholder, simulated,
metadata-only, or canned-success handlers. A new tool needs real behavior,
unit tests, and a real Kafka/provider integration contract before pack entry.

### Handler conventions

All tool handlers go through `s.withToolAuth(toolAuthConfig{tool, resourceType, action}, fn)`, which performs policy enforcement, agent ID extraction, rate limiting, and timing. Inside the inner closure:

- Validate every string input with `validateStringLength(name, value)`; return the resulting deny result.
- Resolve the target cluster with `s.resolveClusterName(cluster)` (multi-cluster aware) before calling `s.kafkaManager`.
- Always call `s.logAudit(agentID, "<tool_name>", args, cluster, "success"|"error", count, err, start)` on both success and error paths.
- Return `s.toolError("message", err)` for user-facing failures and `marshalResult(value)` for successful JSON payloads.
- Pass `ctx` through to all I/O so cancellations propagate.

See `internal/server/tools_p0.go` and `internal/server/tools_aclmgmt.go`.

### Resources

Resources are registered in `register_resources.go` and implemented in
`resources.go`. Six static resources and three templates form the complete
supported resource surface.

### Shutdown order

`Server.Close()` tears down in reverse init order: cancel context → HTTP server (10 s drain) → metrics server → subsystem `Close()` → `KafkaManager.Close()` last. Preserve this ordering when adding new closable resources.

## Conventions

- **Module path:** `github.com/josedab/kafkamcp`. `goimports` is configured with `local-prefixes` to this module — keep import groups: stdlib, third-party, local.
- **Errors:** wrap with `fmt.Errorf("doing thing: %w", err)`; messages lowercase and no trailing punctuation. Distinguish user errors (return `mcp.NewToolResultError` / `s.toolError`) from system errors (return as `error`).
- **Logging:** `*slog.Logger` is plumbed everywhere; do not introduce `log` or `fmt.Println`.
- **Deprecating a tool:** prefix its registered description with `[DEPRECATED]`, log a warning in the handler, keep it for at least one minor version, and note it in `CHANGELOG.md`.
- **Commits:** Conventional Commits (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`).
- **Tests:** `*_test.go` next to the source; mock-only tests use `*_mock_test.go`; integration tests use `//go:build integration`.
- **Coverage:** keep total ≥ 70 % (`make test-coverage-check`).
- **Releases:** triggered by pushing a `v*` tag; handled by GoReleaser via `.goreleaser.yml` and `release.yml`.

## Configuration

Server config is a single YAML file (default `kafkamcp.yaml`, schema in `docs/kafkamcp.schema.json`) with `${VAR}` env-var interpolation. Top-level sections: `server`, `clusters` (one entry per Kafka cluster, with optional `schema_registry` and `connect_url`), `schema_cache`, `policies` (per-agent RBAC, default-deny), `audit`, `masking`, `telemetry`. Two env vars override config: `KAFKAMCP_TRANSPORT` and `KAFKAMCP_LOG_LEVEL`. Transports: `stdio` (default), `sse`, `streamable-http`.

## SDKs

Client SDKs live under `sdk/`: `go/`, `python/kafkamcp_langchain/`, `python/kafkamcp_crewai/`, `typescript/`, `cursor/` (JSON config). Use `make sdk-setup` once, then `make sdk-test` to run them all. SDK tests are not part of `make validate`; run them when touching anything under `sdk/`.
