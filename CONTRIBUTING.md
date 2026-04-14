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

### 3. Run Tests

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
  kafka/            — Kafka client operations (consume, produce, admin)
  auth/             — Policy-based authorization engine
  config/           — YAML configuration parsing
  schema/           — Schema Registry client and cache
  audit/            — Structured audit logging
  metrics/          — Prometheus metrics
  ratelimit/        — Per-agent rate limiting
examples/           — Example configs (docker-compose, Confluent Cloud)
```

## Questions?

Open a [GitHub Discussion](https://github.com/kafkamcp/kafkamcp/discussions) or file an issue.
