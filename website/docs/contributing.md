---
sidebar_position: 12
title: Contributing
---

# Contributing

Thanks for helping improve KafkaMCP.

This guide covers the fastest path to building, testing, and contributing changes.

## Prerequisites

- Go 1.25+
- Docker (useful for integration-style work and the local Kafka examples)
- Git

## Build from source

Clone the repository and build the binary:

```bash
git clone https://github.com/josedab/kafkamcp.git
cd kafkamcp
go build -o bin/kafkamcp ./cmd/kafkamcp
```

## Run tests

Run the standard Go test suite:

```bash
go test ./...
```

If you want to match CI more closely, also run:

```bash
go test -race ./...
go build ./...
go mod tidy
```

:::tip
If you change the Docusaurus site, run `npm --prefix website run build` before opening a PR.
:::

## Validate the example config

```bash
./bin/kafkamcp validate --config kafkamcp.yaml
```

## Project structure

```text
kafkamcp/
├── cmd/kafkamcp/          # CLI entrypoint
├── internal/
│   ├── audit/             # Structured JSON audit logger
│   ├── auth/              # YAML policy engine
│   ├── config/            # Config loader + env var expansion
│   ├── kafka/             # Kafka admin, consume, produce, P1/P2 operations
│   ├── metrics/           # Prometheus metric definitions
│   ├── ratelimit/         # Per-agent limiter
│   ├── schema/            # Schema Registry client + cache
│   └── server/            # MCP tool and resource registration + handlers
├── examples/              # Example configs and docker-compose files
├── website/               # Docusaurus docs site
├── Dockerfile
├── docs/internal/PRD.md
└── kafkamcp.yaml
```

## How to add a new tool

KafkaMCP has no experimental pack for incomplete work. A tool is registered
only after it performs real Kafka/provider behavior and has unit plus
integration/provider-contract tests.

1. Implement reusable logic in `internal/kafka/`, `internal/schema/`, or a
   narrowly scoped supported domain package.
2. Add a handler in `internal/server/tools_<name>.go` using the common auth,
   approval, idempotency, masking, and audit paths.
3. Add a registration file and include the name in exactly one allow-list in
   `internal/server/tool_packs.go`.
4. Add tests, regenerate the tool reference, and run every release gate.

Do not register handlers that return canned, simulated, placeholder, or
metadata-only success. See the root
[`CONTRIBUTING.md`](https://github.com/josedab/kafkamcp/blob/main/CONTRIBUTING.md)
for the full admission checklist.

:::info
If a new tool reads or mutates Kafka data, make sure the auth model, audit logging, and metrics story are clear before you merge it.
:::

## Pull request process

1. Fork the repository.
2. Create a focused branch.
3. Keep changes scoped to one problem.
4. Run build and tests locally.
5. Update docs when behavior changes.
6. Open a PR with a clear description, rationale, and testing notes.

A strong PR description should include:

- what changed
- why it changed
- how you tested it
- any follow-up work or trade-offs

## Development expectations

- prefer precise changes over broad refactors
- keep public behavior documented
- preserve copy-paste-ready examples
- add comments only when code needs clarification

## Code style

- Follow standard Go conventions (`gofmt`, `goimports`).
- Use tabs for indentation (enforced by `gofmt`).
- Keep exported types and functions documented with GoDoc comments.
- Avoid unnecessary comments — let clear naming speak for itself.
- Error messages should be lowercase and not end with punctuation.
- Wrap errors with context: `fmt.Errorf("doing thing: %w", err)`.

## Commit conventions

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

## Code of conduct

This project expects respectful, professional collaboration.

By participating, you agree to:

- be constructive and considerate in issues, discussions, and PR reviews
- focus feedback on the code and the design, not the person
- avoid harassment, discrimination, and personal attacks
- help keep KafkaMCP welcoming to contributors of different backgrounds and experience levels

If maintainers ask for changes in behavior to keep the project healthy, respect that direction.

## Good first contribution ideas

- improve error messages
- add tests for edge cases in config, auth, or rate limiting
- expand examples for managed Kafka providers
- improve docs for real deployment scenarios
- tighten tool request and response examples

## Before you open the PR

Use this checklist:

- [ ] `go build ./...`
- [ ] `go test ./...`
- [ ] `go mod tidy`
- [ ] docs updated if behavior changed
- [ ] examples still work or were updated intentionally

Thanks again for contributing.
