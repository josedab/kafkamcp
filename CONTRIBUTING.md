# Contributing to KafkaMCP

Thank you for improving KafkaMCP. Contributions must preserve the core product
invariant: every registered tool performs real behavior and is covered by
tests. Simulated, canned, placeholder, or metadata-only tools are not accepted.

## Prerequisites

- Go version declared in `go.mod`
- Git
- Docker for integration tests
- Node.js 18+ for the documentation site and TypeScript SDK
- Python 3.8+ for Python SDK tests

Run:

```bash
make setup
make doctor
```

## Development workflow

```bash
git checkout -b feat/short-description
make test-short
make fmt
make validate
```

Useful targets:

| Task | Command |
| --- | --- |
| Full local gate | `make validate` |
| Fast unit tests | `make test-short` |
| One test | `make test-func FUNC=TestName` |
| Package tests | `make test-pkg PKG=./internal/kafka` |
| Coverage gate | `make test-coverage-check MIN_COVERAGE=70` |
| SDKs | `make sdk-test` |
| Security | `make security` |
| Helm | `make helm-check` |
| Readiness | `make readiness` |
| Documentation | `make generate && make docs` |
| Kafka integration | `make test-integration-setup && make integration-test` |

## Supported architecture

KafkaMCP exposes 29 tools across `core-read`, `ops-write`, and `governance`,
plus 9 resources.

```text
cmd/
  kafkamcp/          CLI
  readiness/         readiness command
internal/
  app/               runtime and transport lifecycle
  config/            config schema, defaults, validation
  server/            supported MCP tools/resources and middleware
  kafka/             supported Kafka/ACL/Connect operations
  schema/            Schema Registry client
  auth/              policy and identity providers
  approval/          signed approval grants
  idempotency/       retry receipts
  engine/            in-memory/SQLite persistence
  audit/             audit logging
  masking/           message payload masking
  ratelimit/         request budgets
  metrics/           Prometheus metrics
  telemetry/         OpenTelemetry
  lagdiag/           lag diagnosis
  onboarding/        cluster discovery
  opa/               policy evaluate/simulate
  aclmgmt/           ACL diff/audit/reconcile calculations
  doctor/            deployment diagnostics
  readiness/         release gates
  tooleval/          routing evaluation
sdk/
  go/
  python/
  typescript/
  cursor/
deploy/helm/         production Helm chart
website/             Docusaurus site
```

Read [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) before non-trivial changes.

## Code conventions

- Module path: `github.com/josedab/kafkamcp`.
- Keep imports grouped as standard library, third-party, local.
- Wrap errors with lowercase context: `fmt.Errorf("doing thing: %w", err)`.
- Use `*slog.Logger`; do not add `fmt.Println` or the legacy `log` package.
- Pass `context.Context` through all I/O.
- Avoid broad catches, silent fallbacks, and success-shaped partial failures.
- Keep types explicit; do not use unsafe casts to bypass compiler errors.

## Handler requirements

All tool handlers use `s.withToolAuth(...)`.

Inside the handler:

1. Validate every string with `validateStringLength`.
2. Resolve the cluster with `s.resolveClusterName`.
3. Call only real `KafkaManager`, Schema Registry, Connect, or supported domain
   behavior.
4. Apply message masking before returning payloads.
5. Audit success and error paths.
6. Return `s.toolError` for operational failure and `marshalResult` for success.
7. Let approval and idempotency middleware guard mutations.

See `internal/server/tools_p0.go` and `tools_aclmgmt.go`.

## Adding a tool

There is no experimental pack for incomplete work. Do not register a tool until
all steps are complete:

1. Implement real provider/domain behavior. No `TODO`, canned response, or
   simulated success.
2. Add a handler in `internal/server/tools_<name>.go`.
3. Add registration in `internal/server/register_<name>.go`.
4. Add the name to exactly one allow-list in
   `internal/server/tool_packs.go`.
5. For mutations, define the resource/action policy, approval scope, blast
   radius, recovery guidance, and safe idempotency behavior.
6. Add unit tests plus a real Kafka/provider integration contract.
7. Update config/schema/docs if the interface changes.
8. Run `make generate`; never edit generated tool references manually.
9. Run every release gate.

The optional `make new-tool NAME=name` target creates a compile-only skeleton.
The skeleton is not shippable and must remain unregistered until the
requirements above pass.

## Resources

Resources are read-only and registered in
`internal/server/register_resources.go`. New resources require policy checks,
rate limits, audit logging, stable URI documentation, and tests.

## Tests

- Source tests live next to source as `*_test.go`.
- Mock-focused tests may use `*_mock_test.go`.
- Integration tests use `//go:build integration`.
- New code must preserve total repository coverage of at least 70%.
- Do not add smoke tests whose only purpose is executing unreachable code.

## SDK changes

SDK methods may emit only names in the supported 29-tool allow-list.

```bash
make sdk-setup
make sdk-test
```

Every SDK has an allow-list test. Update SDK docs and tests together.

## Documentation

- Tool docs are generated from Go registrations.
- Operator claims must match live behavior.
- Time-sensitive compatibility claims need dates and evidence.
- Historical/internal strategy documents must be clearly marked as such.

Build the website with:

```bash
cd website
npm ci
npm run build
```

## Pull requests

1. Keep the change focused.
2. Use Conventional Commits.
3. Explain behavior, security impact, migration, and validation.
4. Include tests and direct documentation updates.
5. Run `make validate`, coverage, SDK, security, Helm, readiness, and docs.

## Releases

Releases are tag-driven through GoReleaser. Do not manually publish artifacts.
Before a release:

```bash
make readiness
make security
make helm-check
goreleaser check
```

See [`docs/RELEASE.md`](docs/RELEASE.md) for signature, SBOM, and provenance
verification.

## Questions and conduct

Use GitHub Discussions for design questions and Issues for reproducible bugs.
All contributors must follow [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md).
