# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Adaptive tool surface: register tools by **tier** (`essential`, `advanced`, `experimental`) and **pack** (`essential`, `ops`, `analytics`, `governance`, `streaming`, `experimental`, `all`). Configure via `server.tool_packs` in `kafkamcp.yaml`; defaults to `[essential]` so the advertised surface stays under the ~40–60 tool ceiling where LLM tool-routing degrades. Full catalog still ~317 tools — opt in to additional packs as needed.
- **Pluggable engine persistence backend** (`internal/engine/`, `internal/engine/backend/`). New `server.persistence` config:
  ```yaml
  server:
    persistence:
      backend: sqlite          # or "inmem" (default)
      path: /var/lib/kafkamcp/state.db
  ```
  Ships with two drivers: `inmem` (legacy in-memory, default) and `sqlite` (embedded pure-Go SQLite via `modernc.org/sqlite`, WAL mode, no cgo). A generic `engine.Store[T]` JSON helper makes it cheap for engines to opt in. See [ADR-006](docs/adr/006-persistent-engine-store.md).
- **Materialised view store now persists across restarts** when `server.persistence.backend: sqlite`. View definitions and view records are restored from disk on next start; `DeleteView` removes them durably.
- **Audit ring-buffer, rate-limit token buckets, and agent memory now persist** when `server.persistence.backend: sqlite`. Each engine ships a `NewLoggerWithBackend` / `NewWithBackend` / `NewEngineWithBackend` constructor that mirrors in-memory state to the shared `engine.Backend`. Restarts now retain the most recent audit ring-buffer window, in-flight token-bucket quotas (so an attacker can't restart their way to a fresh quota), and per-agent KV memory. Backwards-compatible: passing `nil` as the backend keeps the legacy in-memory behaviour, and the file-mode `audit.log_file` append stream is unchanged. See [docs/PERSISTENCE.md](docs/PERSISTENCE.md).
- **Agent identity verification** (`internal/auth/identity/`). New `policy.identity` config block adds three pluggable identity providers:
  - `none` (default) — legacy `X-Agent-ID` header behaviour, no migration cost.
  - `static` — shared-secret bearer tokens map to fixed agent IDs.
  - `oidc` — JWT bearer tokens verified against an OIDC issuer's JWKS using `github.com/coreos/go-oidc/v3`. Configurable `agent_id_claim`, `groups_claim`, `email_claim`, optional `jwks_url` override.

  The verified identity is attached to the request context and used by the policy engine in place of the unverified header. See [docs/AGENT_IDENTITY.md](docs/AGENT_IDENTITY.md) and [ADR-007](docs/adr/007-agent-identity-oidc.md).
- **mTLS identity provider** (`policy.identity.provider: mtls`). KafkaMCP now derives `AgentIdentity` from the verified TLS client certificate when the operator opts in. Identity-extraction order matches SPIFFE precedent — SPIFFE URI SAN → URI SAN → Subject CN — and supports `allowed_spiffe_ids` / `allowed_trust_domains` allow-lists, `groups_from_ous` mapping, and an `allow_cn_fallback` toggle for zero-trust deployments. Service-mesh, SPIFFE, and Consul Connect deployments can now authenticate KafkaMCP callers with the x509 identities they already have, with no token-minting layer in between. The HTTP transport automatically switches to `RequireAndVerifyClientCert` against the configured `ca_bundle` so an attacker cannot bypass mTLS by falling back to `X-Agent-ID`. Adds a new optional `identity.RequestVerifier` interface for providers that need the full `*http.Request` (e.g. peer-cert access). See [ADR-009](docs/adr/009-mtls-identity-provider.md).
- `audit.Entry` schema extended with `identity_method`, `issuer`, and `subject` fields. **Now populated**: `s.logAudit` takes a `ctx` parameter and reads the verified `identity.AgentIdentity` attached by the auth middleware. Header-asserted callers (legacy `X-Agent-ID`, STDIO, `provider: none`) leave the identity fields empty so audit consumers can distinguish trusted from un-trusted calls. New helpers `identity.WithIdentity(ctx, ident)` / `identity.FromContext(ctx)` make the verified identity accessible across packages without exposing internal context keys.
- **First-class Avro wire-format codec** (`internal/codec/wire/`, `internal/codec/avro/`). The codec engine now emits **real** Confluent-Schema-Registry-interoperable Avro binary via `github.com/hamba/avro/v2` instead of the previous JSON-in-wire-format placeholder. Drop-in interop with every official Avro consumer (Java, Python, .NET, Go `srclient`, `kafka-avro-console-consumer`). New `codec.NewEngineWithAvro(provider, codec)` constructor; the default platform initializer registers a real codec automatically. Strict-mode encoding (no `AllowJSONFallback`) returns `ErrNoAvroCodec` only when the registered codec is nil. See [ADR-008](docs/adr/008-avro-wire-format-codec.md).

### Changed
- `internal/server`: every `mcp.NewTool` registration now flows through the new `s.addTool` wrapper which consults the active `ToolPackFilter`. Experimental tools (`kafka_chaos_*`, `kafka_wasm_*`, `kafka_proxy`, `kafka_controlplane_*`, `kafka_migration_plan`, `kafka_autopilot_execute/rollback`, `kafka_plugin_install`) are now tagged with tier `experimental` and only register when the `experimental` (or `all`) pack is active.
- `docs/tool-reference.md` is grouped by tier; the generator (`cmd/gen-tool-docs`) now reads tier annotations and the `essentialPackTools` allow-list to render per-tier sections.
- `docs/PERSISTENCE.md` rewritten to reflect the new pluggable backend and migration roadmap.

## [0.1.0] - 2026-05-03

### Added
- YAML configuration loader with environment variable expansion
- Multi-cluster Kafka client manager with TLS and SASL support
- Message decoding with format-aware deserialization (auto, JSON, string, bytes)
- Schema Registry client with LRU cache and field summary
- YAML-based policy engine with glob pattern matching for authorization
- Structured JSON audit logger with ring buffer
- MCP server with 251 tools and 18 resource handlers
- Token bucket rate limiter (per-agent and global)
- Prometheus metrics for all tool operations
- Topic management: create, alter, delete topics
- Consumer group offset reset (earliest, latest, timestamp)
- Message search with key, header, and value filters
- Dead Letter Queue (DLQ) analysis with error categorization
- P2 operations: windowed aggregation, topic graph, stream join
- Natural language query interface for topic discovery
- Schema evolution suggestions
- Kafka Connect client with connector list/describe
- Cobra CLI with stdio, SSE, and streamable-http transports
- `--log-level` flag to override config file log level
- Configurable max message size limit
- Dockerfile and GoReleaser configuration
- CI pipeline with test, lint, and cross-platform build
- Docusaurus documentation website
- Example configurations for common deployment scenarios
- Contributing guidelines, issue templates, PR template, and CODEOWNERS

### Security
- Default-deny for unconfigured agents in rate limiter
- Authorization checks on all tool and resource handlers
- Agent identity validation on tool calls
- Cluster name validation against configuration
- Input length validation for string parameters
- Dangerous topic config keys restriction (min.insync.replicas, etc.)
- Sensitive data redaction in audit logs and connector configs
- Bootstrap servers removed from cluster list resource
- Minimum TLS 1.3 for Kafka and Connect clients
- Cryptographic random subscriber group ID generation
- SASL PLAIN over non-TLS warning
- Schema Registry credential over non-HTTPS warning
- HTTP security headers middleware
- Consume timeout and max_scan capping
- Window bounds clamping in aggregate and join handlers
- Join limit parameter clamping
- Semaphore for concurrent expensive operations
- HTTP response body size limit (10MB) for Connect client
- Metrics endpoint bound to localhost

### Fixed
- HTTP Connect client hardened with URL encoding and timeouts
- Audit logger mutex held during file write to prevent interleaved entries
- Schema Registry safe fallback on JSON escape failure
- Offset list failure logging improved
- Schema error handling in field summary improved

[Unreleased]: https://github.com/kafkamcp/kafkamcp/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/kafkamcp/kafkamcp/releases/tag/v0.1.0
