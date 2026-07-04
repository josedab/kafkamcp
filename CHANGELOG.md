# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **`kafka_acl_manage` performs real broker ACL reconciliation.** The tool is now stateless: `list`/`audit`/`diff`/`dry_run` read live broker ACLs against caller-supplied desired rules, and the new approval-gated `apply`/`reconcile` actions create missing and delete obsolete exact ACLs via franz-go/kadm (`KafkaManager.CreateACL`/`DeleteACL`). The in-memory `set_desired` desired-state plan and the `principals` action were removed. Each desired rule is validated (principal, host, resource type/name, pattern, operation, permission) and per-rule results are returned.

### Removed
- **Unreachable legacy server surface pruned.** `registerTools()` and the platform/analytics/streaming/governance initializers only register/instantiate the 29 supported tools and the engines they need (policy, identity, audit, rate-limit, masking, lag analyzer, onboarding discovery, OPA, ACL reconcile, telemetry). Every uncalled `register_*.go`, unsupported `tools_*.go`, feature resource handler, option hook, unreachable-handler smoke test, and internal feature package no longer reachable from `cmd/kafkamcp` were deleted. `Server.Close` now shuts subsystems down in reverse initialization order.

### Removed
- **Simulated experimental product removed.** The eight simulated tools (`kafka_chaos_inject`, `kafka_chaos_stop`, `kafka_chaos_gymnasium`, `kafka_wasm_load`, `kafka_wasm_execute`, `kafka_wasm_manage`, `kafka_proxy`, `kafka_migration_plan`), the `experimental` tool pack, the `ExperimentalConfig` block, and the `internal/chaos`, `internal/wasmruntime`, `internal/kafkaproxy`, `internal/migration`, and `internal/autopilot` packages were deleted. The supported surface is now exactly **29 tools** (15 stable core-read + 14 beta). No simulated or placeholder tools ship.
- **Decorative deployment/control-plane artifacts removed.** The `KafkaMCPCluster` CRD, the `internal/operator` Kubernetes controller, the `internal/gitops` reconciler surface, and the `internal/tenant` multi-tenant control plane (and their server initialization, registration, and `k8s_operator_manage`/`kafka_gitops_diff`/`kafka_usage` tools) were deleted because they were never wired to real behavior.
- **Fake IDE integrations removed.** The VS Code extension (`sdk/vscode`) and IntelliJ plugin (`ide/intellij`, `sdk/intellij`) were deleted because they targeted phantom endpoints/tools. The Cursor MCP configuration (`sdk/cursor`) — a real MCP client config — is retained.

### Changed
- **Data masking is now enforced on every message-returning read tool.** `kafka_consume`, `kafka_search`, and `kafka_analyze_dlq` (including nested DLQ sample values) apply the configured masking engine to message keys/values before returning. Masking failures are fail-closed: the tool returns an error rather than leaking unmasked data. Kafka metadata (partition/offset/timestamp/headers) and schema fields are not masked.
- **SDKs trimmed to the supported surface.** The Go, TypeScript, LangChain, and CrewAI SDKs no longer expose phantom tools (`kafka_watch`, `kafka_nl_query`, `kafka_anomalies`, `kafka_advisor_*`, `kafka_aggregate`, `kafka_schema_diff`); `kafka_describe_schema` was corrected to `kafka_get_schema`. Each SDK has a test enumerating every emitted tool name against the 29-tool allow-list.

### Security
- Static bearer-token providers now retain only SHA-256 fingerprints and scan every configured fingerprint with constant-time comparison.
- Approval grants and idempotency receipts now use backend-level atomic compare-and-swap operations. Separate managers sharing SQLite cannot consume the same single-use grant or reserve the same idempotency key twice.
- CI and the readiness reporter now block on high-severity, high-confidence gosec findings while retaining the complete report for advisory review.

### Changed
- Kafka broker and Kafka Connect TLS now default to TLS 1.2 minimum for compatibility with managed providers and proxies; operators can require TLS 1.3 with `clusters[].tls_min_version` and `clusters[].connect_tls.min_version`.
- Kubernetes health semantics are split: `/livez` is process-local, `/healthz` checks Kafka, and `/ready` checks Kafka plus persistence. Metrics/health binding is configurable through `server.metrics_host` and `server.metrics_port`.
- The Helm chart now rejects unsafe multi-replica/autoscaling configurations when guarded packs or SQLite are enabled, uses `Recreate` for singleton stateful rollouts, and validates service/config port alignment.
- **Autopilot execute/rollback and migration execution/planning tools removed.** `kafka_autopilot_execute`, `kafka_autopilot_rollback`, `kafka_migrate_execute`, `kafka_migrate_status`, `kafka_migrate_rollback`, and `kafka_migration_plan` are no longer registered in any pack, and the `experimental` config block and pack were removed entirely. The supported surface is the 29-tool `core-read` + `ops-write` + `governance` union.
- **`ops-write` is now rejected on stdio.** The config validator refuses to start with `server.tool_packs: [ops-write]` and `server.transport: stdio`. The complete requirement chain for `ops-write` is: (1) `sse` or `streamable-http` transport, (2) cryptographic identity provider (`static`/`oidc`/`mtls`), (3) `allow_header_fallback: false`, (4) `default_deny: true`, (5) `KAFKAMCP_APPROVAL_SIGNING_KEY` ≥32 bytes. Any missing condition is a hard startup error. **Migration:** switch to a network transport and supply the required config before enabling `ops-write`.
- **Tool pack set tightened.** Production pack names are now `core-read`, `ops-write`, and `governance`. The `analytics`, `streaming`, `all`, and `experimental` pack names are no longer accepted; the config validator rejects them at startup. Deprecated aliases `essential` and `ops` remain accepted but emit migration guidance.
- **`ops-write` always enforces approval and requires a signing key.** Enabling the `ops-write` pack now unconditionally activates approval enforcement and requires `KAFKAMCP_APPROVAL_SIGNING_KEY` (≥32 bytes) to be set in the environment. Setting `policies.approval.enabled: false` while `ops-write` is active is a startup error.
- **Approval defaults to two-person (allow_self_approval: false).** The requesting identity cannot approve its own grant without explicitly setting `policies.approval.allow_self_approval: true`. This is now documented and the default in `ApprovalConfig`.
- **Network transports reject identity header fallback.** The `TransportSecurityError` guard now explicitly requires `policies.identity.allow_header_fallback: false` for network transports (`sse`, `streamable-http`). An attacker cannot bypass a cryptographic identity provider by omitting the bearer token and falling back to `X-Agent-ID`.
- **Marketplace, control-plane, agent-store, and cloud-deploy surfaces fully deleted.** Server handlers, tool registrations, and the `ExperimentalConfig.control_plane` and `ExperimentalConfig.marketplace` config fields were removed from the codebase. These are now deferred architecture only — not runtime stubs — and cannot be re-enabled by any config flag. Tools affected: `kafka_plugin_install`, `kafka_plugin_search`, `kafka_plugin_list`, `kafka_controlplane_*`, `kafka_agent_store_*`, `kafka_cloud_deploy_*`.

### Added
- Optional Helm-managed ReadWriteOnce persistence (`persistence.enabled`) that provisions or mounts a PVC and injects the SQLite backend path.
- Helm production smoke tests covering probe wiring, metrics reachability configuration, PVC rendering, singleton guards, session affinity, and OTLP egress.
- `server.metrics_host`, `server.metrics_port`, `clusters[].tls_min_version`, and `clusters[].connect_tls.min_version` configuration fields.
- `policies.approval.allow_self_approval` config field (bool, default `false`): controls the two-person approval rule.
- `policies.approval.signing_key_env` config field (string, default `KAFKAMCP_APPROVAL_SIGNING_KEY`): names the environment variable holding the HMAC-SHA-256 decision-signing key.
- `policies.identity` config block added to `docs/kafkamcp.schema.json` with full subfields for `static`, `oidc`, and `mtls` providers, and `allow_header_fallback`.
- `KAFKAMCP_APPROVAL_SIGNING_KEY` environment variable documented in README, schema, and OPERATIONS.md.
- `KAFKAMCP_UNSAFE_ALLOW_UNAUTHENTICATED_NETWORK` environment variable documented (equivalent to `server.dangerously_allow_unauthenticated_network: true`).
- Config validation rules for `ops-write` key requirement and network transport identity rejection added to README Config Validation table.

<details>
<summary>Superseded pre-reset development notes</summary>

The notes below describe intermediate implementations that existed during the
roadmap reset. They are retained for development history only and are not the
current supported product contract. The sections above and the generated tool
reference are authoritative.

### Added
- Adaptive tool surface: register tools by **tier** (`stable`, `beta`) and **pack** (`core-read`, `ops-write`, `governance`). Configure via `server.tool_packs` in `kafkamcp.yaml`; defaults to `[core-read]` (≤15 read-only tools). Deprecated aliases `essential` and `ops` are accepted with migration guidance. The former `analytics`, `streaming`, `all`, and `experimental` packs are removed.
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
- **First-class Protobuf wire-format codec** (`internal/codec/protobuf/`). The codec engine now emits real Confluent-interoperable Protobuf binary via `github.com/bufbuild/protocompile` (schema compilation) + `google.golang.org/protobuf/types/dynamicpb` (schema-driven message instantiation). No `protoc` build step required, no code generation. Implements the Confluent message-index zigzag-varint prefix (including the `[0]` single-zero-byte optimisation). New `codec.NewEngineWithProtobuf(provider, codec)` / `Engine.SetProtobufCodec` API; the default platform initializer registers a real Protobuf codec alongside Avro so `kafka_codec_encode` / `kafka_codec_decode` produce schema-registry-readable bytes out of the box. See [ADR-010](docs/adr/010-protobuf-wire-codec.md).

### Changed
- `internal/server`: every `mcp.NewTool` registration now flows through the new `s.addTool` wrapper which consults the active `ToolPackFilter`. Experimental tools (`kafka_chaos_*`, `kafka_wasm_*`, `kafka_proxy`, `kafka_migration_plan`) are tagged with tier `experimental` and only register when the `experimental` pack is active. Migration execution/rollback tools and marketplace, control-plane, agent-store, and cloud-deploy tool families are fully deleted (handlers, registrations, and `ExperimentalConfig` fields removed).
- `docs/tool-reference.md` and the website API reference are generated from the explicit `coreReadTools`, `writeToolPolicies`, `governancePackTools`, and `experimentalPackTools` allow-lists.
- `docs/PERSISTENCE.md` rewritten to reflect the new pluggable backend and migration roadmap.

</details>

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

[Unreleased]: https://github.com/josedab/kafkamcp/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/josedab/kafkamcp/releases/tag/v0.1.0
