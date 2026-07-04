# KafkaMCP Product Requirements Document

**Status:** Implemented production baseline

**Last updated:** 2026-07-27

**Product boundary:** Self-hosted, vendor-neutral Kafka operations gateway for
AI agents

## 1. Executive Summary

**Problem Statement:** AI agents need controlled access to Kafka operational
data, but direct broker credentials and ad hoc wrappers create excessive
security, audit, and maintenance risk.

**Proposed Solution:** KafkaMCP provides a self-hosted MCP server with a small,
real Kafka surface: 15 stable read-only tools, 14 approval-gated beta tools,
and 9 read-only resources. It applies identity, policy, rate limits, audit,
masking, approvals, and idempotency before Kafka operations execute.

**Success Criteria:**

- Generated references and runtime allowlists agree on exactly 29 supported
  tools and 9 resources.
- No registered tool or resource returns simulated broker results.
- All write and destructive tools require an authorization policy and a
  short-lived approval bound to the exact request.
- `make validate`, the 70% whole-repository coverage gate, security analysis,
  SDK tests, Helm checks, website build, and release checks pass before a tag.
- At least three independent production design partners complete a trial before
  expanding the product boundary.

## 2. User Experience & Functionality

**User Personas:**

- **Kafka operator/SRE:** investigates topic health, consumer lag, and DLQ
  failures during incidents.
- **Data platform engineer:** grants agents narrowly scoped topic, group,
  schema, or connector access.
- **Security/platform owner:** requires attributable identity, default-deny
  authorization, approvals, masking, and durable audit evidence.
- **Agent developer:** integrates through MCP directly or through the tested Go,
  TypeScript, LangChain, or CrewAI wrappers.

**User Stories:**

- As an operator, I want an agent to inspect topics, messages, schemas, groups,
  health, and lag so that it can gather incident evidence without a shell on a
  broker host.
- As a platform engineer, I want mutations to require explicit approval so that
  an agent cannot make an unreviewed production change.
- As a security owner, I want sensitive message fields masked before results
  leave the server so that policy enforcement is not delegated to the model.
- As an agent developer, I want runtime discovery and generated contracts to
  match so that integrations never call phantom tools.
- As an operator, I want retry-safe topic mutations so that transport retries do
  not duplicate an accepted operation.

**Acceptance Criteria:**

- Read operations resolve the requested cluster, validate bounded inputs,
  propagate cancellation, and audit both success and failure.
- Message-returning consume, search, and DLQ operations apply configured masking
  recursively and fail closed when masking cannot be completed.
- Guarded operations reject missing, expired, reused, or request-mismatched
  approvals.
- Supported idempotent operations atomically reserve and replay receipts across
  the configured persistence backend.
- ACL reconciliation normalizes broker values, matches principals exactly,
  creates before deleting, and reports partial failures.
- Kubernetes liveness is process-local; readiness evaluates dependencies without
  creating restart loops.
- SDK factories emit only supported tool names and their request contracts match
  the server schemas.

**Non-Goals:**

- Managed KafkaMCP cloud, billing, marketplace, or multi-tenant control plane.
- SAML, hosted identity, or a bundled user-facing dashboard.
- General-purpose stream processing, continuous subscriptions, joins,
  aggregations, semantic search, or natural-language query translation.
- Standalone Avro or Protobuf wire-codec tools.
- Simulated chaos, migration, proxy, WASM, or operator APIs.

## 3. AI System Requirements

**Tool Requirements:**

- MCP discovery must expose only the tools enabled by the configured packs and
  available dependencies such as Schema Registry or Kafka Connect.
- `core-read` is the safe default pack. `ops-write` and `governance` are opt-in
  and require production security configuration.
- Tool descriptions, JSON schemas, generated references, SDK allowlists, and
  runtime registrations must derive from or be checked against the same
  supported surface.
- Agent identity must come from the transport authentication layer rather than
  caller-controlled tool arguments.

**Evaluation Strategy:**

- Snapshot the stable tool schemas and fail CI on unreviewed changes.
- Run deterministic tool-evaluation fixtures for discovery, authorization,
  approval, masking, and error behavior.
- Test SDK wrappers against fake transports and real local MCP subprocesses.
- Compile integration-tagged tests even when a Kafka broker is unavailable; run
  broker-backed tests before a production release.
- Treat unsupported tool emission, success-shaped errors, or unmasked sensitive
  output as release blockers.

## 4. Technical Specifications

**Architecture Overview:**

1. An MCP transport receives an authenticated request.
2. Server middleware extracts identity, enforces policy and rate limits, checks
   approval/idempotency requirements, and starts audit/metrics timing.
3. A handler validates arguments and resolves the configured cluster.
4. `KafkaManager` performs all Kafka, Schema Registry, or Connect I/O.
5. The handler masks message data where required and returns structured MCP
   content.
6. Audit, activation, latency, and error telemetry record the outcome.

The supported runtime is intentionally limited to core configuration, Kafka,
schema, server, identity/policy, audit, masking, metrics, persistence,
approvals, idempotency, readiness, lag diagnosis, onboarding, OPA, ACL
reconciliation, and telemetry packages.

**Integration Points:**

- Apache Kafka-compatible brokers through franz-go.
- Optional Schema Registry and Kafka Connect HTTP endpoints per cluster.
- MCP over stdio, SSE, or streamable HTTP.
- Optional OIDC, mTLS, or static-token identity.
- In-memory or SQLite persistence for guarded state.
- Prometheus metrics and optional OpenTelemetry export.
- Kubernetes/Helm deployment with persistent SQLite support.

**Security & Privacy:**

- Production remote transports require authenticated identity and TLS.
- Policy evaluation is default-deny when production mode or guarded packs are
  enabled.
- Static tokens are compared by SHA-256 fingerprint in constant time.
- Approval and idempotency state transitions use backend-level atomic
  compare-and-swap.
- Audit records avoid raw secret material; masking occurs before message content
  is returned to the client.
- SQLite-backed guarded deployments enforce a single active replica.

## 5. Risks & Roadmap

**Phased Rollout:**

- **Production baseline:** Ship the implemented 29-tool/9-resource self-hosted
  product with release, deployment, compatibility, and operations documentation.
- **v1.0 evidence gate:** Promote stable contracts only after broker-backed
  reliability evidence, upgrade/rollback rehearsal, and three independent
  design-partner trials.
- **Post-v1 expansion:** Add capabilities only when customer evidence justifies
  the security and maintenance cost; managed cloud and SAML additionally require
  two committed paid pilots.

**Technical Risks:**

- Kafka provider differences can break ACL, TLS, or admin behavior; mitigate
  with the provider compatibility matrix and broker-backed release testing.
- Long or unbounded reads can exhaust broker/client resources; keep consume and
  search bounded by limits, scan caps, and timeouts.
- Approval or SQLite state can become unsafe under active-active replicas;
  reject unsupported replica/HPA combinations.
- MCP and SDK dependency changes can alter invocation contracts; pin/test
  release inputs and keep subprocess integration tests.
- Product breadth can reintroduce simulated features; require a real backing
  implementation, tests, documentation, and an explicit pack before
  registration.
