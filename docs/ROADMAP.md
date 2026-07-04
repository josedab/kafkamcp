# KafkaMCP Roadmap (Reset)

> **Status:** current shipping direction. This document supersedes the pre-reset
> breadth-first roadmap. Historical/internal product documents under
> `docs/internal/` are retained for context only and do **not** describe current
> behavior.

## Positioning

KafkaMCP is a **secure, vendor-neutral Kafka operations gateway for AI agents**.
It gives any MCP-compatible agent a small, trustworthy, read-only-by-default
surface over Apache Kafka and compatible platforms, with every mutating or
destructive operation placed behind explicit policy and short-lived approval.

We deliberately optimize for **trust and governance**, not tool count.

## Principles

1. **Secure by default.** The default surface is read-only (`core-read`, ≤15
   tools). Network transports fail closed without cryptographic identity and
   default-deny authorization.
2. **Explicit escalation.** Mutating tools require short-lived,
   single-use, auditable approval grants scoped to agent, tool, cluster,
   resource, and normalized arguments. The guarded `ops-write` and `governance`
   packs require secure HTTP identity, approval enforcement, and a 32-byte
   signing key. Two-person approval is the default.
3. **Truthful capabilities.** Every shipped tool performs real behavior;
   KafkaMCP ships no simulated or placeholder tools. Future incomplete work
   stays out of runtime packs until it has a real implementation and integration
   tests.
4. **Vendor neutrality.** Apache Kafka and Redpanda are first-class; MSK and
   Confluent Cloud are supported via documented profiles. We coexist with, and
   can delegate to, first-party servers (e.g. KIP-1318) rather than competing on
   primitive CRUD.
5. **Demand-gated commercialization.** Managed cloud, billing, SSO/SAML, and a
   marketplace are non-goals until real, verifiable customer demand exists.

## Capability classification

| Classification | Meaning | Where |
| --- | --- | --- |
| **Stable** | Read-only core, production-ready | `core-read` pack |
| **Beta** | Real and functional, still hardening | `ops-write`, `governance`, approval grants |

## Phases

### Phase 0 — Reset & unblock (done / in progress)

- Canonical repository identity (`github.com/josedab/kafkamcp`).
- CI readiness: supported Go toolchain, resolved govulncheck findings, modern
  golangci-lint, pinned CI tooling.
- Truthful public docs and reset roadmap.
- Security correctness: keyed HMAC-SHA-256 masking/tokenization (fail-closed
  without a key); fail-closed network transports; persistence-aware warnings.
- Release supply chain: checksums, SBOM, and provenance attestations.

### Phase 1 — Production core (done / beta)

- `core-read` default pack (≤15 strictly read-only tools). ✓
- Explicit `ops-write` and `governance` packs; `analytics`, `streaming`, `all`, and the simulated `experimental` pack removed. ✓
- Central unique tool-name enforcement (duplicate registration fails startup). ✓
- Short-lived approval grants for every write/destructive tool via a common
  enforcement path. ✓
- Guarded packs always enforce signed approvals, support verification-key
  rotation, and prune terminal grant history after a bounded retention window. ✓
- Default two-person approval (`allow_self_approval: false`). ✓
- AI tool-routing evaluation harness and expanded integration tests.

### Phase 2 — Validation assets

- Opt-in, privacy-preserving activation metrics (no phone-home).
- `validate`/doctor workflow proving config, identity, policy, and connectivity.
- Reproducible incident-triage demo and design-partner playbook.
- Verifiable comparison vs KIP-1318 / vendor-native MCP offerings.

### Phase 3 — Differentiated wedge (governance/approval, beta)

- Policy simulation, human approval workflow, blast-radius previews, recovery
  guidance, signed per-grant decision chains, verify/export. ✓
- Two-person approval default (`allow_self_approval: false`). ✓
- Network transport identity header fallback rejected. ✓
- Marketplace, control-plane, agent-store, cloud-deploy, and the eight
  simulated chaos/WASM/proxy/migration tools removed from all packs. ✓
- Vendor-neutral compatibility contracts and local harnesses (Kafka, Redpanda);
  documented MSK/Confluent profiles.
- KIP-1318 compatibility document and adapter boundary. ✓

### Phase 4 — v1 readiness

- Stable API/tool naming, semantic-versioning, deprecation, and LTS policies. ✓ ([`API_STABILITY.md`](API_STABILITY.md))
- Machine-checkable compatibility snapshots for stable tools and config schema. ✓ ([`COMPATIBILITY.md`](COMPATIBILITY.md))
- Production operations documentation and a readiness report. ✓ ([`OPERATIONS.md`](OPERATIONS.md), [`RELEASE.md`](RELEASE.md))

### Phase 5 — Commercial gate (not cloud implementation)

- A managed cloud or enterprise service is **not** implemented until:
  - three independent customers request it, and
  - two commit to paid pilots.
- Control-plane, marketplace/plugin, agent-store, cloud-deploy, and the
  simulated chaos/WASM/proxy/migration tool handlers (and the `ExperimentalConfig`
  block) have been **deleted** from the binary.
  These are deferred architecture only — not runtime stubs — and cannot be
  re-enabled by any config flag. See ADR-0001.

## Non-goals (demand-gated)

Managed cloud, billing, SSO/SAML, a plugin marketplace, generic
streaming-platform expansion beyond Kafka-compatible systems, and additional
feature families. See
[`docs/adr/0001-defer-managed-cloud-saml-billing.md`](adr/0001-defer-managed-cloud-saml-billing.md).
