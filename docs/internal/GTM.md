# KafkaMCP Go-to-Market Plan

**Status:** Current internal plan

**Last updated:** 2026-07-27

## Positioning

KafkaMCP is a **secure, self-hosted Kafka operations gateway for AI agents**.
It is not a stream-processing platform, hosted control plane, or catalog of
simulated agent demos.

Primary message:

> Give MCP-compatible agents production-safe Kafka visibility and tightly
> governed operational access without handing them unrestricted broker
> credentials.

Proof points:

- 15 stable read-only tools, 14 approval-gated beta tools, and 9 resources.
- Default-deny policy controls, transport identity, rate limits, audit logging,
  masking, approvals, and idempotency.
- Apache Kafka, Confluent-compatible, Redpanda, and multi-cluster configuration.
- Single Go binary plus Docker and Helm deployment paths.
- Generated tool contracts and tested Go, TypeScript, LangChain, and CrewAI
  wrappers.

## Target Users

1. Kafka operators and SREs building incident-response agents.
2. Data platform teams exposing governed Kafka self-service to internal agents.
3. AI platform teams that need Kafka access without embedding Kafka clients and
   credentials in every agent.

The initial use case is read-heavy incident investigation: discover clusters
and topics, inspect lag, sample bounded messages, analyze DLQs, and retrieve
schemas. Guarded writes are an opt-in extension, not the acquisition hook.

## Distribution

- GitHub repository, release artifacts, container image, and Helm chart.
- MCP client setup guides for Claude Desktop and Cursor.
- Source-distributed SDK integrations until package-registry publishing is part
  of a release process.
- Technical content focused on safe incident triage, policy design, masking,
  approvals, and provider compatibility.

Do not market unsupported capabilities, benchmark claims without reproducible
evidence, or unverified customer outcomes.

## Production Launch Gate

A public production release requires:

- All mandatory readiness checks passing.
- Whole-repository Go coverage at or above 70%.
- Go validation, security scans, SDK tests, Helm checks, integration-tag
  compilation, GoReleaser validation, and website build passing.
- A clean generated-contract diff with 29 supported tools and 9 resources.
- Completed production, persistence, identity, monitoring, compatibility,
  upgrade, rollback, and incident-response documentation.
- At least one broker-backed smoke test on each provider claimed as verified.

## Adoption Plan

**Days 0-30**

- Recruit five design partners running Kafka in production.
- Complete guided read-only installations with least-privilege policies.
- Measure time to first successful MCP tool call and first incident workflow.

**Days 31-60**

- Run at least three incident or operational exercises.
- Measure tool-call success rate, authorization denials, masking correctness,
  and operator intervention.
- Fix reliability or contract issues before adding product surface.

**Days 61-90**

- Convert repeated workflows into documented examples and evaluation fixtures.
- Ask whether approval-gated mutations solve a demonstrated workflow.
- Decide whether the evidence supports a v1.0 contract freeze.

## Success and Stop/Go Criteria

Proceed to v1.0 when all of the following are true:

- Three independent teams complete a production-representative trial.
- At least two teams use KafkaMCP repeatedly after initial setup.
- Supported tool calls achieve at least 99% success excluding expected policy
  denials and broker outages during the measured trial window.
- No critical security issue or unmasked sensitive output remains open.
- Upgrade and rollback are rehearsed on a production-representative deployment.

Do not expand into managed cloud, billing, marketplace, or SAML until three
customers request the managed offering and two commit to paid pilots. If design
partners do not retain the product after the 90-day trial, narrow the use case
further instead of restoring the old breadth-first roadmap.
