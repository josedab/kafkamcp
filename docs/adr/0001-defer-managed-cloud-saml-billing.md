# ADR 0001: Defer managed cloud, SSO/SAML, and billing

- **Status:** Accepted
- **Date:** 2026-07-26
- **Deciders:** KafkaMCP maintainers
- **Supersedes:** the pre-reset roadmap's "v1.0: Cloud managed service, enterprise features (SSO)" goal

## Context

The pre-reset roadmap treated a managed cloud service, enterprise SSO/SAML, and
billing/marketplace as near-term v1 goals. An audit found that the existing
control-plane, billing, and marketplace surfaces were **in-memory simulations**
with no real backing service, and that they were being presented as production
capabilities. Building and operating a real managed control plane, identity
federation, and billing system is a large, ongoing investment with significant
security, compliance, and operational surface area.

At the same time, the differentiated value of KafkaMCP is a **secure,
vendor-neutral Kafka operations gateway** with governance and approval controls.
That value can be delivered and validated entirely in the self-hosted binary
without a control plane.

## Decision

We **defer** managed cloud, enterprise SSO/SAML, and billing/marketplace
functionality. Concretely:

1. These capabilities are **non-goals** for the current roadmap and are removed
   from public-facing production claims.
2. The server handlers, tool registrations, and `ExperimentalConfig` fields for
   control-plane, marketplace/plugin, agent-store, and cloud-deploy surfaces have
   been **deleted from the binary**. They are deferred architecture only — not
   runtime stubs — and cannot be re-enabled by any config flag.
3. The production binary must not depend on, or leak, control-plane/billing code
   paths.

## Evidence required to revisit

A managed cloud or enterprise service will only be built when **all** of the
following hold:

- **Three independent customers** explicitly request a managed/hosted offering,
  and
- **Two of them commit to paid pilots.**

SSO/SAML will be revisited when a committed pilot customer requires it for a
concrete deployment (OIDC and mTLS already cover the near-term identity needs).

## Future architecture (non-binding sketch)

If the gate is met, a managed offering would be a **separate** service that
embeds the existing self-hosted server as a library and adds, as independent
components:

- A stateless control-plane API for provisioning and configuration.
- Identity federation (OIDC/SAML) layered on the existing identity providers.
- A metering/billing pipeline fed by the existing opt-in activation metrics.
- Per-tenant isolation on top of the existing policy/approval/audit primitives.

None of this leaks into the current production binary; the self-hosted server
remains fully functional and independently deployable.

## Consequences

- **Positive:** smaller, more trustworthy production surface; no fabricated SaaS
  claims; engineering focus on the governance/approval wedge; clear, auditable
  criteria for when to invest in cloud.
- **Negative:** no managed offering in the near term; enterprises requiring
  hosted SSO must self-host with OIDC/mTLS until the gate is met.
