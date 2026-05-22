# ADR-009: mTLS Identity Provider

- Status: **Accepted**
- Date: 2026-05-04
- Supersedes: —
- Superseded by: —
- Related: [ADR-007 OIDC agent identity](./007-agent-identity-oidc.md)

## Context

[ADR-007](./007-agent-identity-oidc.md) introduced the `identity.Provider`
abstraction with three implementations: `none` (legacy header trust),
`static` (shared-secret bearer tokens), and `oidc` (JWT verification
against an OIDC issuer).

Service-mesh, SPIFFE, and zero-trust deployments already issue every
workload a short-lived x509 client certificate. Forcing those
deployments to *additionally* mint OIDC tokens to talk to KafkaMCP is
both redundant and weaker — the mTLS handshake has already
chain-verified the caller against the operator's CA, but the audit
log and policy engine still don't see the verified identity.

We therefore need a fourth provider that derives `AgentIdentity` from
the TLS peer certificate.

## Decision

Add an `mtls` identity provider.

Identity is extracted from the leaf certificate exposed on
`r.TLS.PeerCertificates[0]` in the following precedence order:

1. **SPIFFE URI SAN** (`spiffe://<trust-domain>/<path>`).
   - Subject to optional `allowed_trust_domains` and
     `allowed_spiffe_ids` allow-lists configured under
     `policy.identity.mtls`.
2. **Any other URI SAN** (e.g. `https://example.com/agents/billing`).
3. **Subject CN** — only when no URI SAN is present and
   `allow_cn_fallback` is true (the default; deployments running in
   SPIFFE-only mode set it to false).

Subject `OrganizationalUnit` values are surfaced as
`AgentIdentity.Groups` when `groups_from_ous: true`, preserving
existing policy-engine group bindings.

### Why the HTTP middleware, not the existing `Verify(headers)` path

The existing `Provider.Verify(ctx, http.Header)` signature only sees
the request headers, not the underlying `*http.Request`. mTLS
verification fundamentally needs access to `r.TLS.PeerCertificates`,
which is only available on the raw `*http.Request`. Two options were
considered:

1. **Widen the Provider interface** to take `*http.Request`. Rejected:
   breaks every existing provider and forces the static / OIDC
   providers to drag in HTTP-specific imports.
2. **Add an optional `RequestVerifier` interface** that providers can
   opt into. ✅ Chosen — header-only providers continue to work
   unchanged, and the HTTP layer type-asserts the active provider
   against `RequestVerifier` to discover whether it wants the full
   request.

The HTTP middleware (`Server.MTLSIdentityMiddleware`) runs before the
MCP handler, extracts the verified identity, and attaches it to the
request context via `identity.WithIdentity`. Downstream
`resolveAgentIdentity` checks `identity.FromContext(ctx)` first and
prefers the upstream-attached identity over re-running
`Verify(headers)`.

### Why `RequireAndVerifyClientCert`, not `VerifyClientCertIfGiven`

When `provider: mtls` is configured the HTTP transport rebuilds the
TLS config with `tls.RequireAndVerifyClientCert` and the configured
CA bundle. This means clients without a valid cert fail at the TLS
handshake, before any HTTP request is parsed — making it impossible
for an attacker to bypass mTLS by simply omitting the client cert and
relying on the legacy `X-Agent-ID` header fallback. mTLS deployments
explicitly do **not** support header fallback.

When `policy.identity.mtls.ca_bundle` is empty the system trust pool
is used and a WARN is logged at server start — this is rarely the
desired configuration in production but is convenient for laptop
development against an OS-installed CA.

## Consequences

### Positive

- Service-mesh-style deployments (Istio, Linkerd, Consul Connect) and
  SPIFFE/SPIRE setups can authenticate KafkaMCP callers using the
  identities they already have, with no token-minting layer in
  between.
- The verified identity (SPIFFE ID, CN, OUs) lands in the audit log
  via the FU1 `Subject`/`Issuer`/`IdentityMethod` fields with no
  additional wiring.
- The `RequestVerifier` extension point is generic — future providers
  that need request-level context (e.g. AWS SigV4, header-asserted
  identity propagated by a service-mesh sidecar) plug into the same
  hook.

### Negative

- mTLS provider does not participate in the bearer-token
  `Verify(headers)` path; its `Verify` always returns
  `ErrNoCredentials`. Operators who try to mix mTLS with bearer
  tokens on the same listener will be surprised — documented in
  [docs/AGENT_IDENTITY.md](../AGENT_IDENTITY.md).
- mTLS deployments must use one of the HTTP transports
  (`streamable-http`, `sse`). STDIO transport has no TLS layer and
  therefore no peer cert; the provider is a no-op there. (Bearer
  token providers have the same limitation.)
- Self-signed leaf certs work, but **only** because the TLS terminator
  is configured to trust their issuer. The provider itself does **not**
  re-validate the chain — that is the TLS layer's job.

## Alternatives considered

- **Forward verified identity in a header from a sidecar** (e.g.
  `X-Forwarded-Client-Cert`). Rejected as the default: header
  spoofing is the original problem ADR-007 set out to solve. The
  `MTLSProvider.IdentityFromCert` helper *is* exposed so a future
  ADR can build a "trusted sidecar" provider on top of it, but the
  default path requires KafkaMCP to terminate TLS itself.
- **OPA / Cedar policy on the cert** instead of an allow-list.
  Rejected for v1 — simple allow-lists cover the 90% case; OPA
  integration is tracked separately.
