# ADR-007: Agent Identity & OIDC Verification

## Status

Accepted

## Context

KafkaMCP's policy engine and audit log key off the **agent ID** of the
caller. Until this ADR the agent ID came from an `X-Agent-ID` HTTP
header that the client could set to any string. In any multi-tenant
deployment this meant:

- **Impersonation** — agent `customer-A` could send `X-Agent-ID:
  customer-B` and inherit `customer-B`'s policy bindings.
- **Audit-log unforgeability** — the audit trail records the header
  value, so a compromised client could attribute its actions to another
  agent.
- **Rate-limit / quota evasion** — quotas are agent-scoped; an attacker
  could cycle through many agent IDs to bypass them.

We need a way for the operator to **plug in a trusted identity source**
without forcing it on the (large) population of single-tenant /
laptop / demo deployments where the header behaviour is fine.

## Decision

Introduce a pluggable `identity.Provider` interface and ship three
implementations:

| Provider | Used when |
| --- | --- |
| `none` (default) | No verification configured. Existing deployments keep working. |
| `static` | Shared-secret bearer tokens → fixed agent IDs. Smallest viable upgrade. |
| `oidc` | JWT bearer tokens verified against an OIDC issuer's JWKS. |

The provider runs inside the existing auth middleware (`withToolAuth`,
`withResourceAuth`) **before** the policy engine sees the agent ID. The
resolved identity is attached to the request context so downstream
handlers (audit, rate-limit, policy) can read it.

### Why OIDC first (not mTLS / SPIFFE)

- **Adoption surface** — every cloud IdP (Okta, Auth0, Azure AD,
  Cognito, Keycloak) speaks OIDC. mTLS requires per-deployment PKI work
  before the first agent can authenticate.
- **Reuses existing transport** — KafkaMCP already exposes HTTP/SSE
  endpoints; tokens flow through the same `Authorization: Bearer`
  header path that every SDK already supports. mTLS would require
  reworking the transport plumbing and the SDK.
- **Tooling parity** — the same JWT a customer uses for their own
  product can be reused for KafkaMCP without a separate cert lifecycle.
- **mTLS is not foreclosed** — the `Provider` interface is identity-source
  agnostic. Adding `internal/auth/identity/mtls.go` as a fourth
  provider is a follow-up issue once the connection-level cert plumbing
  is in place.

### Library choice: `github.com/coreos/go-oidc/v3`

- Maintained by the CoreOS / Red Hat team, used by Kubernetes.
- Verifies JWTs against a discovery-document-derived JWKS with
  background key rotation.
- No cgo. No surprise transitive deps beyond `go-jose/v4` and
  `golang.org/x/oauth2`.
- Permissive Apache-2.0 license matching the project.

We rejected DIY JWT verification (too easy to get signature/audience
checks wrong) and heavyweight frameworks like `lestrrat-go/jwx` (we
only need the verification subset).

### Configuration shape

```yaml
policy:
  identity:
    provider: oidc           # none | static | oidc
    static:
      tokens:
        - { token: "...", agent_id: "..." }
    oidc:
      issuer: "https://..."
      audience: "kafkamcp"
      agent_id_claim: "sub"       # default sub
      groups_claim: "groups"      # optional
      email_claim: "email"        # optional
      jwks_url: ""                # optional override
```

Validation runs in `config.Config.validate()`:
- `provider` must be in {none, static, oidc}.
- `static` requires at least one `(token, agent_id)` pair with non-empty values.
- `oidc` requires non-empty `issuer` and `audience`.

### Request flow

1. MCP request arrives at a tool/resource handler wrapped by
   `withToolAuth` / `withResourceAuth`.
2. `s.resolveAgentIdentity(ctx, header)` runs the configured provider:
   - Returns `(verifiedID, *AgentIdentity)` on success.
   - Returns `(headerFallback, nil)` when the provider reports
     `ErrNoCredentials` (no bearer token at all).
   - Returns `("anonymous", nil)` and logs a WARN when verification
     fails (bad signature, expired, wrong audience).
3. The verified `*AgentIdentity` is attached to `ctx` via
   `withIdentity()` so audit/policy can read it via
   `identityFromContext(ctx)`.
4. The agent ID is then passed to the policy engine and rate-limiter as
   before.

## Consequences

### Positive

- **Multi-tenancy unblocked** — operators can now safely host shared
  KafkaMCP instances with per-tenant agent IDs that cannot be forged.
- **Audit trail is trustworthy** — when OIDC is enabled the audit log's
  agent ID is bound to a verified JWT subject.
- **Zero migration cost for existing users** — `provider: none` is the
  default and preserves bit-for-bit behaviour.
- **Future-proof** — `Provider` interface admits mTLS, SPIFFE, OAuth
  introspection, and custom verifiers without touching call sites.

### Negative

- **New dependency** — `go-oidc/v3` + `go-jose/v4` + `oauth2` add ~600
  KB to the binary and a small attack surface.
- **Per-tool audit-entry enrichment is not yet wired** — the schema is
  ready (`audit.Entry.IdentityMethod`, `Issuer`) but ~100 `s.logAudit`
  call sites would need a `ctx` parameter to populate them. Deferred to
  a follow-up to keep this ADR's commit reviewable.
- **STDIO transport lacks headers** — STDIO callers cannot use the OIDC
  provider; they must rely on the legacy header behaviour or run
  KafkaMCP behind an HTTP transport.
- **No introspection / revocation** — short token TTLs are the only
  revocation knob for now.

### Neutral

- The new package lives at `internal/auth/identity/` rather than
  `internal/auth/` to leave room for related sub-packages (mtls,
  introspection) without further refactor.
- Identity is logged via `slog` attrs from `(*AgentIdentity).LogAttrs()`
  which intentionally omits the email by default (it's available in
  `entry.Email` for callers that want it).

## Alternatives considered

1. **Header HMAC** — sign the `X-Agent-ID` header with a shared secret.
   Rejected: requires per-agent secret distribution, no SSO benefit, no
   key rotation.
2. **mTLS first** — verify client certificates. Rejected for now: too
   much infra burden compared with OIDC for early adopters; will be
   added as a fourth provider once the transport plumbing supports it.
3. **OAuth introspection** — call the IdP's `/introspect` on every
   request. Rejected: extra latency on every call, single point of
   failure. JWT signature verification + reasonable token TTL covers
   the same threat model with much less coupling.
4. **JWT validation by hand** — write it ourselves. Rejected:
   verification edge cases (algorithm confusion, audience-array vs
   string, leeway) are exactly where DIY JWT impls go wrong.

## References

- [`docs/AGENT_IDENTITY.md`](../AGENT_IDENTITY.md) — operator-facing guide
- [`internal/auth/identity/`](../../internal/auth/identity/) — package source
- [go-oidc](https://pkg.go.dev/github.com/coreos/go-oidc/v3/oidc)
- [RFC 7519: JSON Web Token](https://datatracker.ietf.org/doc/html/rfc7519)
- [RFC 8414: OAuth 2.0 Authorization Server Metadata](https://datatracker.ietf.org/doc/html/rfc8414)
- [ADR-006](./006-persistent-engine-store.md) — adjacent durability work
