# Agent Identity & OIDC Verification

KafkaMCP can now **verify** the identity of the calling agent instead of
trusting whatever `X-Agent-ID` header the client sends. This document
explains the providers, configuration, and how the resolved identity
flows through the request lifecycle.

> See also: [ADR-007: Agent Identity & OIDC Verification](./adr/007-agent-identity-oidc.md).

## Why this exists

The policy engine (`internal/policy/`) and audit log (`internal/audit/`)
both key off **agent IDs**. Before this feature, the agent ID came from
the `X-Agent-ID` header — a string the client picks at will. In a
multi-tenant deployment that means any caller could impersonate any
agent and bypass policy. The identity provider verifies the caller
against a trusted source before the policy engine ever sees the ID.

## Provider overview

| Provider | Verification source | Best for |
| --- | --- | --- |
| `none` *(default)* | None — falls back to `X-Agent-ID` header. | Local dev, demos, single-tenant trusted networks. |
| `static` | Shared-secret bearer tokens defined in config. | Self-hosted single-tenant; tiny upgrade from headers. |
| `oidc` | JWT bearer tokens verified against an OIDC issuer's JWKS. | Multi-tenant, cloud, SSO-managed deployments. |

When a provider is configured but the request does not carry a token,
KafkaMCP **falls back** to the legacy `X-Agent-ID` header. When a token
is present but invalid, the request is treated as **anonymous** and the
failure is logged with the provider name and error class (the token
itself is never logged).

## Configuration

The identity block lives under `policy.identity` in `kafkamcp.yaml`:

```yaml
policy:
  enabled: true
  identity:
    provider: oidc      # none | static | oidc
    static:
      tokens:
        - token: "supersecret"
          agent_id: "ci-runner"
        - token: "anothersecret"
          agent_id: "scheduler"
    oidc:
      issuer: "https://auth.example.com/realms/agents"
      audience: "kafkamcp"
      agent_id_claim: "sub"        # default: sub
      groups_claim: "groups"       # optional
      email_claim: "email"         # optional
      # Optional: explicit JWKS URL when the issuer's discovery
      # document is unreachable from KafkaMCP (e.g. internal-only).
      # jwks_url: "https://auth.example.com/realms/agents/protocol/openid-connect/certs"
```

### Issuer-specific examples

#### Auth0

```yaml
oidc:
  issuer: "https://YOUR_TENANT.auth0.com/"
  audience: "https://kafkamcp.example.com"
  agent_id_claim: "sub"
  groups_claim: "https://kafkamcp.example.com/groups"
```

Auth0 namespaces custom claims; configure a Rule/Action that writes the
agent ID to `sub` (default) or to a custom namespaced claim and update
`agent_id_claim` accordingly.

#### Okta

```yaml
oidc:
  issuer: "https://YOUR_ORG.okta.com/oauth2/default"
  audience: "api://kafkamcp"
  agent_id_claim: "cid"     # client_id (service-to-service)
  groups_claim: "groups"
```

For service-to-service flows we recommend `cid` (client ID) so each
service agent has a stable ID independent of the user it acts on behalf
of.

#### Keycloak

```yaml
oidc:
  issuer: "https://keycloak.example.com/realms/agents"
  audience: "kafkamcp"
  agent_id_claim: "preferred_username"
  groups_claim: "groups"
```

Add a Keycloak **client mapper** that puts the realm/client roles into
the `groups` claim.

#### Azure AD / Entra ID

```yaml
oidc:
  issuer: "https://login.microsoftonline.com/TENANT_ID/v2.0"
  audience: "api://kafkamcp"
  agent_id_claim: "oid"     # immutable object ID
  groups_claim: "groups"
  email_claim: "preferred_username"
```

`oid` is preferred over `sub` because it survives token-format changes
and is stable across all of a user's Azure AD logins.

## Precedence

For every incoming MCP request the middleware applies the following
order:

1. **Identity provider** — if configured and the request carries an
   `Authorization: Bearer ...` header, the provider verifies the token
   and returns an `AgentIdentity`. On success the identity's `ID` is
   used as the agent ID and the full identity is attached to the request
   context so downstream handlers (audit, policy, rate-limits) can read
   it.
2. **`X-Agent-ID` header** — if no token was present (`ErrNoCredentials`
   from the provider) the legacy header is used.
3. **`X-Client-ID` header** — for backwards compatibility.
4. **`anonymous`** — used when none of the above applies.

A failed verification (bad signature, expired, wrong audience) is
**never** silently downgraded to anonymous-via-header; it is logged at
WARN with the provider name and the agent ID becomes `anonymous`, which
the policy engine treats as untrusted.

## Bearer token format

KafkaMCP accepts a case-insensitive `Authorization: Bearer <token>`
header. The scheme matching is liberal: `Bearer`, `bearer`, `BEARER`
all work. Any other scheme (Basic, Digest, ...) is ignored — the
identity provider returns `ErrNoCredentials` and the middleware falls
back to header-based behaviour.

For SSE/HTTP transports the headers are taken from the underlying HTTP
request. For STDIO transports there is no header surface and the
provider always returns `ErrNoCredentials`; STDIO deployments must
either use the legacy header behaviour or run KafkaMCP behind a proxy
that injects the headers.

## Custom claim handling

- **`agent_id_claim`** — the JWT claim that the policy engine sees as
  the agent ID. Defaults to `sub`. If the configured claim is missing or
  empty in the token, the provider falls back to `sub` (and logs a
  WARN).
- **`groups_claim`** — accepts a JSON array of strings, a single string
  (single group), or `[]any` (JSON-decoded). Used by ABAC policies.
- **`email_claim`** — purely informational; surfaced in audit logs.

## Verifying your setup

Two CLI helpers are useful while testing:

```bash
# 1. Confirm the issuer discovery doc loads.
curl -fsS https://auth.example.com/realms/agents/.well-known/openid-configuration | jq .

# 2. Confirm KafkaMCP accepts a real token.
TOKEN=$(curl -fsS -X POST -d "..." https://auth.example.com/token | jq -r .access_token)
curl -fsS \
  -H "Authorization: Bearer ${TOKEN}" \
  http://localhost:9091/mcp \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

On the server side, set `logging.level: debug` to see per-request
provider decisions:

```
DEBUG identity verified provider=oidc agent_id=ci-runner issuer=https://auth.example.com/...
WARN  agent identity verification failed provider=oidc error="oidc: token is expired"
```

## Limitations & follow-ups

- **Audit log enrichment** — done. Each audit entry now carries
  `identity_method`, `issuer`, and `subject` populated from the verified
  identity attached to the request context (see
  [`audit.Entry`](../internal/audit/logger.go)). Header-asserted callers
  (legacy `X-Agent-ID`, STDIO transport, `provider: none`) leave those
  fields empty, which downstream audit consumers can use to distinguish
  trusted from un-trusted identities.
- **mTLS / SPIFFE** — not implemented in this iteration. ADR-007 lays
  out why we picked OIDC first. mTLS will be added as a fourth provider
  when the connection-level plumbing is in place.
- **Token caching / introspection** — the OIDC provider verifies the
  signature locally against cached JWKS but does not call the
  introspection endpoint. For very short-lived tokens or revocation
  scenarios use short OIDC `exp` values.

## Audit-log identity fields

When the identity provider verifies a request, the resulting
[`audit.Entry`](../internal/audit/logger.go) is enriched with:

| Field | Value when verified | Value when un-verified |
| --- | --- | --- |
| `identity_method` | `static` / `oidc` / `mtls` (etc.) | empty |
| `issuer` | `iss` claim (OIDC) or empty (other providers) | empty |
| `subject` | `sub` claim (OIDC), agent ID (static), URI/CN (mTLS) | empty |
| `agent_id` | value of the configured `agent_id_claim` (defaults to `sub`) | header-provided value or `anonymous` |

Audit consumers can therefore distinguish a trusted call (any non-empty
`identity_method`) from a legacy header-asserted call (empty
`identity_method`).

## Related docs

- [ADR-007: Agent Identity & OIDC Verification](./adr/007-agent-identity-oidc.md)
- [`internal/auth/identity/`](../internal/auth/identity/) — package source
- [`SECURITY.md`](../SECURITY.md) — broader threat model
- [`docs/POLICY.md`](POLICY.md) — how the policy engine consumes agent IDs
