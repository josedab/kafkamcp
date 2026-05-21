// Package identity verifies caller identities (typically MCP agents) for
// KafkaMCP request handlers.
//
// Historically the server trusted whatever `X-Agent-ID` header the client
// sent. That was fine for laptop/dev usage but unacceptable in any
// multi-tenant deployment where the policy engine then matches on the
// agent ID. This package introduces a small, pluggable [Provider]
// abstraction so deployments can verify identities against:
//
//   - **none** (default): no verification, fall back to legacy
//     `X-Agent-ID` behaviour.
//   - **static**: deterministic mapping from a shared-secret bearer token
//     to a configured agent ID. Useful for self-hosted, single-tenant
//     setups that want a tiny upgrade path from headers-only.
//   - **oidc**: verify a JWT bearer token against a configured OIDC
//     issuer (Auth0, Okta, Keycloak, Azure AD, ...). The agent ID is
//     extracted from a configurable JWT claim (default `sub`) and
//     optional `groups` / `email` claims are surfaced for downstream
//     audit logging.
//
// All providers return a fully populated [AgentIdentity] on success, or
// [ErrNoCredentials] when the request did not carry verifiable
// credentials. Callers (the auth middleware) decide whether to fall back
// to legacy behaviour or reject the request.
package identity

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// AgentIdentity is the verified identity of the calling agent. All
// fields are best-effort; only ID is guaranteed to be non-empty when
// returned by [Provider.Verify].
type AgentIdentity struct {
	// ID is the stable agent identifier that the policy engine matches
	// against. For OIDC providers this is the value of the configured
	// claim (default `sub`).
	ID string

	// Method is the provider type that produced this identity
	// ("none", "static", "oidc"). Useful for audit logs.
	Method string

	// Subject is the JWT `sub` claim (oidc) or the static ID
	// (static/none).
	Subject string

	// Issuer is the JWT `iss` claim (oidc) or empty.
	Issuer string

	// Email is the optional email claim from the JWT.
	Email string

	// Groups is the optional groups claim from the JWT.
	Groups []string

	// Claims holds the raw JWT claims for advanced policy use
	// (intentionally a generic map; callers should type-assert).
	Claims map[string]any

	// VerifiedAt is the wall-clock time the identity was verified.
	VerifiedAt time.Time
}

// LogAttrs returns the identity as a set of key/value pairs suitable for
// passing to `slog` logging methods, with sensitive fields redacted.
func (i *AgentIdentity) LogAttrs() []any {
	if i == nil {
		return []any{"identity", "none"}
	}
	attrs := []any{
		"agent_id", i.ID,
		"identity_method", i.Method,
	}
	if i.Issuer != "" {
		attrs = append(attrs, "identity_issuer", i.Issuer)
	}
	if len(i.Groups) > 0 {
		attrs = append(attrs, "identity_groups", i.Groups)
	}
	return attrs
}

// Provider verifies request credentials and returns the caller's
// identity.
type Provider interface {
	// Name returns the provider type for logging and metrics.
	Name() string

	// Verify inspects headers (typically `Authorization: Bearer ...`)
	// and returns the verified [AgentIdentity]. Returns
	// [ErrNoCredentials] when the request did not carry verifiable
	// credentials, or an error describing the verification failure.
	Verify(ctx context.Context, headers http.Header) (*AgentIdentity, error)
}

// Sentinel errors returned by [Provider.Verify].
var (
	// ErrNoCredentials is returned when the request carries no
	// credentials that the provider knows how to verify.
	ErrNoCredentials = errors.New("identity: no verifiable credentials")

	// ErrInvalidToken is returned when a token is present but fails
	// verification (bad signature, expired, wrong audience, ...).
	ErrInvalidToken = errors.New("identity: invalid token")
)
