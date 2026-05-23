package identity

import (
	"context"
	"net/http"
	"time"
)

// NoneProvider is the default identity provider used when no provider
// is configured. It always returns [ErrNoCredentials]; the caller (the
// auth middleware) then falls back to legacy `X-Agent-ID` header
// behavior.
type NoneProvider struct{}

// NewNone returns a NoneProvider.
func NewNone() *NoneProvider { return &NoneProvider{} }

// Name implements Provider.
func (*NoneProvider) Name() string { return "none" }

// Verify implements Provider. It never succeeds.
func (*NoneProvider) Verify(_ context.Context, _ http.Header) (*AgentIdentity, error) {
	return nil, ErrNoCredentials
}

// StaticProvider verifies requests by checking an `Authorization: Bearer
// <token>` header against an in-memory map of token → agent ID. It is
// intended for self-hosted, single-tenant deployments that need
// something better than client-supplied X-Agent-ID headers without
// running an OIDC provider.
//
// The token-to-identity map is loaded once at construction; rotating
// tokens requires restarting the server (or a future hot-reload story).
type StaticProvider struct {
	tokens map[string]*AgentIdentity
}

// StaticToken describes a single shared-secret token recognized by the
// static provider.
type StaticToken struct {
	// Token is the bearer token value (must be non-empty and unique).
	Token string

	// AgentID is the identifier the policy engine will match against.
	AgentID string

	// Groups are arbitrary group labels surfaced on the verified
	// AgentIdentity (optional).
	Groups []string
}

// NewStatic constructs a static provider. Duplicate tokens are rejected
// at construction time.
func NewStatic(tokens []StaticToken) (*StaticProvider, error) {
	m := make(map[string]*AgentIdentity, len(tokens))
	for _, t := range tokens {
		if t.Token == "" {
			return nil, &ConfigError{Field: "token", Message: "static token must not be empty"}
		}
		if t.AgentID == "" {
			return nil, &ConfigError{Field: "agent_id", Message: "static token agent_id must not be empty"}
		}
		if _, dup := m[t.Token]; dup {
			return nil, &ConfigError{Field: "token", Message: "duplicate static token"}
		}
		m[t.Token] = &AgentIdentity{
			ID:      t.AgentID,
			Method:  "static",
			Subject: t.AgentID,
			Groups:  append([]string(nil), t.Groups...),
		}
	}
	return &StaticProvider{tokens: m}, nil
}

// Name implements Provider.
func (*StaticProvider) Name() string { return "static" }

// Verify implements Provider.
func (p *StaticProvider) Verify(_ context.Context, h http.Header) (*AgentIdentity, error) {
	tok := bearerToken(h)
	if tok == "" {
		return nil, ErrNoCredentials
	}
	ident, ok := p.tokens[tok]
	if !ok {
		return nil, ErrInvalidToken
	}
	// Clone so callers cannot mutate our entry.
	out := *ident
	out.VerifiedAt = time.Now()
	if ident.Groups != nil {
		out.Groups = append([]string(nil), ident.Groups...)
	}
	return &out, nil
}

// ConfigError describes an invalid identity-provider configuration.
type ConfigError struct {
	Field   string
	Message string
}

func (e *ConfigError) Error() string {
	return "identity: " + e.Field + ": " + e.Message
}
