package identity

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

// OIDCConfig describes how the [OIDCProvider] verifies bearer tokens.
type OIDCConfig struct {
	// IssuerURL is the OIDC issuer (e.g. `https://example.auth0.com/`).
	// Required. The provider performs discovery against
	// `<IssuerURL>/.well-known/openid-configuration` at construction.
	IssuerURL string

	// ClientID is the audience the token must contain in its `aud`
	// claim. Required.
	ClientID string

	// AgentIDClaim names the JWT claim to use as the agent ID
	// (matched against the policy engine). Defaults to "sub".
	AgentIDClaim string

	// GroupsClaim names the JWT claim that carries the agent's group
	// memberships. Defaults to "groups". Set to "" to disable group
	// extraction.
	GroupsClaim string

	// EmailClaim names the JWT claim carrying the user's email.
	// Defaults to "email".
	EmailClaim string

	// SkipExpiryCheck disables the token expiry check. Strongly
	// discouraged outside tests.
	SkipExpiryCheck bool

	// SkipClientIDCheck disables the audience (aud) check. Strongly
	// discouraged outside tests.
	SkipClientIDCheck bool
}

// OIDCProvider verifies JWT bearer tokens against an OIDC issuer's JWKS.
//
// Discovery is performed once at construction; the underlying
// go-oidc.RemoteKeySet handles JWKS caching and rotation.
type OIDCProvider struct {
	cfg      OIDCConfig
	verifier *oidc.IDTokenVerifier
}

// NewOIDC constructs an OIDC provider. It performs OIDC discovery
// against cfg.IssuerURL using ctx; the ctx deadline (if any) bounds the
// discovery HTTP call.
func NewOIDC(ctx context.Context, cfg OIDCConfig) (*OIDCProvider, error) {
	if strings.TrimSpace(cfg.IssuerURL) == "" {
		return nil, &ConfigError{Field: "issuer_url", Message: "must not be empty"}
	}
	if strings.TrimSpace(cfg.ClientID) == "" {
		return nil, &ConfigError{Field: "client_id", Message: "must not be empty"}
	}
	if cfg.AgentIDClaim == "" {
		cfg.AgentIDClaim = "sub"
	}
	if cfg.GroupsClaim == "" {
		cfg.GroupsClaim = "groups"
	}
	if cfg.EmailClaim == "" {
		cfg.EmailClaim = "email"
	}

	provider, err := oidc.NewProvider(ctx, cfg.IssuerURL)
	if err != nil {
		return nil, fmt.Errorf("identity: oidc discovery for %s: %w", cfg.IssuerURL, err)
	}
	verifier := provider.Verifier(&oidc.Config{
		ClientID:          cfg.ClientID,
		SkipClientIDCheck: cfg.SkipClientIDCheck,
		SkipExpiryCheck:   cfg.SkipExpiryCheck,
	})
	return &OIDCProvider{cfg: cfg, verifier: verifier}, nil
}

// newOIDCWithVerifier exists for tests so we don't need a real issuer.
func newOIDCWithVerifier(cfg OIDCConfig, v *oidc.IDTokenVerifier) *OIDCProvider {
	if cfg.AgentIDClaim == "" {
		cfg.AgentIDClaim = "sub"
	}
	if cfg.GroupsClaim == "" {
		cfg.GroupsClaim = "groups"
	}
	if cfg.EmailClaim == "" {
		cfg.EmailClaim = "email"
	}
	return &OIDCProvider{cfg: cfg, verifier: v}
}

// Name implements Provider.
func (*OIDCProvider) Name() string { return "oidc" }

// Verify implements Provider.
func (p *OIDCProvider) Verify(ctx context.Context, h http.Header) (*AgentIdentity, error) {
	tok := bearerToken(h)
	if tok == "" {
		return nil, ErrNoCredentials
	}
	idToken, err := p.verifier.Verify(ctx, tok)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}

	var claims map[string]any
	if err := idToken.Claims(&claims); err != nil {
		return nil, fmt.Errorf("identity: decode oidc claims: %w", err)
	}

	agentID, _ := claims[p.cfg.AgentIDClaim].(string)
	if agentID == "" {
		// Fallback to sub if the configured claim is missing — better
		// to log a verified identity under sub than to drop into
		// anonymous mode silently.
		agentID = idToken.Subject
	}
	if agentID == "" {
		return nil, errors.New("identity: oidc token has no usable agent identifier")
	}

	email, _ := claims[p.cfg.EmailClaim].(string)
	groups := extractStringSlice(claims[p.cfg.GroupsClaim])

	return &AgentIdentity{
		ID:         agentID,
		Method:     "oidc",
		Subject:    idToken.Subject,
		Issuer:     idToken.Issuer,
		Email:      email,
		Groups:     groups,
		Claims:     claims,
		VerifiedAt: time.Now(),
	}, nil
}

// bearerToken extracts the bearer token from the Authorization header.
// It returns "" when the header is absent or malformed.
func bearerToken(h http.Header) string {
	if h == nil {
		return ""
	}
	v := strings.TrimSpace(h.Get("Authorization"))
	if v == "" {
		return ""
	}
	const prefix = "bearer "
	if len(v) <= len(prefix) {
		return ""
	}
	if !strings.EqualFold(v[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(v[len(prefix):])
}

// extractStringSlice converts a claim value into a string slice,
// tolerating both real []any (the typical JSON unmarshal shape) and the
// occasional single-string claim.
func extractStringSlice(v any) []string {
	switch t := v.(type) {
	case nil:
		return nil
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			if s, ok := item.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	case []string:
		return append([]string(nil), t...)
	case string:
		if t == "" {
			return nil
		}
		return []string{t}
	default:
		return nil
	}
}
