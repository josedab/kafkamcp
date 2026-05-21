package identity

import (
	"context"
	"fmt"
)

// Config is the operator-facing identity configuration unmarshalled
// from kafkamcp.yaml. It is converted to a concrete [Provider] via
// [Build].
type Config struct {
	// Provider selects the verification method: "none" (default),
	// "static", or "oidc".
	Provider string

	// Static is consulted when Provider == "static".
	Static StaticConfig

	// OIDC is consulted when Provider == "oidc".
	OIDC OIDCConfig
}

// StaticConfig holds the inline token table for the static provider.
type StaticConfig struct {
	Tokens []StaticToken
}

// Build instantiates the [Provider] selected by cfg. ctx bounds the
// (possibly remote) initialisation work (e.g. OIDC discovery).
func Build(ctx context.Context, cfg Config) (Provider, error) {
	switch cfg.Provider {
	case "", "none", "disabled":
		return NewNone(), nil
	case "static":
		return NewStatic(cfg.Static.Tokens)
	case "oidc":
		return NewOIDC(ctx, cfg.OIDC)
	default:
		return nil, fmt.Errorf("identity: unknown provider %q (expected none|static|oidc)", cfg.Provider)
	}
}
