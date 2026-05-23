package server

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/kafkamcp/kafkamcp/internal/auth/identity"
)

// MTLSIdentityMiddleware returns an http.Handler that verifies the
// caller's mTLS peer certificate (when the configured identity
// Provider implements [identity.RequestVerifier]) and attaches the
// resulting [identity.AgentIdentity] to the request context via
// [identity.WithIdentity].
//
// When no client certificate is presented, the middleware passes the
// request through unchanged — header-based providers and the legacy
// X-Agent-ID path still work for non-mTLS callers.
//
// This middleware MUST be wrapped around the MCP HTTP handler AFTER
// the TLS terminator has run (so r.TLS is populated), but BEFORE the
// MCP request enters the handler so that downstream code reading
// [identity.FromContext] sees the verified identity.
func (s *Server) MTLSIdentityMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.identityProvider == nil {
			next.ServeHTTP(w, r)
			return
		}
		rv, ok := s.identityProvider.(identity.RequestVerifier)
		if !ok {
			next.ServeHTTP(w, r)
			return
		}
		ident, err := rv.VerifyRequest(r.Context(), r)
		switch {
		case err == nil && ident != nil && ident.ID != "":
			ctx := identity.WithIdentity(r.Context(), ident)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		case errors.Is(err, identity.ErrNoCredentials):
			// No client cert presented (or non-mTLS provider returned
			// ErrNoCredentials). Let header-based fallback handle it.
			next.ServeHTTP(w, r)
			return
		default:
			s.logger.Warn("mtls identity verification failed",
				"provider", s.identityProvider.Name(),
				"remote", r.RemoteAddr,
				"error", err)
			http.Error(w, "client certificate rejected", http.StatusUnauthorized)
			return
		}
	})
}

// BuildMTLSTLSConfig produces a *tls.Config that requests-and-verifies
// client certificates against the configured CA bundle. Returns
// (nil, nil) when the active provider does not need mTLS, signaling
// the caller to use its default *tls.Config.
//
// caBundlePath is consulted only when non-empty; otherwise the system
// trust store is used (in practice this means the deployment is
// trusting any CA known to the OS, which is rarely what you want — a
// warning is logged in that case).
func (s *Server) BuildMTLSTLSConfig(caBundlePath string) (*tls.Config, error) {
	if s.identityProvider == nil || s.identityProvider.Name() != "mtls" {
		return nil, nil
	}
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		ClientAuth: tls.RequireAndVerifyClientCert,
	}
	if caBundlePath != "" {
		pem, err := os.ReadFile(caBundlePath)
		if err != nil {
			return nil, fmt.Errorf("reading mtls ca_bundle %q: %w", caBundlePath, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("mtls ca_bundle %q contains no PEM certificates", caBundlePath)
		}
		cfg.ClientCAs = pool
	} else {
		s.logger.Warn("mtls enabled without policy.identity.mtls.ca_bundle; falling back to system trust pool")
	}
	return cfg, nil
}
