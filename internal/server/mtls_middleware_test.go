package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth/identity"
)

func mintLeaf(t *testing.T, cn string, uris []string) *x509.Certificate {
	t.Helper()
	priv, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	parsedURIs := make([]*url.URL, 0, len(uris))
	for _, u := range uris {
		p, err := url.Parse(u)
		if err != nil {
			t.Fatalf("parse %q: %v", u, err)
		}
		parsedURIs = append(parsedURIs, p)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn, Organization: []string{"acme"}},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		URIs:         parsedURIs,
	}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	parsed, _ := x509.ParseCertificate(der)
	return parsed
}

func TestMTLSIdentityMiddleware_AttachesIdentity(t *testing.T) {
	prov := identity.NewMTLS(identity.MTLSConfig{})
	s := &Server{
		Governance: Governance{identityProvider: prov},
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	var seen *identity.AgentIdentity
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ident, _ := identity.FromContext(r.Context())
		seen = ident
		w.WriteHeader(200)
	})
	cert := mintLeaf(t, "fallback-cn", []string{"spiffe://example.org/agents/alice"})
	req := httptest.NewRequest("POST", "/", nil)
	req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
	rec := httptest.NewRecorder()

	s.MTLSIdentityMiddleware(next).ServeHTTP(rec, req)

	if seen == nil {
		t.Fatal("expected identity attached to ctx")
	}
	if seen.ID != "spiffe://example.org/agents/alice" {
		t.Errorf("id = %q", seen.ID)
	}
	if rec.Code != 200 {
		t.Errorf("status = %d", rec.Code)
	}
}

func TestMTLSIdentityMiddleware_PassThroughWithoutCert(t *testing.T) {
	prov := identity.NewMTLS(identity.MTLSConfig{})
	s := &Server{
		Governance: Governance{identityProvider: prov},
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	var sawIdent bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, sawIdent = identity.FromContext(r.Context())
		w.WriteHeader(200)
	})
	req := httptest.NewRequest("POST", "/", nil) // no r.TLS
	rec := httptest.NewRecorder()

	s.MTLSIdentityMiddleware(next).ServeHTTP(rec, req)

	if sawIdent {
		t.Error("identity should not be attached when no TLS state is present")
	}
	if rec.Code != 200 {
		t.Errorf("status = %d, expected pass-through", rec.Code)
	}
}

func TestMTLSIdentityMiddleware_RejectsBadSPIFFE(t *testing.T) {
	prov := identity.NewMTLS(identity.MTLSConfig{
		AllowedSPIFFEIDs: []string{"spiffe://example.org/agents/alice"},
	})
	s := &Server{
		Governance: Governance{identityProvider: prov},
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})
	cert := mintLeaf(t, "x", []string{"spiffe://example.org/agents/mallory"})
	req := httptest.NewRequest("POST", "/", nil)
	req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
	rec := httptest.NewRecorder()

	s.MTLSIdentityMiddleware(next).ServeHTTP(rec, req)

	if called {
		t.Error("next handler should not be invoked when SPIFFE id is rejected")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

func TestMTLSIdentityMiddleware_NoOpForNonMTLSProvider(t *testing.T) {
	// Static provider does not implement RequestVerifier; middleware
	// should pass through untouched.
	prov, _ := identity.NewStatic([]identity.StaticToken{{Token: "x", AgentID: "alice"}})
	s := &Server{
		Governance: Governance{identityProvider: prov},
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	var sawIdent bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, sawIdent = identity.FromContext(r.Context())
		w.WriteHeader(200)
	})
	req := httptest.NewRequest("POST", "/", nil)
	rec := httptest.NewRecorder()

	s.MTLSIdentityMiddleware(next).ServeHTTP(rec, req)

	if sawIdent {
		t.Error("static provider must not attach identity via the mTLS middleware")
	}
	if rec.Code != 200 {
		t.Errorf("status = %d", rec.Code)
	}
}

func TestResolveAgentIdentity_PrefersContextIdentity(t *testing.T) {
	// Verify that an identity attached upstream (by the mTLS
	// middleware) is preferred over re-running Verify(headers).
	prov := identity.NewMTLS(identity.MTLSConfig{})
	s := &Server{
		Governance: Governance{identityProvider: prov},
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	preset := &identity.AgentIdentity{ID: "preset-agent", Method: "mtls"}
	ctx := identity.WithIdentity(context.Background(), preset)

	got, gotIdent := s.resolveAgentIdentity(ctx, http.Header{})
	if got != "preset-agent" {
		t.Errorf("agentID = %q, want preset-agent", got)
	}
	if gotIdent == nil || gotIdent.ID != "preset-agent" {
		t.Errorf("ident = %+v", gotIdent)
	}
}
