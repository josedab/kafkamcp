package identity

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net/http"
	"net/url"
	"testing"
	"time"
)

// makeCert mints a self-signed leaf cert with the given URI SANs / CN
// / OUs. Returns the *x509.Certificate (already parsed) ready to be
// placed in r.TLS.PeerCertificates.
func makeCert(t *testing.T, cn string, ous []string, uris []string) *x509.Certificate {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	uriList := make([]*url.URL, 0, len(uris))
	for _, u := range uris {
		parsed, err := url.Parse(u)
		if err != nil {
			t.Fatalf("parse uri %q: %v", u, err)
		}
		uriList = append(uriList, parsed)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			CommonName:         cn,
			OrganizationalUnit: ous,
			Organization:       []string{"acme"},
		},
		Issuer:    pkix.Name{CommonName: "test-ca"},
		NotBefore: time.Now().Add(-1 * time.Hour),
		NotAfter:  time.Now().Add(1 * time.Hour),
		KeyUsage:  x509.KeyUsageDigitalSignature,
		URIs:      uriList,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("ParseCertificate: %v", err)
	}
	return parsed
}

func reqWithCert(cert *x509.Certificate) *http.Request {
	r := &http.Request{TLS: &tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}}
	return r
}

func TestMTLS_SPIFFEPrecedence(t *testing.T) {
	p := NewMTLS(MTLSConfig{})
	cert := makeCert(t, "fallback-cn", nil, []string{
		"spiffe://example.org/agents/alice",
		"https://example.com/alice",
	})
	id, err := p.VerifyRequest(context.Background(), reqWithCert(cert))
	if err != nil {
		t.Fatalf("VerifyRequest: %v", err)
	}
	if id.ID != "spiffe://example.org/agents/alice" {
		t.Errorf("id = %q, want SPIFFE id", id.ID)
	}
	if id.Method != "mtls" {
		t.Errorf("method = %q", id.Method)
	}
	if id.Subject != "fallback-cn" {
		t.Errorf("subject = %q", id.Subject)
	}
	// self-signed: issuer CN should equal subject CN
	if id.Issuer != "fallback-cn" {
		t.Errorf("issuer = %q", id.Issuer)
	}
}

func TestMTLS_URISANFallback(t *testing.T) {
	p := NewMTLS(MTLSConfig{})
	cert := makeCert(t, "fallback-cn", nil, []string{"https://example.com/billing"})
	id, err := p.VerifyRequest(context.Background(), reqWithCert(cert))
	if err != nil {
		t.Fatalf("VerifyRequest: %v", err)
	}
	if id.ID != "https://example.com/billing" {
		t.Errorf("id = %q, want URI SAN", id.ID)
	}
}

func TestMTLS_CNFallback(t *testing.T) {
	p := NewMTLS(MTLSConfig{AllowCNFallback: true})
	cert := makeCert(t, "billing-agent", nil, nil)
	id, err := p.VerifyRequest(context.Background(), reqWithCert(cert))
	if err != nil {
		t.Fatalf("VerifyRequest: %v", err)
	}
	if id.ID != "billing-agent" {
		t.Errorf("id = %q", id.ID)
	}
}

func TestMTLS_CNFallbackDisabledWhenAllowlistSet(t *testing.T) {
	p := NewMTLS(MTLSConfig{
		AllowedSPIFFEIDs: []string{"spiffe://example.org/agents/alice"},
		AllowCNFallback:  false,
	})
	cert := makeCert(t, "billing-agent", nil, nil)
	_, err := p.VerifyRequest(context.Background(), reqWithCert(cert))
	if err == nil {
		t.Fatal("expected error when CN fallback is disabled and no URI SAN present")
	}
	if !errors.Is(err, ErrInvalidToken) {
		t.Errorf("err = %v, want ErrInvalidToken", err)
	}
}

func TestMTLS_AllowedSPIFFEIDs(t *testing.T) {
	p := NewMTLS(MTLSConfig{
		AllowedSPIFFEIDs: []string{"spiffe://example.org/agents/alice"},
	})
	good := makeCert(t, "x", nil, []string{"spiffe://example.org/agents/alice"})
	if _, err := p.VerifyRequest(context.Background(), reqWithCert(good)); err != nil {
		t.Errorf("good cert rejected: %v", err)
	}
	bad := makeCert(t, "x", nil, []string{"spiffe://example.org/agents/mallory"})
	if _, err := p.VerifyRequest(context.Background(), reqWithCert(bad)); err == nil {
		t.Error("bad spiffe id was accepted")
	}
}

func TestMTLS_AllowedTrustDomains(t *testing.T) {
	p := NewMTLS(MTLSConfig{
		AllowedTrustDomains: []string{"example.org"},
	})
	good := makeCert(t, "x", nil, []string{"spiffe://example.org/agents/alice"})
	if _, err := p.VerifyRequest(context.Background(), reqWithCert(good)); err != nil {
		t.Errorf("good trust domain rejected: %v", err)
	}
	bad := makeCert(t, "x", nil, []string{"spiffe://other-domain.test/agents/alice"})
	if _, err := p.VerifyRequest(context.Background(), reqWithCert(bad)); err == nil {
		t.Error("bad trust domain accepted")
	}
}

func TestMTLS_GroupsFromOUs(t *testing.T) {
	p := NewMTLS(MTLSConfig{GroupsFromOUs: true})
	cert := makeCert(t, "agent-billing", []string{"finance", "billing-team"}, nil)
	id, err := p.VerifyRequest(context.Background(), reqWithCert(cert))
	if err != nil {
		t.Fatalf("VerifyRequest: %v", err)
	}
	if len(id.Groups) != 2 {
		t.Fatalf("groups = %v, want 2", id.Groups)
	}
}

func TestMTLS_NoPeerCertReturnsErrNoCredentials(t *testing.T) {
	p := NewMTLS(MTLSConfig{})
	// no TLS
	if _, err := p.VerifyRequest(context.Background(), &http.Request{}); !errors.Is(err, ErrNoCredentials) {
		t.Errorf("no TLS: got %v, want ErrNoCredentials", err)
	}
	// TLS but no client certs
	r := &http.Request{TLS: &tls.ConnectionState{}}
	if _, err := p.VerifyRequest(context.Background(), r); !errors.Is(err, ErrNoCredentials) {
		t.Errorf("empty peer certs: got %v, want ErrNoCredentials", err)
	}
}

func TestMTLS_HeaderVerifyAlwaysErrNoCredentials(t *testing.T) {
	p := NewMTLS(MTLSConfig{})
	_, err := p.Verify(context.Background(), http.Header{"Authorization": {"Bearer x"}})
	if !errors.Is(err, ErrNoCredentials) {
		t.Errorf("mtls.Verify(headers): got %v, want ErrNoCredentials", err)
	}
}

func TestMTLS_BuildIntegration(t *testing.T) {
	prov, err := Build(context.Background(), Config{Provider: "mtls", MTLS: MTLSConfig{}})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if prov.Name() != "mtls" {
		t.Errorf("name = %q", prov.Name())
	}
	if _, ok := prov.(RequestVerifier); !ok {
		t.Error("mtls provider does not satisfy RequestVerifier")
	}
}

func TestMTLS_NilCert(t *testing.T) {
	p := NewMTLS(MTLSConfig{})
	if _, err := p.IdentityFromCert(nil); !errors.Is(err, ErrNoCredentials) {
		t.Errorf("nil cert: got %v, want ErrNoCredentials", err)
	}
}
