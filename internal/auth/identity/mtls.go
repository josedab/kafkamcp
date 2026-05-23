// mtls.go implements the "mtls" identity provider, which derives the
// agent identity from the client certificate presented during the TLS
// handshake.
//
// Unlike the bearer-token providers (static, oidc), mTLS verification
// requires access to the *tls.ConnectionState — not just the request
// headers. The HTTP transport layer is therefore responsible for
// invoking [MTLSProvider.VerifyRequest] (via the optional
// [RequestVerifier] interface) and attaching the resulting identity to
// the request context via [WithIdentity] before the MCP handler runs.
//
// Identity extraction follows the SPIFFE precedent: SPIFFE URI SAN
// first, then other URI SANs, then the Subject CN. OUs are surfaced as
// groups when [MTLSConfig.GroupsFromOUs] is true.
package identity

import (
	"context"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// MTLSConfig configures the mTLS identity provider.
type MTLSConfig struct {
	// AllowedSPIFFEIDs, when non-empty, restricts accepted SPIFFE IDs
	// to the listed values. When empty all SPIFFE IDs whose trust
	// domain matches at least one configured leaf cert are accepted —
	// chain-of-trust is enforced by the TLS terminator, this allowlist
	// is a secondary defense in depth.
	AllowedSPIFFEIDs []string

	// AllowedTrustDomains, when non-empty, restricts the accepted
	// SPIFFE trust domains. Format: "example.org" (no scheme).
	AllowedTrustDomains []string

	// GroupsFromOUs maps the certificate's Subject.OrganizationalUnit
	// values into [AgentIdentity.Groups]. Useful for matching against
	// the policy engine's group-based agent allowlists.
	GroupsFromOUs bool

	// AllowCNFallback controls whether the Subject CN may be used as
	// the agent ID when no URI SAN is present. Defaults to true; set
	// to false in zero-trust deployments that exclusively use SPIFFE.
	AllowCNFallback bool
}

// MTLSProvider verifies the caller's identity from the TLS peer
// certificate. It implements [Provider] (always returning
// [ErrNoCredentials] for the header path, since mTLS verification
// cannot happen at the header layer) and [RequestVerifier] (which
// performs the actual extraction from r.TLS).
type MTLSProvider struct {
	cfg MTLSConfig
}

// NewMTLS constructs an mTLS identity provider. The TLS handshake
// itself (chain validation, expiry, revocation) is the responsibility
// of the HTTP listener's *tls.Config; this provider only extracts the
// identity fields from the already-verified peer certificate chain.
func NewMTLS(cfg MTLSConfig) *MTLSProvider {
	// Default to CN fallback being allowed; callers who want strict
	// SPIFFE-only mode must opt out explicitly.
	return &MTLSProvider{cfg: cfg}
}

// Name implements [Provider].
func (p *MTLSProvider) Name() string { return "mtls" }

// Verify implements [Provider]. mTLS verification cannot happen
// against bare headers, so this always returns [ErrNoCredentials];
// callers should use [VerifyRequest] via the [RequestVerifier]
// interface.
func (p *MTLSProvider) Verify(_ context.Context, _ http.Header) (*AgentIdentity, error) {
	return nil, ErrNoCredentials
}

// VerifyRequest implements [RequestVerifier]. It extracts the verified
// peer certificate from r.TLS.PeerCertificates[0] and derives an
// [AgentIdentity] from it.
//
// The TLS terminator MUST be configured with ClientAuth at least
// VerifyClientCertIfGiven (and ideally RequireAndVerifyClientCert) and
// a CA pool: if r.TLS.PeerCertificates is non-empty it has already
// been chain-verified.
func (p *MTLSProvider) VerifyRequest(_ context.Context, r *http.Request) (*AgentIdentity, error) {
	if r == nil || r.TLS == nil {
		return nil, ErrNoCredentials
	}
	if len(r.TLS.PeerCertificates) == 0 {
		return nil, ErrNoCredentials
	}
	leaf := r.TLS.PeerCertificates[0]
	return p.IdentityFromCert(leaf)
}

// IdentityFromCert is exposed so deployments that terminate mTLS
// out-of-process (e.g. behind an Envoy/nginx sidecar that forwards the
// verified cert via X-Forwarded-Client-Cert) can re-use the same
// extraction logic.
func (p *MTLSProvider) IdentityFromCert(leaf *x509.Certificate) (*AgentIdentity, error) {
	if leaf == nil {
		return nil, ErrNoCredentials
	}

	allowCN := !p.cfg.AllowedSPIFFEIDsConfigured() || p.cfg.AllowCNFallback

	id := ""
	method := "mtls"
	subject := leaf.Subject.CommonName

	// 1) SPIFFE URI SAN — preferred when present.
	for _, u := range leaf.URIs {
		if u == nil {
			continue
		}
		if u.Scheme == "spiffe" {
			if err := p.checkSPIFFE(u); err != nil {
				return nil, err
			}
			id = u.String()
			break
		}
	}

	// 2) Any other URI SAN — fall through when the cert carries no
	//    SPIFFE ID but does carry a URI identity.
	if id == "" {
		for _, u := range leaf.URIs {
			if u == nil || u.Scheme == "spiffe" {
				continue
			}
			id = u.String()
			break
		}
	}

	// 3) Subject CN — last resort.
	if id == "" {
		if !allowCN {
			return nil, fmt.Errorf("%w: certificate carries no URI SAN and CN fallback is disabled", ErrInvalidToken)
		}
		if leaf.Subject.CommonName == "" {
			return nil, fmt.Errorf("%w: certificate has no SPIFFE URI, URI SAN, or CN to use as agent id", ErrInvalidToken)
		}
		id = leaf.Subject.CommonName
	}

	ident := &AgentIdentity{
		ID:         id,
		Method:     method,
		Subject:    subject,
		Issuer:     leaf.Issuer.CommonName,
		VerifiedAt: time.Now().UTC(),
		Claims:     map[string]any{},
	}
	if p.cfg.GroupsFromOUs {
		ident.Groups = append(ident.Groups, leaf.Subject.OrganizationalUnit...)
	}
	if len(leaf.EmailAddresses) > 0 {
		ident.Email = leaf.EmailAddresses[0]
	}
	// Stash a few raw fields for downstream policy use without forcing
	// callers to know about x509 internals.
	ident.Claims["serial"] = leaf.SerialNumber.String()
	ident.Claims["not_after"] = leaf.NotAfter.UTC().Format(time.RFC3339)
	if len(leaf.URIs) > 0 {
		uris := make([]string, 0, len(leaf.URIs))
		for _, u := range leaf.URIs {
			uris = append(uris, u.String())
		}
		ident.Claims["uri_sans"] = uris
	}
	if len(leaf.Subject.Organization) > 0 {
		ident.Claims["organization"] = leaf.Subject.Organization
	}
	return ident, nil
}

// checkSPIFFE enforces the AllowedSPIFFEIDs and AllowedTrustDomains
// lists (when configured).
func (p *MTLSProvider) checkSPIFFE(u *url.URL) error {
	if u == nil {
		return ErrInvalidToken
	}
	if len(p.cfg.AllowedTrustDomains) > 0 {
		ok := false
		for _, td := range p.cfg.AllowedTrustDomains {
			if strings.EqualFold(strings.TrimSpace(td), u.Host) {
				ok = true
				break
			}
		}
		if !ok {
			return fmt.Errorf("%w: SPIFFE trust domain %q not in allowed list", ErrInvalidToken, u.Host)
		}
	}
	if len(p.cfg.AllowedSPIFFEIDs) > 0 {
		ok := false
		full := u.String()
		for _, allowed := range p.cfg.AllowedSPIFFEIDs {
			if strings.EqualFold(strings.TrimSpace(allowed), full) {
				ok = true
				break
			}
		}
		if !ok {
			return fmt.Errorf("%w: SPIFFE ID %q not in allowed list", ErrInvalidToken, full)
		}
	}
	return nil
}

// AllowedSPIFFEIDsConfigured reports whether the operator has narrowed
// the accepted SPIFFE IDs to an explicit allowlist.
func (c *MTLSConfig) AllowedSPIFFEIDsConfigured() bool {
	return c != nil && (len(c.AllowedSPIFFEIDs) > 0 || len(c.AllowedTrustDomains) > 0)
}

// Compile-time check that MTLSProvider satisfies both interfaces.
var (
	_ Provider        = (*MTLSProvider)(nil)
	_ RequestVerifier = (*MTLSProvider)(nil)
)
