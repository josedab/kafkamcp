package app

import (
	"fmt"
	"strings"

	"github.com/josedab/kafkamcp/internal/config"
)

// ValidateTLSPairing returns an error if the TLS configuration is incomplete:
// both tls_cert and tls_key must be set together or both must be absent.
func ValidateTLSPairing(cfg *config.Config) error {
	certSet := strings.TrimSpace(cfg.Server.TLSCert) != ""
	keySet := strings.TrimSpace(cfg.Server.TLSKey) != ""
	if certSet != keySet {
		return fmt.Errorf("incomplete TLS configuration: tls_cert and tls_key must both be set (cert=%v, key=%v)", certSet, keySet)
	}
	return nil
}

// ValidateMTLSRequiresTLS returns an error when the mTLS identity provider is
// configured but TLS is not, since mTLS cannot operate without a server
// certificate and private key.
func ValidateMTLSRequiresTLS(cfg *config.Config) error {
	if strings.ToLower(strings.TrimSpace(cfg.Policies.Identity.Provider)) != "mtls" {
		return nil
	}
	if strings.TrimSpace(cfg.Server.TLSCert) == "" || strings.TrimSpace(cfg.Server.TLSKey) == "" {
		return fmt.Errorf("mTLS identity provider requires TLS: set server.tls_cert and server.tls_key")
	}
	return nil
}
