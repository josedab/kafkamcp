package app_test

import (
	"testing"

	"github.com/josedab/kafkamcp/internal/app"
	"github.com/josedab/kafkamcp/internal/config"
)

func TestValidateTLSPairing_BothSet(t *testing.T) {
	cfg := &config.Config{Server: config.ServerConfig{TLSCert: "cert.pem", TLSKey: "key.pem"}}
	if err := app.ValidateTLSPairing(cfg); err != nil {
		t.Errorf("expected no error when both cert+key set, got: %v", err)
	}
}

func TestValidateTLSPairing_NeitherSet(t *testing.T) {
	cfg := &config.Config{}
	if err := app.ValidateTLSPairing(cfg); err != nil {
		t.Errorf("expected no error when neither cert nor key set, got: %v", err)
	}
}

func TestValidateTLSPairing_OnlyCert(t *testing.T) {
	cfg := &config.Config{Server: config.ServerConfig{TLSCert: "cert.pem"}}
	if err := app.ValidateTLSPairing(cfg); err == nil {
		t.Error("expected error when only cert is set")
	}
}

func TestValidateTLSPairing_OnlyKey(t *testing.T) {
	cfg := &config.Config{Server: config.ServerConfig{TLSKey: "key.pem"}}
	if err := app.ValidateTLSPairing(cfg); err == nil {
		t.Error("expected error when only key is set")
	}
}

func TestValidateMTLSRequiresTLS_MTLSWithTLS(t *testing.T) {
	cfg := &config.Config{
		Server:   config.ServerConfig{TLSCert: "cert.pem", TLSKey: "key.pem"},
		Policies: config.PolicyConfig{Identity: config.IdentityConfig{Provider: "mtls"}},
	}
	if err := app.ValidateMTLSRequiresTLS(cfg); err != nil {
		t.Errorf("expected no error when mTLS+TLS both set, got: %v", err)
	}
}

func TestValidateMTLSRequiresTLS_MTLSWithoutTLS(t *testing.T) {
	cfg := &config.Config{
		Policies: config.PolicyConfig{Identity: config.IdentityConfig{Provider: "mtls"}},
	}
	if err := app.ValidateMTLSRequiresTLS(cfg); err == nil {
		t.Error("expected error when mTLS is set without TLS cert+key")
	}
}

func TestValidateMTLSRequiresTLS_NonMTLSProviderNoError(t *testing.T) {
	for _, provider := range []string{"", "none", "oidc", "static"} {
		t.Run(provider, func(t *testing.T) {
			cfg := &config.Config{
				Policies: config.PolicyConfig{Identity: config.IdentityConfig{Provider: provider}},
			}
			if err := app.ValidateMTLSRequiresTLS(cfg); err != nil {
				t.Errorf("expected no error for provider %q, got: %v", provider, err)
			}
		})
	}
}
