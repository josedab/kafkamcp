package tenant

import "fmt"

// SSOConfig holds SSO/OIDC authentication configuration.
type SSOConfig struct {
	Provider     string `json:"provider" yaml:"provider"`         // "oidc", "saml", "github", "google"
	ClientID     string `json:"client_id" yaml:"client_id"`
	ClientSecret string `json:"-" yaml:"client_secret"`
	IssuerURL    string `json:"issuer_url" yaml:"issuer_url"`
	CallbackURL  string `json:"callback_url" yaml:"callback_url"`
	Scopes       []string `json:"scopes,omitempty" yaml:"scopes,omitempty"`
}

// ConfigureSSO sets up SSO/OIDC for a tenant.
func (cp *ControlPlane) ConfigureSSO(tenantID string, sso SSOConfig) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if _, err := cp.tenantMgr.GetTenant(tenantID); err != nil {
		return err
	}

	cp.ssoConfigs[tenantID] = &sso
	return nil
}

// GetSSO returns SSO configuration for a tenant.
func (cp *ControlPlane) GetSSO(tenantID string) (*SSOConfig, error) {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	sso, ok := cp.ssoConfigs[tenantID]
	if !ok {
		return nil, fmt.Errorf("no SSO configured for tenant %q", tenantID)
	}
	return sso, nil
}
