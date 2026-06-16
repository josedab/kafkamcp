package config

import "testing"

func newNetworkCfg(transport, provider string, defaultDeny bool) *Config {
	c := &Config{}
	c.Server.Transport = transport
	c.Policies.Identity.Provider = provider
	c.Policies.DefaultDeny = defaultDeny
	return c
}

func TestTransportSecurityError_StdioAlwaysAllowed(t *testing.T) {
	c := newNetworkCfg("stdio", "none", false)
	if err := c.TransportSecurityError(); err != nil {
		t.Fatalf("stdio must always be allowed, got: %v", err)
	}
}

func TestTransportSecurityError_NetworkFailsClosedWithoutIdentity(t *testing.T) {
	for _, transport := range []string{"sse", "streamable-http"} {
		c := newNetworkCfg(transport, "none", true)
		if err := c.TransportSecurityError(); err == nil {
			t.Errorf("%s without cryptographic identity must fail closed", transport)
		}
	}
}

func TestTransportSecurityError_NetworkFailsClosedWithoutDefaultDeny(t *testing.T) {
	c := newNetworkCfg("streamable-http", "oidc", false)
	if err := c.TransportSecurityError(); err == nil {
		t.Error("network transport without default_deny must fail closed")
	}
}

func TestTransportSecurityError_NetworkAllowedWhenSecure(t *testing.T) {
	for _, provider := range []string{"static", "oidc", "mtls"} {
		c := newNetworkCfg("streamable-http", provider, true)
		denyFallback := false
		c.Policies.Identity.AllowHeaderFallback = &denyFallback
		if err := c.TransportSecurityError(); err != nil {
			t.Errorf("secure %s posture should be allowed, got: %v", provider, err)
		}
	}
}

func TestTransportSecurityError_NetworkRejectsHeaderFallback(t *testing.T) {
	c := newNetworkCfg("streamable-http", "oidc", true)
	allowFallback := true
	c.Policies.Identity.AllowHeaderFallback = &allowFallback
	if err := c.TransportSecurityError(); err == nil {
		t.Fatal("network transport must reject legacy header fallback")
	}
}

func TestTransportSecurityError_ConfigOverride(t *testing.T) {
	c := newNetworkCfg("sse", "none", false)
	c.Server.DangerouslyAllowUnauthenticatedNetwork = true
	if err := c.TransportSecurityError(); err != nil {
		t.Errorf("explicit config override should allow insecure network transport, got: %v", err)
	}
}

func TestTransportSecurityError_EnvOverride(t *testing.T) {
	t.Setenv(UnsafeNetworkOverrideEnv, "true")
	c := newNetworkCfg("sse", "none", false)
	if !c.UnsafeNetworkOverrideEnabled() {
		t.Fatal("env override should be detected")
	}
	if err := c.TransportSecurityError(); err != nil {
		t.Errorf("env override should allow insecure network transport, got: %v", err)
	}
}

func TestIdentityConfig_IsCryptographic(t *testing.T) {
	cases := map[string]bool{
		"":         false,
		"none":     false,
		"NONE":     false,
		"static":   true,
		"oidc":     true,
		"mtls":     true,
		"  mtls  ": true,
		"bogus":    false,
	}
	for provider, want := range cases {
		ic := IdentityConfig{Provider: provider}
		if got := ic.IsCryptographic(); got != want {
			t.Errorf("IsCryptographic(%q)=%v, want %v", provider, got, want)
		}
	}
}

func TestNetworkTransport(t *testing.T) {
	cases := map[string]bool{
		"stdio":           false,
		"STDIO":           false,
		"":                false,
		"sse":             true,
		"streamable-http": true,
	}
	for transport, want := range cases {
		c := &Config{}
		c.Server.Transport = transport
		if got := c.NetworkTransport(); got != want {
			t.Errorf("NetworkTransport(%q)=%v, want %v", transport, got, want)
		}
	}
}

func TestValidateOpsWriteRequiresSignedApprovals(t *testing.T) {
	c := &Config{
		Server: ServerConfig{Transport: "streamable-http", ToolPacks: []string{PackOpsWrite}},
		Clusters: []ClusterConfig{{
			Name:             "local",
			BootstrapServers: "localhost:9092",
		}},
		Policies: PolicyConfig{
			DefaultDeny: true,
			Identity: IdentityConfig{
				Provider: "static",
				Static: StaticIdentityConfig{Tokens: []StaticTokenConfig{{
					Token: "0123456789abcdef", AgentID: "operator",
				}}},
			},
		},
	}
	if err := c.validate(); err == nil {
		t.Fatal("ops-write must require an approval signing key")
	}
	t.Setenv(ApprovalSigningKeyEnv, "0123456789abcdef0123456789abcdef")
	if err := c.validate(); err != nil {
		t.Fatalf("valid signed approval configuration rejected: %v", err)
	}
}

func TestValidateOpsWriteCannotDisableApproval(t *testing.T) {
	disabled := false
	c := &Config{
		Server: ServerConfig{Transport: "streamable-http", ToolPacks: []string{PackOpsWrite}},
		Clusters: []ClusterConfig{{
			Name:             "local",
			BootstrapServers: "localhost:9092",
		}},
		Policies: PolicyConfig{
			DefaultDeny: true,
			Identity: IdentityConfig{
				Provider: "static",
				Static: StaticIdentityConfig{Tokens: []StaticTokenConfig{{
					Token: "0123456789abcdef", AgentID: "operator",
				}}},
			},
			Approval: ApprovalConfig{Enabled: &disabled},
		},
	}
	t.Setenv(ApprovalSigningKeyEnv, "0123456789abcdef0123456789abcdef")
	if err := c.validate(); err == nil {
		t.Fatal("ops-write must not permit approval enforcement to be disabled")
	}
}
