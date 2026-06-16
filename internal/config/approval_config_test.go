package config

import "testing"

func secureGuardedConfig(pack string) *Config {
	return &Config{
		Server: ServerConfig{
			Transport: "streamable-http",
			ToolPacks: []string{pack},
		},
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
}

func TestGovernancePackRequiresSignedSecurePosture(t *testing.T) {
	cfg := secureGuardedConfig(PackGovernance)
	if err := cfg.validate(); err == nil {
		t.Fatal("governance must require a signing key")
	}
	t.Setenv(ApprovalSigningKeyEnv, "0123456789abcdef0123456789abcdef")
	if err := cfg.validate(); err != nil {
		t.Fatalf("secure governance config rejected: %v", err)
	}
}

func TestPreviousApprovalSigningKeyMustBePresent(t *testing.T) {
	cfg := secureGuardedConfig(PackOpsWrite)
	cfg.Policies.Approval.PreviousSigningKeyEnvs = []string{"KAFKAMCP_OLD_APPROVAL_KEY"}
	t.Setenv(ApprovalSigningKeyEnv, "0123456789abcdef0123456789abcdef")
	if err := cfg.validate(); err == nil {
		t.Fatal("missing previous signing key must be rejected")
	}
	t.Setenv("KAFKAMCP_OLD_APPROVAL_KEY", "abcdef0123456789abcdef0123456789")
	if err := cfg.validate(); err != nil {
		t.Fatalf("valid key rotation config rejected: %v", err)
	}
}

func TestApprovalTerminalRetentionBound(t *testing.T) {
	cfg := secureGuardedConfig(PackOpsWrite)
	cfg.Policies.Approval.TerminalRetentionSeconds = 2592001
	t.Setenv(ApprovalSigningKeyEnv, "0123456789abcdef0123456789abcdef")
	if err := cfg.validate(); err == nil {
		t.Fatal("terminal retention above 30 days must be rejected")
	}
}
