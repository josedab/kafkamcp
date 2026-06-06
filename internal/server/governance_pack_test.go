package server

import (
	"io"
	"log/slog"
	"testing"

	"github.com/josedab/kafkamcp/internal/config"
	"github.com/josedab/kafkamcp/internal/kafka"
)

func TestGovernancePackRequiresSignedApprovals(t *testing.T) {
	t.Setenv(config.ApprovalSigningKeyEnv, "0123456789abcdef0123456789abcdef")
	cfg := &config.Config{
		Server: config.ServerConfig{
			Transport: "streamable-http",
			ToolPacks: []string{config.PackGovernance},
		},
		Clusters: []config.ClusterConfig{{
			Name: "local", BootstrapServers: "localhost:9092", Default: true,
		}},
		Policies: config.PolicyConfig{
			DefaultDeny: true,
			Identity: config.IdentityConfig{
				Provider: "static",
				Static: config.StaticIdentityConfig{Tokens: []config.StaticTokenConfig{{
					Token: "0123456789abcdef", AgentID: "operator",
				}}},
			},
			Agents: []config.AgentPolicy{{ID: "operator", Role: "admin"}},
		},
		SchemaCache: config.CacheConfig{MaxEntries: 10, TTLSeconds: 60},
		Audit:       config.AuditConfig{Enabled: false, MaxEntries: 10},
	}
	server, err := New(cfg, &kafka.MockKafkaManager{}, slog.New(slog.NewTextHandler(io.Discard, nil)), "test")
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("close server: %v", err)
		}
	}()
	if !server.approvalEnforced || !server.approvalManager.SignedDecisionsRequired() {
		t.Fatal("governance pack must enforce signed approval decisions")
	}
	for _, tool := range []string{
		"kafka_approval",
		"kafka_governance_decision",
		"kafka_acl_manage",
		"kafka_policy_evaluate",
		"kafka_policy_simulate",
	} {
		if _, ok := server.RegisteredTools()[tool]; !ok {
			t.Errorf("governance tool %q not registered", tool)
		}
	}
}
