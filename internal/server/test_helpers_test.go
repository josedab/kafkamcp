package server

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/josedab/kafkamcp/internal/aclmgmt"
	"github.com/josedab/kafkamcp/internal/audit"
	"github.com/josedab/kafkamcp/internal/auth"
	"github.com/josedab/kafkamcp/internal/config"
	"github.com/josedab/kafkamcp/internal/kafka"
	"github.com/josedab/kafkamcp/internal/lagdiag"
	"github.com/josedab/kafkamcp/internal/masking"
	"github.com/josedab/kafkamcp/internal/metrics"
	"github.com/josedab/kafkamcp/internal/onboarding"
	"github.com/josedab/kafkamcp/internal/opa"
	"github.com/josedab/kafkamcp/internal/ratelimit"
	"github.com/josedab/kafkamcp/internal/schema"
)

func makeToolRequest(args map[string]any) mcp.CallToolRequest {
	req := mcp.CallToolRequest{}
	req.Params.Arguments = args
	return req
}

func assertIsError(t *testing.T, result *mcp.CallToolResult) {
	t.Helper()
	if result == nil {
		t.Fatal("expected non-nil error result, got nil")
	}
	if !result.IsError {
		t.Error("expected IsError=true in result")
	}
}

// newTestServer constructs a minimal Server with nil kafkaManager.
// Handlers that touch kafkaManager will panic, but validation/auth paths work.
func newTestServer(t *testing.T, opts ...func(*config.Config)) *Server {
	t.Helper()
	cfg := &config.Config{
		Server:      config.ServerConfig{Transport: "stdio"},
		Policies:    config.PolicyConfig{DefaultDeny: false},
		SchemaCache: config.CacheConfig{MaxEntries: 100, TTLSeconds: 60},
		Audit:       config.AuditConfig{Enabled: false, MaxEntries: 100},
	}
	for _, o := range opts {
		o(cfg)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	auditLogger, err := audit.NewLogger(context.Background(), false, 100, "", logger)
	if err != nil {
		t.Fatalf("creating audit logger: %v", err)
	}

	rl := ratelimit.New(0)
	for _, agent := range cfg.Policies.Agents {
		if agent.RateLimit.RequestsPerMinute > 0 {
			rl.SetAgentLimit(agent.ID, agent.RateLimit.RequestsPerMinute)
		}
	}

	return &Server{
		config:         cfg,
		kafkaManager:   nil,
		schemaClients:  make(map[string]*schema.Client),
		connectClients: make(map[string]*kafka.ConnectClient),
		metrics:        metrics.New(),
		logger:         logger,
		expensiveOps:   make(chan struct{}, 5),
		Analytics: Analytics{
			lagAnalyzer: lagdiag.NewAnalyzer(),
		},
		Governance: Governance{
			policyEngine:  auth.NewPolicyEngine(cfg.Policies),
			auditLogger:   auditLogger,
			rateLimiter:   rl,
			maskingEngine: masking.NewEngine(cfg.Masking, logger),
		},
		Platform: Platform{
			onboardingEngine: onboarding.NewEngine(logger),
			opaPolicyEngine:  opa.NewPolicyEngine(logger),
			aclMgmtEngine:    aclmgmt.NewEngine(logger),
		},
	}
}

func withDefaultDeny(cfg *config.Config) {
	cfg.Policies.DefaultDeny = true
	cfg.Policies.Agents = []config.AgentPolicy{
		{
			ID:   "allowed-agent",
			Role: "admin",
			Topics: []config.TopicPermission{
				{Pattern: "*", Permissions: []string{"read", "write", "create", "delete"}},
			},
			ConsumerGroups: config.GroupPermission{Permissions: []string{"read", "write"}},
			Schemas:        config.SchemaPermission{Permissions: []string{"read", "write"}},
		},
	}
}

func withRateLimit(cfg *config.Config) {
	cfg.Policies.Agents = append(cfg.Policies.Agents, config.AgentPolicy{
		ID:        "rate-limited-agent",
		Role:      "reader",
		RateLimit: config.RateLimitConfig{RequestsPerMinute: 1},
		Topics: []config.TopicPermission{
			{Pattern: "*", Permissions: []string{"read"}},
		},
	})
}
