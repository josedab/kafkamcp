package server

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/anomaly"
	"github.com/kafkamcp/kafkamcp/internal/audit"
	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/config"
	"github.com/kafkamcp/kafkamcp/internal/contracts"
	"github.com/kafkamcp/kafkamcp/internal/cost"
	"github.com/kafkamcp/kafkamcp/internal/dlq"
	"github.com/kafkamcp/kafkamcp/internal/forecast"
	"github.com/kafkamcp/kafkamcp/internal/gitops"
	"github.com/kafkamcp/kafkamcp/internal/graph"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/kafkamcp/kafkamcp/internal/lagdiag"
	"github.com/kafkamcp/kafkamcp/internal/lineage"
	"github.com/kafkamcp/kafkamcp/internal/masking"
	"github.com/kafkamcp/kafkamcp/internal/metrics"
	"github.com/kafkamcp/kafkamcp/internal/migration"
	"github.com/kafkamcp/kafkamcp/internal/nlquery"
	"github.com/kafkamcp/kafkamcp/internal/persona"
	"github.com/kafkamcp/kafkamcp/internal/platform"
	"github.com/kafkamcp/kafkamcp/internal/plugin"
	"github.com/kafkamcp/kafkamcp/internal/ratelimit"
	"github.com/kafkamcp/kafkamcp/internal/schema"
	"github.com/kafkamcp/kafkamcp/internal/selfheal"
	"github.com/kafkamcp/kafkamcp/internal/watch"
	"github.com/kafkamcp/kafkamcp/internal/workflow"
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
			anomalyDetector: anomaly.NewDetector(anomaly.DefaultConfig(), logger),
			nlQueryEngine:   nlquery.NewEngine(logger),
			costAnalyzer:    cost.NewAnalyzer(logger),
			forecaster:      forecast.NewForecaster(30),
			knowledgeGraph:  graph.NewKnowledgeGraph(logger),
			lagAnalyzer:     lagdiag.NewAnalyzer(),
		},
		Governance: Governance{
			policyEngine:   auth.NewPolicyEngine(cfg.Policies),
			auditLogger:    auditLogger,
			rateLimiter:    rl,
			maskingEngine:  masking.NewEngine(cfg.Masking, logger),
			contractEngine: contracts.NewEngine(),
			personaEngine:  persona.NewEngine(),
		},
		Streaming: Streaming{
			watchManager:   watch.NewManager(logger),
			dlqRemediator:  dlq.NewRemediator(logger),
			selfHealMon:    selfheal.NewMonitor(),
			lineageTracker: lineage.NewTracker(),
		},
		Platform: Platform{
			pluginRegistry:   plugin.NewRegistry(logger),
			agentRegistry:    kafka.NewAgentRegistry(),
			platformMgr:      platform.NewManager(logger),
			gitopsManager:    gitops.NewManager(logger),
			migrationPlanner: migration.NewPlanner(),
			workflowEngine:   workflow.NewEngine(logger),
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
