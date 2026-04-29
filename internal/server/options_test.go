package server

import (
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/anomaly"
	"github.com/kafkamcp/kafkamcp/internal/audit"
	"github.com/kafkamcp/kafkamcp/internal/collaboration"
	"github.com/kafkamcp/kafkamcp/internal/contracts"
	"github.com/kafkamcp/kafkamcp/internal/cost"
	"github.com/kafkamcp/kafkamcp/internal/dlq"
	"github.com/kafkamcp/kafkamcp/internal/forecast"
	"github.com/kafkamcp/kafkamcp/internal/gateway"
	"github.com/kafkamcp/kafkamcp/internal/gitops"
	"github.com/kafkamcp/kafkamcp/internal/graph"
	"github.com/kafkamcp/kafkamcp/internal/lagdiag"
	"github.com/kafkamcp/kafkamcp/internal/lineage"
	"github.com/kafkamcp/kafkamcp/internal/masking"
	"github.com/kafkamcp/kafkamcp/internal/metrics"
	"github.com/kafkamcp/kafkamcp/internal/migration"
	"github.com/kafkamcp/kafkamcp/internal/nlquery"
	"github.com/kafkamcp/kafkamcp/internal/onboarding"
	"github.com/kafkamcp/kafkamcp/internal/opa"
	"github.com/kafkamcp/kafkamcp/internal/operator"
	"github.com/kafkamcp/kafkamcp/internal/persona"
	"github.com/kafkamcp/kafkamcp/internal/pipeline"
	"github.com/kafkamcp/kafkamcp/internal/platform"
	"github.com/kafkamcp/kafkamcp/internal/plugin"
	"github.com/kafkamcp/kafkamcp/internal/ratelimit"
	"github.com/kafkamcp/kafkamcp/internal/replication"
	"github.com/kafkamcp/kafkamcp/internal/selfheal"
	"github.com/kafkamcp/kafkamcp/internal/semantic"
	"github.com/kafkamcp/kafkamcp/internal/watch"
	"github.com/kafkamcp/kafkamcp/internal/workflow"
)

func TestWithOptions(t *testing.T) {
	tests := []struct {
		name   string
		option Option
		verify func(*Server) bool
	}{
		{
			name:   "WithAuditLogger",
			option: WithAuditLogger(&audit.Logger{}),
			verify: func(s *Server) bool { return s.auditLogger != nil },
		},
		{
			name:   "WithRateLimiter",
			option: WithRateLimiter(&ratelimit.Limiter{}),
			verify: func(s *Server) bool { return s.rateLimiter != nil },
		},
		{
			name:   "WithMetrics",
			option: WithMetrics(&metrics.Metrics{}),
			verify: func(s *Server) bool { return s.metrics != nil },
		},
		{
			name:   "WithWatchManager",
			option: WithWatchManager(&watch.Manager{}),
			verify: func(s *Server) bool { return s.watchManager != nil },
		},
		{
			name:   "WithMaskingEngine",
			option: WithMaskingEngine(&masking.Engine{}),
			verify: func(s *Server) bool { return s.maskingEngine != nil },
		},
		{
			name:   "WithDLQRemediator",
			option: WithDLQRemediator(&dlq.Remediator{}),
			verify: func(s *Server) bool { return s.dlqRemediator != nil },
		},
		{
			name:   "WithReplicationMonitor",
			option: WithReplicationMonitor(&replication.Monitor{}),
			verify: func(s *Server) bool { return s.replicationMon != nil },
		},
		{
			name:   "WithPluginRegistry",
			option: WithPluginRegistry(&plugin.Registry{}),
			verify: func(s *Server) bool { return s.pluginRegistry != nil },
		},
		{
			name:   "WithAnomalyDetector",
			option: WithAnomalyDetector(&anomaly.Detector{}),
			verify: func(s *Server) bool { return s.anomalyDetector != nil },
		},
		{
			name:   "WithNLQueryEngine",
			option: WithNLQueryEngine(&nlquery.Engine{}),
			verify: func(s *Server) bool { return s.nlQueryEngine != nil },
		},
		{
			name:   "WithWorkflowEngine",
			option: WithWorkflowEngine(&workflow.Engine{}),
			verify: func(s *Server) bool { return s.workflowEngine != nil },
		},
		{
			name:   "WithCostAnalyzer",
			option: WithCostAnalyzer(&cost.Analyzer{}),
			verify: func(s *Server) bool { return s.costAnalyzer != nil },
		},
		{
			name:   "WithGitOpsManager",
			option: WithGitOpsManager(&gitops.Manager{}),
			verify: func(s *Server) bool { return s.gitopsManager != nil },
		},
		{
			name:   "WithKnowledgeGraph",
			option: WithKnowledgeGraph(&graph.KnowledgeGraph{}),
			verify: func(s *Server) bool { return s.knowledgeGraph != nil },
		},
		{
			name:   "WithPlatformManager",
			option: WithPlatformManager(&platform.Manager{}),
			verify: func(s *Server) bool { return s.platformMgr != nil },
		},
		{
			name:   "WithContractEngine",
			option: WithContractEngine(&contracts.Engine{}),
			verify: func(s *Server) bool { return s.contractEngine != nil },
		},
		{
			name:   "WithLagAnalyzer",
			option: WithLagAnalyzer(&lagdiag.Analyzer{}),
			verify: func(s *Server) bool { return s.lagAnalyzer != nil },
		},
		{
			name:   "WithMigrationPlanner",
			option: WithMigrationPlanner(&migration.Planner{}),
			verify: func(s *Server) bool { return s.migrationPlanner != nil },
		},
		{
			name:   "WithLineageTracker",
			option: WithLineageTracker(&lineage.Tracker{}),
			verify: func(s *Server) bool { return s.lineageTracker != nil },
		},
		{
			name:   "WithSelfHealMonitor",
			option: WithSelfHealMonitor(&selfheal.Monitor{}),
			verify: func(s *Server) bool { return s.selfHealMon != nil },
		},
		{
			name:   "WithPersonaEngine",
			option: WithPersonaEngine(&persona.Engine{}),
			verify: func(s *Server) bool { return s.personaEngine != nil },
		},
		{
			name:   "WithForecaster",
			option: WithForecaster(&forecast.Forecaster{}),
			verify: func(s *Server) bool { return s.forecaster != nil },
		},
		{
			name:   "WithGatewayManager",
			option: WithGatewayManager(&gateway.Manager{}),
			verify: func(s *Server) bool { return s.gatewayManager != nil },
		},
		{
			name:   "WithCollaborationCoordinator",
			option: WithCollaborationCoordinator(&collaboration.Coordinator{}),
			verify: func(s *Server) bool { return s.collaborationCoord != nil },
		},
		{
			name:   "WithSemanticEngine",
			option: WithSemanticEngine(&semantic.Engine{}),
			verify: func(s *Server) bool { return s.semanticEngine != nil },
		},
		{
			name:   "WithOnboardingEngine",
			option: WithOnboardingEngine(&onboarding.Engine{}),
			verify: func(s *Server) bool { return s.onboardingEngine != nil },
		},
		{
			name:   "WithOPAPolicyEngine",
			option: WithOPAPolicyEngine(&opa.PolicyEngine{}),
			verify: func(s *Server) bool { return s.opaPolicyEngine != nil },
		},
		{
			name:   "WithOperatorController",
			option: WithOperatorController(&operator.Controller{}),
			verify: func(s *Server) bool { return s.operatorController != nil },
		},
		{
			name:   "WithPipelineEngine",
			option: WithPipelineEngine(&pipeline.Engine{}),
			verify: func(s *Server) bool { return s.pipelineEngine != nil },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{}
			tt.option(s)
			if !tt.verify(s) {
				t.Errorf("%s did not set the expected field", tt.name)
			}
		})
	}
}

func TestWithOptions_Composable(t *testing.T) {
	s := &Server{}
	opts := []Option{
		WithAuditLogger(&audit.Logger{}),
		WithRateLimiter(&ratelimit.Limiter{}),
		WithMetrics(&metrics.Metrics{}),
		WithSelfHealMonitor(&selfheal.Monitor{}),
	}

	for _, opt := range opts {
		opt(s)
	}

	if s.auditLogger == nil {
		t.Error("auditLogger not set after chaining")
	}
	if s.rateLimiter == nil {
		t.Error("rateLimiter not set after chaining")
	}
	if s.metrics == nil {
		t.Error("metrics not set after chaining")
	}
	if s.selfHealMon == nil {
		t.Error("selfHealMon not set after chaining")
	}
}

func TestWithOptions_OverridesExisting(t *testing.T) {
	s := &Server{}
	first := &audit.Logger{}
	second := &audit.Logger{}

	WithAuditLogger(first)(s)
	WithAuditLogger(second)(s)

	if s.auditLogger != second {
		t.Error("expected second value to override first")
	}
}
