package server

import (
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

// Option configures a Server during construction.
type Option func(*Server)

// WithAuditLogger sets a custom audit logger.
func WithAuditLogger(a *audit.Logger) Option {
	return func(s *Server) { s.auditLogger = a }
}

// WithRateLimiter sets a custom rate limiter.
func WithRateLimiter(rl *ratelimit.Limiter) Option {
	return func(s *Server) { s.rateLimiter = rl }
}

// WithMetrics sets a custom metrics collector.
func WithMetrics(m *metrics.Metrics) Option {
	return func(s *Server) { s.metrics = m }
}

// WithWatchManager sets a custom watch manager.
func WithWatchManager(w *watch.Manager) Option {
	return func(s *Server) { s.watchManager = w }
}

// WithMaskingEngine sets a custom masking engine.
func WithMaskingEngine(m *masking.Engine) Option {
	return func(s *Server) { s.maskingEngine = m }
}

// WithDLQRemediator sets a custom DLQ remediator.
func WithDLQRemediator(d *dlq.Remediator) Option {
	return func(s *Server) { s.dlqRemediator = d }
}

// WithReplicationMonitor sets a custom replication monitor.
func WithReplicationMonitor(r *replication.Monitor) Option {
	return func(s *Server) { s.replicationMon = r }
}

// WithPluginRegistry sets a custom plugin registry.
func WithPluginRegistry(p *plugin.Registry) Option {
	return func(s *Server) { s.pluginRegistry = p }
}

// WithAnomalyDetector sets a custom anomaly detector.
func WithAnomalyDetector(a *anomaly.Detector) Option {
	return func(s *Server) { s.anomalyDetector = a }
}

// WithNLQueryEngine sets a custom natural language query engine.
func WithNLQueryEngine(n *nlquery.Engine) Option {
	return func(s *Server) { s.nlQueryEngine = n }
}

// WithWorkflowEngine sets a custom workflow engine.
func WithWorkflowEngine(w *workflow.Engine) Option {
	return func(s *Server) { s.workflowEngine = w }
}

// WithCostAnalyzer sets a custom cost analyzer.
func WithCostAnalyzer(c *cost.Analyzer) Option {
	return func(s *Server) { s.costAnalyzer = c }
}

// WithGitOpsManager sets a custom GitOps manager.
func WithGitOpsManager(g *gitops.Manager) Option {
	return func(s *Server) { s.gitopsManager = g }
}

// WithKnowledgeGraph sets a custom knowledge graph.
func WithKnowledgeGraph(kg *graph.KnowledgeGraph) Option {
	return func(s *Server) { s.knowledgeGraph = kg }
}

// WithPlatformManager sets a custom platform manager.
func WithPlatformManager(p *platform.Manager) Option {
	return func(s *Server) { s.platformMgr = p }
}

// WithContractEngine sets a custom contract engine.
func WithContractEngine(c *contracts.Engine) Option {
	return func(s *Server) { s.contractEngine = c }
}

// WithLagAnalyzer sets a custom lag analyzer.
func WithLagAnalyzer(l *lagdiag.Analyzer) Option {
	return func(s *Server) { s.lagAnalyzer = l }
}

// WithMigrationPlanner sets a custom migration planner.
func WithMigrationPlanner(m *migration.Planner) Option {
	return func(s *Server) { s.migrationPlanner = m }
}

// WithLineageTracker sets a custom lineage tracker.
func WithLineageTracker(l *lineage.Tracker) Option {
	return func(s *Server) { s.lineageTracker = l }
}

// WithSelfHealMonitor sets a custom self-heal monitor.
func WithSelfHealMonitor(sh *selfheal.Monitor) Option {
	return func(s *Server) { s.selfHealMon = sh }
}

// WithPersonaEngine sets a custom persona engine.
func WithPersonaEngine(p *persona.Engine) Option {
	return func(s *Server) { s.personaEngine = p }
}

// WithForecaster sets a custom forecaster.
func WithForecaster(f *forecast.Forecaster) Option {
	return func(s *Server) { s.forecaster = f }
}

// WithGatewayManager sets a custom gateway manager.
func WithGatewayManager(g *gateway.Manager) Option {
	return func(s *Server) { s.gatewayManager = g }
}

// WithCollaborationCoordinator sets a custom collaboration coordinator.
func WithCollaborationCoordinator(c *collaboration.Coordinator) Option {
	return func(s *Server) { s.collaborationCoord = c }
}

// WithSemanticEngine sets a custom semantic search engine.
func WithSemanticEngine(se *semantic.Engine) Option {
	return func(s *Server) { s.semanticEngine = se }
}

// WithOnboardingEngine sets a custom onboarding engine.
func WithOnboardingEngine(o *onboarding.Engine) Option {
	return func(s *Server) { s.onboardingEngine = o }
}

// WithOPAPolicyEngine sets a custom OPA policy engine.
func WithOPAPolicyEngine(o *opa.PolicyEngine) Option {
	return func(s *Server) { s.opaPolicyEngine = o }
}

// WithOperatorController sets a custom K8s operator controller.
func WithOperatorController(o *operator.Controller) Option {
	return func(s *Server) { s.operatorController = o }
}

// WithPipelineEngine sets a custom pipeline engine.
func WithPipelineEngine(p *pipeline.Engine) Option {
	return func(s *Server) { s.pipelineEngine = p }
}
