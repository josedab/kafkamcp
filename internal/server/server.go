package server

import (
"context"
"fmt"
"log/slog"
"sync"
"time"

"github.com/kafkamcp/kafkamcp/internal/adaptiverate"
"github.com/kafkamcp/kafkamcp/internal/agentmemory"
"github.com/kafkamcp/kafkamcp/internal/agentprofile"
"github.com/kafkamcp/kafkamcp/internal/anomaly"
"github.com/kafkamcp/kafkamcp/internal/advisor"
"github.com/kafkamcp/kafkamcp/internal/audit"
"github.com/kafkamcp/kafkamcp/internal/auth"
"github.com/kafkamcp/kafkamcp/internal/catalog"
"github.com/kafkamcp/kafkamcp/internal/capability"
"github.com/kafkamcp/kafkamcp/internal/cdc"
"github.com/kafkamcp/kafkamcp/internal/chat"
"github.com/kafkamcp/kafkamcp/internal/chaos"
"github.com/kafkamcp/kafkamcp/internal/clouddeploy"
"github.com/kafkamcp/kafkamcp/internal/collaboration"
"github.com/kafkamcp/kafkamcp/internal/codec"
"github.com/kafkamcp/kafkamcp/internal/compliance"
"github.com/kafkamcp/kafkamcp/internal/config"
"github.com/kafkamcp/kafkamcp/internal/configvcs"
"github.com/kafkamcp/kafkamcp/internal/contracts"
"github.com/kafkamcp/kafkamcp/internal/controlplane"
"github.com/kafkamcp/kafkamcp/internal/correlation"
"github.com/kafkamcp/kafkamcp/internal/cost"
"github.com/kafkamcp/kafkamcp/internal/costattribution"
"github.com/kafkamcp/kafkamcp/internal/dagworkflow"
"github.com/kafkamcp/kafkamcp/internal/dataquality"
"github.com/kafkamcp/kafkamcp/internal/devmode"
"github.com/kafkamcp/kafkamcp/internal/digitaltwin"
"github.com/kafkamcp/kafkamcp/internal/dlq"
"github.com/kafkamcp/kafkamcp/internal/evolution"
"github.com/kafkamcp/kafkamcp/internal/eventsourcing"
"github.com/kafkamcp/kafkamcp/internal/federation"
"github.com/kafkamcp/kafkamcp/internal/finops"
"github.com/kafkamcp/kafkamcp/internal/flowdebug"
"github.com/kafkamcp/kafkamcp/internal/forecast"
"github.com/kafkamcp/kafkamcp/internal/gateway"
"github.com/kafkamcp/kafkamcp/internal/gitops"
"github.com/kafkamcp/kafkamcp/internal/graph"
"github.com/kafkamcp/kafkamcp/internal/incident"
"github.com/kafkamcp/kafkamcp/internal/kafka"
"github.com/kafkamcp/kafkamcp/internal/lagdiag"
"github.com/kafkamcp/kafkamcp/internal/lineage"
"github.com/kafkamcp/kafkamcp/internal/llmpipeline"
"github.com/kafkamcp/kafkamcp/internal/masking"
"github.com/kafkamcp/kafkamcp/internal/match"
"github.com/kafkamcp/kafkamcp/internal/metrics"
"github.com/kafkamcp/kafkamcp/internal/migration"
"github.com/kafkamcp/kafkamcp/internal/nlpipeline"
"github.com/kafkamcp/kafkamcp/internal/nlquery"
"github.com/kafkamcp/kafkamcp/internal/observability"
"github.com/kafkamcp/kafkamcp/internal/onboarding"
"github.com/kafkamcp/kafkamcp/internal/opa"
"github.com/kafkamcp/kafkamcp/internal/operator"
"github.com/kafkamcp/kafkamcp/internal/orchestration"
"github.com/kafkamcp/kafkamcp/internal/pipeline"
"github.com/kafkamcp/kafkamcp/internal/platform"
"github.com/kafkamcp/kafkamcp/internal/playbook"
"github.com/kafkamcp/kafkamcp/internal/plugin"
"github.com/kafkamcp/kafkamcp/internal/profiler"
"github.com/kafkamcp/kafkamcp/internal/ratelimit"
"github.com/kafkamcp/kafkamcp/internal/replay"
"github.com/kafkamcp/kafkamcp/internal/replication"
"github.com/kafkamcp/kafkamcp/internal/retention"
"github.com/kafkamcp/kafkamcp/internal/sandbox"
"github.com/kafkamcp/kafkamcp/internal/schema"
"github.com/kafkamcp/kafkamcp/internal/schemaadvisor"
"github.com/kafkamcp/kafkamcp/internal/schemaplayground"
"github.com/kafkamcp/kafkamcp/internal/selfheal"
"github.com/kafkamcp/kafkamcp/internal/semantic"
"github.com/kafkamcp/kafkamcp/internal/smartroute"
"github.com/kafkamcp/kafkamcp/internal/streamsql"
"github.com/kafkamcp/kafkamcp/internal/taskqueue"
"github.com/kafkamcp/kafkamcp/internal/telemetry"
"github.com/kafkamcp/kafkamcp/internal/timeseries"
"github.com/kafkamcp/kafkamcp/internal/topicportal"
"github.com/kafkamcp/kafkamcp/internal/vectorsync"
"github.com/kafkamcp/kafkamcp/internal/viewstore"
"github.com/kafkamcp/kafkamcp/internal/watch"
"github.com/kafkamcp/kafkamcp/internal/workflow"
"github.com/kafkamcp/kafkamcp/internal/persona"
	"github.com/kafkamcp/kafkamcp/internal/tenant"
"github.com/mark3labs/mcp-go/server"
)

// Analytics groups AI-powered analysis and intelligence subsystems.
type Analytics struct {
	anomalyDetector *anomaly.Detector    // AI-powered anomaly detection
	nlQueryEngine   *nlquery.Engine      // natural language query engine
	costAnalyzer    *cost.Analyzer       // Kafka cost analysis
	forecaster      *forecast.Forecaster // intelligent capacity forecasting
	knowledgeGraph  *graph.KnowledgeGraph // embedded knowledge graph
	lagAnalyzer     *lagdiag.Analyzer    // consumer lag root cause analysis
	schemaWorkflow      *evolution.WorkflowEngine   // schema evolution workflow engine
	timeseriesAnalyzer  *anomaly.TimeSeriesAnalyzer // time-series analytics
	advisorEngine   *advisor.Engine      // intelligent topic advisor
	dataQualityEngine   *dataquality.Engine       // data quality scoring & SLA
	incidentCorrelator  *incident.Correlator      // cross-cluster incident correlation
}

// Governance groups policy, audit, security, and compliance subsystems.
type Governance struct {
	policyEngine     *auth.PolicyEngine
	auditLogger      *audit.Logger
	rateLimiter      *ratelimit.Limiter
	maskingEngine    *masking.Engine    // PII redaction engine
	contractEngine   *contracts.Engine  // schema-aware data contracts
	personaEngine    *persona.Engine    // declarative agent personas
	tenantManager    *tenant.Manager    // multi-tenant management
	usageTracker     *tenant.UsageTracker // usage analytics tracking
	complianceEngine *compliance.Engine // compliance & regulatory reporting
	threatDetector   *agentprofile.Detector // agent behavioral threat detection
	retentionEngine  *retention.Engine      // intelligent retention policy
}

// Streaming groups event-driven and stream-processing subsystems.
type Streaming struct {
	watchManager   *watch.Manager      // event-driven watch subscriptions
	dlqRemediator  *dlq.Remediator     // DLQ auto-remediation
	replicationMon *replication.Monitor // cross-cluster replication monitoring
	selfHealMon    *selfheal.Monitor   // self-healing consumer groups
	lineageTracker *lineage.Tracker        // topic data lineage
	messageTracer  *lineage.MessageTracer  // distributed message tracing
	flowDebugger   *flowdebug.Debugger     // live message flow debugger
}

// Platform groups infrastructure, orchestration, and extensibility subsystems.
type Platform struct {
	pluginRegistry   *plugin.Registry    // plugin/extension system
	agentRegistry    *kafka.AgentRegistry // multi-agent collaboration
	otelProvider     *telemetry.Provider // OpenTelemetry trace provider
	platformMgr      *platform.Manager   // multi-platform abstraction
	gitopsManager    *gitops.Manager     // GitOps configuration management
	migrationPlanner *migration.Planner  // kafka migration assistant
	workflowEngine   *workflow.Engine    // agent workflow templates
	gatewayManager   *gateway.Manager    // multi-protocol streaming gateway
	collaborationCoord *collaboration.Coordinator // agent collaboration
	semanticEngine     *semantic.Engine           // semantic search & RAG
	onboardingEngine   *onboarding.Engine         // zero-config onboarding
	opaPolicyEngine    *opa.PolicyEngine          // OPA policy-as-code engine
	operatorController *operator.Controller       // Kubernetes operator
	pipelineEngine     *pipeline.Engine           // streaming function pipelines
	marketplace        *plugin.Marketplace        // plugin marketplace
	observabilityMgr   *observability.Manager     // observability dashboards & alerts
	chaosEngine        *chaos.Engine              // chaos engineering toolkit
	replayEngine       *replay.Engine             // interactive replay & time-travel
	federationCoord    *federation.Coordinator    // federated multi-instance coordination
	agentProfiler      *profiler.Profiler         // agent performance profiler
	dagWorkflowEngine  *dagworkflow.Engine        // DAG-based workflow orchestrator
	viewStoreEngine    *viewstore.Engine          // materialized view store
	agentMemoryEngine  *agentmemory.Engine        // per-agent persistent memory
	taskQueueEngine    *taskqueue.Engine          // multi-agent task queue
	streamSQLEngine    *streamsql.Engine          // streaming SQL engine
	catalogEngine      *catalog.Engine            // data catalog & discovery
	adaptiveRateEngine *adaptiverate.Engine       // ML-driven rate limiting
	configVCSEngine    *configvcs.Engine          // config version control
	costAttrEngine     *costattribution.Engine    // real-time cost attribution
	sandboxEngine      *sandbox.Engine            // agent sandbox environments
	agentConfigStore   *plugin.AgentConfigStore   // agent config marketplace
	cdcEngine          *cdc.Engine                // CDC bridge engine
	orchestrationEngine *orchestration.Engine      // multi-agent orchestration
	schemaAdvisor       *schemaadvisor.Advisor     // schema evolution advisor
	nlPipelineBuilder   *nlpipeline.Builder        // NL data pipeline builder
	playbookEngine      *playbook.Engine           // anomaly response playbooks
	cloudDeployGenerator *clouddeploy.Generator    // cloud deployment generator
	vectorSyncEngine    *vectorsync.Engine         // vector DB sync bridge
	finOpsDashboard     *finops.Dashboard          // FinOps cost intelligence
	codecEngine         *codec.Engine              // Avro/Protobuf wire codec
	eventSourcingEngine *eventsourcing.Engine       // agent action event sourcing
	topicPortalEngine   *topicportal.Engine         // self-service topic portal
	gitopsReconciler    *gitops.Reconciler          // GitOps reconciliation loop
	llmPipelineEngine   *llmpipeline.Engine         // LLM training data pipeline
	chatEngine          *chat.Engine                // conversational REPL chat engine
	smartRouter         *smartroute.Router          // intelligent tool routing & composition
	timeseriesEngine    *timeseries.Engine          // time-series analytics engine
	controlPlane        *controlplane.Manager       // multi-tenant SaaS control plane
	schemaPlayground    *schemaplayground.Engine    // interactive schema playground
	capabilityNegotiator *capability.Negotiator    // agent capability negotiation
	digitalTwin         *digitaltwin.Engine        // cluster digital twin simulation
	correlationEngine   *correlation.Engine        // observability correlation engine
	devModeEngine       *devmode.Engine            // offline-first local dev mode
}

// Server is the KafkaMCP MCP server.
type Server struct {
	// Core fields
	config         *config.Config
	kafkaManager   kafka.KafkaManager
	schemaClients  map[string]*schema.Client      // per-cluster schema clients
	connectMu      sync.RWMutex
	connectClients map[string]*kafka.ConnectClient // cached per-cluster Connect clients
	metrics        *metrics.Metrics
	logger         *slog.Logger
	mcpServer      *server.MCPServer
	expensiveOps   chan struct{} // semaphore for expensive operations (aggregate, join, search)

	// Composite subsystems (embedded for direct field access)
	Analytics
	Governance
	Streaming
	Platform
}

// New creates a new KafkaMCP server. Functional options override default
// subsystem instances (e.g., WithMetrics, WithAuditLogger).
func New(cfg *config.Config, kafkaManager kafka.KafkaManager, logger *slog.Logger, version string, opts ...Option) (*Server, error) {
// Per-cluster schema registry clients
schemaClients := make(map[string]*schema.Client)
for _, cl := range cfg.Clusters {
	if cl.SchemaRegistry.URL != "" {
		sc, err := schema.NewClient(
			cl.SchemaRegistry.URL,
			cl.SchemaRegistry.Auth.Username,
			cl.SchemaRegistry.Auth.Password,
			cfg.SchemaCache.MaxEntries,
			cfg.SchemaCache.TTLSeconds,
		)
		if err != nil {
			return nil, fmt.Errorf("schema registry for cluster %s: %w", cl.Name, err)
		}
		schemaClients[cl.Name] = sc
	}
}

policyEngine := auth.NewPolicyEngine(cfg.Policies)

auditLogger, err := audit.NewLogger(cfg.Audit.Enabled, cfg.Audit.MaxEntries, cfg.Audit.LogFile, logger)
if err != nil {
return nil, fmt.Errorf("creating audit logger: %w", err)
}

rl := ratelimit.New(0)
for _, agent := range cfg.Policies.Agents {
if agent.RateLimit.RequestsPerMinute > 0 {
rl.SetAgentLimit(agent.ID, agent.RateLimit.RequestsPerMinute)
}
}

s := &Server{
config:         cfg,
kafkaManager:   kafkaManager,
schemaClients:  schemaClients,
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
	schemaWorkflow:     evolution.NewWorkflowEngine(),
	timeseriesAnalyzer: anomaly.NewTimeSeriesAnalyzer(),
	advisorEngine:      advisor.NewEngine(logger),
	dataQualityEngine:  dataquality.NewEngine(logger),
	incidentCorrelator: incident.NewCorrelator(incident.DefaultCorrelatorConfig(), logger),
},
Governance: Governance{
	policyEngine:   policyEngine,
	auditLogger:    auditLogger,
	rateLimiter:    rl,
	maskingEngine:  masking.NewEngine(cfg.Masking, logger),
	contractEngine: contracts.NewEngine(),
	personaEngine:  persona.NewEngine(),
	tenantManager:  tenant.NewManager(),
	usageTracker:     tenant.NewUsageTracker(),
	complianceEngine: compliance.NewEngine(logger),
	threatDetector:   agentprofile.NewDetector(agentprofile.DefaultDetectorConfig(), logger),
	retentionEngine:  retention.NewEngine(logger),
},
Streaming: Streaming{
	watchManager:   watch.NewManager(logger),
	dlqRemediator:  dlq.NewRemediator(logger),
	replicationMon: replication.NewMonitor(logger),
	selfHealMon:    selfheal.NewMonitor(),
	lineageTracker: lineage.NewTracker(),
	messageTracer:  lineage.NewMessageTracer(),
	flowDebugger:   flowdebug.NewDebugger(logger),
},
Platform: Platform{
	pluginRegistry:   plugin.NewRegistry(logger),
	agentRegistry:    kafka.NewAgentRegistry(),
	otelProvider:     nil,
	platformMgr:      platform.NewManager(logger),
	gitopsManager:    gitops.NewManager(logger),
	migrationPlanner: migration.NewPlanner(),
	workflowEngine:   workflow.NewEngine(logger),
	gatewayManager:   gateway.NewManager(logger),
	collaborationCoord: collaboration.NewCoordinator(logger),
	semanticEngine:     semantic.NewEngine(logger),
	onboardingEngine:   onboarding.NewEngine(logger),
	opaPolicyEngine:    opa.NewPolicyEngine(logger),
	operatorController: operator.NewController(logger),
	pipelineEngine:     pipeline.NewEngine(logger),
	observabilityMgr:   observability.NewManager(),
	chaosEngine:        chaos.NewEngine(logger),
	replayEngine:       replay.NewEngine(logger),
	federationCoord:    federation.NewCoordinator("local", logger),
	agentProfiler:      profiler.NewProfiler(),
	dagWorkflowEngine:  dagworkflow.NewEngine(logger),
	viewStoreEngine:    viewstore.NewEngine(logger),
	agentMemoryEngine:  agentmemory.NewEngine(logger),
	taskQueueEngine:    taskqueue.NewEngine(logger),
	streamSQLEngine:    streamsql.NewEngine(logger),
	catalogEngine:      catalog.NewEngine(logger),
	adaptiveRateEngine: adaptiverate.NewEngine(logger),
	configVCSEngine:    configvcs.NewEngine(logger),
	costAttrEngine:     costattribution.NewEngine(logger),
	sandboxEngine:      sandbox.NewEngine(logger),
	agentConfigStore:   plugin.NewAgentConfigStore(),
	cdcEngine:          cdc.NewEngine(logger),
	orchestrationEngine: orchestration.NewEngine(logger),
	schemaAdvisor:       schemaadvisor.NewAdvisor(logger),
	nlPipelineBuilder:   nlpipeline.NewBuilder(logger),
	playbookEngine:      playbook.NewEngine(logger),
	cloudDeployGenerator: clouddeploy.NewGenerator(logger),
	vectorSyncEngine:    vectorsync.NewEngine(logger),
	finOpsDashboard:     finops.NewDashboard(logger),
	codecEngine:         codec.NewEngine(nil),
	eventSourcingEngine: eventsourcing.NewEngine(logger),
	topicPortalEngine:   topicportal.NewEngine(topicportal.DefaultApprovalChainConfig(), logger),
	gitopsReconciler:    gitops.NewReconciler(gitops.DefaultReconcilerConfig(), gitops.NewManager(logger), logger),
	llmPipelineEngine:   llmpipeline.NewEngine(logger),
	chatEngine:          chat.NewEngine(logger),
	smartRouter:         smartroute.NewRouter(logger),
	timeseriesEngine:    timeseries.NewEngine(logger),
	controlPlane:        controlplane.NewManager(logger),
	schemaPlayground:    schemaplayground.NewEngine(logger),
	capabilityNegotiator: capability.NewNegotiator(logger),
	digitalTwin:         digitaltwin.NewEngine(logger),
	correlationEngine:   correlation.NewEngine(logger),
	devModeEngine:       devmode.NewEngine(logger),
},
}

s.marketplace = plugin.NewMarketplace(s.pluginRegistry)

// Apply functional options, overriding defaults
for _, opt := range opts {
	opt(s)
}

mcpServer := server.NewMCPServer(
"KafkaMCP",
version,
server.WithToolCapabilities(true),
server.WithResourceCapabilities(true, true),
)

s.mcpServer = mcpServer
s.registerTools()
s.registerResources()

return s, nil
}

// MCPServer returns the underlying MCP server.
func (s *Server) MCPServer() *server.MCPServer { return s.mcpServer }

// Metrics returns the Prometheus metrics.
func (s *Server) Metrics() *metrics.Metrics { return s.metrics }

// HealthCheck verifies Kafka connectivity by pinging the default cluster.
func (s *Server) HealthCheck(ctx context.Context) error {
	return s.kafkaManager.Ping(ctx)
}

// SetOTelProvider sets the OpenTelemetry provider for trace instrumentation.
func (s *Server) SetOTelProvider(p *telemetry.Provider) {
	s.otelProvider = p
}

// Close shuts down the server and all subsystems.
func (s *Server) Close() error {
s.kafkaManager.Close()
if s.watchManager != nil {
	s.watchManager.Close()
}
if s.anomalyDetector != nil {
	s.anomalyDetector.Stop()
}
if s.otelProvider != nil {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.otelProvider.Shutdown(shutdownCtx); err != nil {
		s.logger.Error("failed to shutdown OTel provider", "error", err)
	}
}
if s.pluginRegistry != nil {
	s.pluginRegistry.Close()
}
if s.chaosEngine != nil {
	s.chaosEngine.Close()
}
return s.auditLogger.Close()
}

// getSchemaClient returns the schema client for a cluster, falling back to default.
func (s *Server) getSchemaClient(cluster string) *schema.Client {
name := cluster
if name == "" {
	name = s.kafkaManager.DefaultClusterName()
}
if c, ok := s.schemaClients[name]; ok {
	return c
}
// Fall back to default cluster's client
if c, ok := s.schemaClients[s.kafkaManager.DefaultClusterName()]; ok {
	return c
}
return nil
}

func (s *Server) hasSchemaRegistry() bool {
return len(s.schemaClients) > 0
}

// getConnectClient returns a cached ConnectClient for the given cluster config.
// Clients are created once and reused across requests.
func (s *Server) getConnectClient(clCfg *config.ClusterConfig) (*kafka.ConnectClient, error) {
	if clCfg.ConnectURL == "" {
		return nil, nil
	}
	s.connectMu.RLock()
	if c, ok := s.connectClients[clCfg.Name]; ok {
		s.connectMu.RUnlock()
		return c, nil
	}
	s.connectMu.RUnlock()

	s.connectMu.Lock()
	defer s.connectMu.Unlock()
	// Double-check after acquiring write lock
	if c, ok := s.connectClients[clCfg.Name]; ok {
		return c, nil
	}
	c, err := kafka.NewConnectClient(clCfg.ConnectURL, connectTLSOpts(clCfg)...)
	if err != nil {
		return nil, err
	}
	if clCfg.ConnectTLS.InsecureSkipVerify {
		s.logger.Warn("Kafka Connect client using InsecureSkipVerify - not recommended for production", "cluster", clCfg.Name)
	}
	if c != nil {
		s.connectClients[clCfg.Name] = c
	}
	return c, nil
}

// matchTopicFilter checks whether a topic name matches a glob pattern.
func matchTopicFilter(pattern, name string) bool {
	return match.Glob(pattern, name)
}

// acquireExpensiveOp blocks until a slot is available for an expensive operation,
// or returns an error if the context is cancelled while waiting.
func (s *Server) acquireExpensiveOp(ctx context.Context) error {
	select {
	case s.expensiveOps <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("timed out waiting for available capacity; too many concurrent expensive operations")
	}
}

// releaseExpensiveOp releases a slot from the expensive operation semaphore.
func (s *Server) releaseExpensiveOp() {
	<-s.expensiveOps
}

// resolveClusterName returns the given cluster name if it maps to a valid
// configured cluster, otherwise falls back to the default cluster.
func (s *Server) resolveClusterName(cluster string) string {
	if cluster != "" {
		if _, err := s.kafkaManager.GetClusterConfig(cluster); err == nil {
			return cluster
		}
		s.logger.Warn("invalid cluster name, falling back to default", "requested", cluster, "default", s.kafkaManager.DefaultClusterName())
		return s.kafkaManager.DefaultClusterName()
	}
	return s.kafkaManager.DefaultClusterName()
}

// clampInt64 constrains a value to [min, max].
func clampInt64(v, min, max int64) int64 {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

// Tool and resource handler implementations are in:
// - tools_p0.go           (P0 handlers: list topics, describe, consume, produce, schemas, consumer groups)
// - tools_p1.go           (P1 handlers: create/alter/delete topic, search, reset offsets, DLQ, subscribe)
// - tools_p2.go           (P2 handlers: aggregate, schema evolution, connectors, join, NL query)
// - tools_p3.go           (P3 handlers: watch, DLQ remediation, schema diff, streaming, agent registry)
// - tools_advanced.go     (Advanced handlers: connector CRUD, time travel, recommend, trace, ACLs, config drift, playground)
// - tools_features.go     (Feature handlers: anomalies, NL query, cost analysis, workflows, GitOps, knowledge graph, contracts)
// - tools_streaming.go    (Streaming consumer session management)
// - tools_new_features.go (New features: lag diagnosis, migration, lineage, group health, personas, forecasting)
// - resources.go          (Resource handlers: cluster, topics, schemas, groups, audit, graph)
