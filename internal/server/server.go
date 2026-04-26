package server

import (
"context"
"encoding/json"
"fmt"
"log/slog"
"strings"
"sync"
"time"

"github.com/kafkamcp/kafkamcp/internal/adaptiverate"
"github.com/kafkamcp/kafkamcp/internal/agentmemory"
"github.com/kafkamcp/kafkamcp/internal/anomaly"
"github.com/kafkamcp/kafkamcp/internal/advisor"
"github.com/kafkamcp/kafkamcp/internal/audit"
"github.com/kafkamcp/kafkamcp/internal/auth"
"github.com/kafkamcp/kafkamcp/internal/catalog"
"github.com/kafkamcp/kafkamcp/internal/chaos"
"github.com/kafkamcp/kafkamcp/internal/collaboration"
"github.com/kafkamcp/kafkamcp/internal/compliance"
"github.com/kafkamcp/kafkamcp/internal/config"
"github.com/kafkamcp/kafkamcp/internal/configvcs"
"github.com/kafkamcp/kafkamcp/internal/contracts"
"github.com/kafkamcp/kafkamcp/internal/cost"
"github.com/kafkamcp/kafkamcp/internal/costattribution"
"github.com/kafkamcp/kafkamcp/internal/dagworkflow"
"github.com/kafkamcp/kafkamcp/internal/dlq"
"github.com/kafkamcp/kafkamcp/internal/evolution"
"github.com/kafkamcp/kafkamcp/internal/federation"
"github.com/kafkamcp/kafkamcp/internal/flowdebug"
"github.com/kafkamcp/kafkamcp/internal/forecast"
"github.com/kafkamcp/kafkamcp/internal/gateway"
"github.com/kafkamcp/kafkamcp/internal/gitops"
"github.com/kafkamcp/kafkamcp/internal/graph"
"github.com/kafkamcp/kafkamcp/internal/kafka"
"github.com/kafkamcp/kafkamcp/internal/lagdiag"
"github.com/kafkamcp/kafkamcp/internal/lineage"
"github.com/kafkamcp/kafkamcp/internal/masking"
"github.com/kafkamcp/kafkamcp/internal/match"
"github.com/kafkamcp/kafkamcp/internal/metrics"
"github.com/kafkamcp/kafkamcp/internal/migration"
"github.com/kafkamcp/kafkamcp/internal/nlquery"
"github.com/kafkamcp/kafkamcp/internal/observability"
"github.com/kafkamcp/kafkamcp/internal/onboarding"
"github.com/kafkamcp/kafkamcp/internal/opa"
"github.com/kafkamcp/kafkamcp/internal/operator"
"github.com/kafkamcp/kafkamcp/internal/pipeline"
"github.com/kafkamcp/kafkamcp/internal/platform"
"github.com/kafkamcp/kafkamcp/internal/plugin"
"github.com/kafkamcp/kafkamcp/internal/profiler"
"github.com/kafkamcp/kafkamcp/internal/ratelimit"
"github.com/kafkamcp/kafkamcp/internal/replay"
"github.com/kafkamcp/kafkamcp/internal/replication"
"github.com/kafkamcp/kafkamcp/internal/sandbox"
"github.com/kafkamcp/kafkamcp/internal/schema"
"github.com/kafkamcp/kafkamcp/internal/selfheal"
"github.com/kafkamcp/kafkamcp/internal/semantic"
"github.com/kafkamcp/kafkamcp/internal/streamsql"
"github.com/kafkamcp/kafkamcp/internal/taskqueue"
"github.com/kafkamcp/kafkamcp/internal/telemetry"
"github.com/kafkamcp/kafkamcp/internal/viewstore"
"github.com/kafkamcp/kafkamcp/internal/watch"
"github.com/kafkamcp/kafkamcp/internal/workflow"
"github.com/kafkamcp/kafkamcp/internal/persona"
	"github.com/kafkamcp/kafkamcp/internal/tenant"
"github.com/mark3labs/mcp-go/mcp"
"github.com/mark3labs/mcp-go/server"
otelTrace "go.opentelemetry.io/otel/trace"
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

// extractAgentID gets the agent ID from the MCP request headers or defaults.
func extractAgentID(request mcp.CallToolRequest) string {
if request.Header != nil {
if id := request.Header.Get("X-Agent-ID"); id != "" {
return id
}
if id := request.Header.Get("X-Client-ID"); id != "" {
return id
}
}
return "anonymous"
}

// extractAgentIDFromResource gets the agent ID from a resource request.
func extractAgentIDFromResource(request mcp.ReadResourceRequest) string {
if request.Header != nil {
if id := request.Header.Get("X-Agent-ID"); id != "" {
return id
}
if id := request.Header.Get("X-Client-ID"); id != "" {
return id
}
}
return "anonymous"
}

// validateAgentID checks that the claimed agent ID exists in policy config when default_deny is true.
func (s *Server) validateAgentID(agentID string) *mcp.CallToolResult {
if !s.policyEngine.DefaultDeny() {
	return nil
}
if agentID == "anonymous" || !s.policyEngine.AgentExists(agentID) {
	return mcp.NewToolResultError("Authentication required: valid X-Agent-ID header must be provided")
}
return nil
}



// --- Auth & Rate Limit helpers ---

// toolAuthConfig defines the authorization requirements for a tool handler,
// specifying which resource type and action to check against the policy engine.
type toolAuthConfig struct {
	tool         string
	resourceType auth.ResourceType
	action       auth.Action
	// resourceName extracts the resource name from the request.
	// If nil, defaults to "*".
	resourceName func(mcp.CallToolRequest) string
}

// toolHandler is a handler function that processes an MCP tool call after
// agent identity, rate limiting, and authorization have been validated.
type toolHandler func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error)

// withToolAuth wraps a tool handler with cross-cutting concerns: agent ID
// extraction, OpenTelemetry span creation, rate limiting, authorization
// checks, and audit logging. It eliminates repeated boilerplate across handlers.
func (s *Server) withToolAuth(cfg toolAuthConfig, handler toolHandler) func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		start := time.Now()
		agentID := extractAgentID(request)
		args := request.GetArguments()

		// Start OTel span if provider is configured
		if s.otelProvider != nil {
			cluster := ""
			if args != nil {
				if c, ok := args["cluster"].(string); ok {
					cluster = c
				}
			}
			var otelSpan otelTrace.Span
			ctx, otelSpan = s.otelProvider.StartToolSpan(ctx, cfg.tool, agentID, cluster)
			defer otelSpan.End()
		}

		if deny := s.checkRateLimit(agentID, cfg.tool); deny != nil {
			return deny, nil
		}

		resourceName := "*"
		if cfg.resourceName != nil {
			resourceName = cfg.resourceName(request)
		}
		if deny := s.checkAuth(agentID, cfg.tool, cfg.resourceType, resourceName, cfg.action); deny != nil {
			return deny, nil
		}

		result, err := handler(ctx, request, agentID, args, start)
		return result, err
	}
}

// checkAuth verifies that the given agent is authorized to perform the specified
// action on the resource. Returns a denial result if unauthorized, nil if allowed.
func (s *Server) checkAuth(agentID, tool string, resource auth.ResourceType, resourceName string, action auth.Action) *mcp.CallToolResult {
if deny := s.validateAgentID(agentID); deny != nil {
	return deny
}
if !s.policyEngine.Authorize(agentID, resource, resourceName, action) {
s.metrics.AuthzDenials.WithLabelValues(metrics.SanitizeAgentLabel(agentID), tool, string(action)).Inc()
s.logger.Warn("authorization denied", "agent", agentID, "tool", tool, "resource", resourceName, "action", action)
return mcp.NewToolResultError("Unauthorized: insufficient permission for this operation")
}
return nil
}

// checkRateLimit checks whether the agent has exceeded its rate limit.
// Returns a denial result if rate-limited, nil if the request is allowed.
func (s *Server) checkRateLimit(agentID, tool string) *mcp.CallToolResult {
if !s.rateLimiter.Allow(agentID) {
s.metrics.RateLimitDenials.WithLabelValues(metrics.SanitizeAgentLabel(agentID), tool).Inc()
return mcp.NewToolResultError("Rate limit exceeded. Please retry later.")
}
return nil
}

// checkResourceRateLimit checks rate limits for resource handlers.
func (s *Server) checkResourceRateLimit(agentID, resource string) error {
if !s.rateLimiter.Allow(agentID) {
s.metrics.RateLimitDenials.WithLabelValues(metrics.SanitizeAgentLabel(agentID), resource).Inc()
return fmt.Errorf("rate limit exceeded")
}
return nil
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
	// Only return the cluster name if it's a valid configured cluster
	if _, err := s.kafkaManager.GetClusterConfig(cluster); err == nil {
		return cluster
	}
	s.logger.Warn("invalid cluster name, falling back to default", "requested", cluster, "default", s.kafkaManager.DefaultClusterName())
	return s.kafkaManager.DefaultClusterName()
}
return s.kafkaManager.DefaultClusterName()
}

// logAudit records a tool call in the audit log and increments Prometheus metrics.
func (s *Server) logAudit(agentID, tool string, params map[string]any, cluster, status string, count int, err error, start time.Time) {
latency := time.Since(start)
entry := audit.Entry{
AgentID:    agentID,
Tool:       tool,
Parameters: sanitizeAuditParams(tool, params),
Cluster:    s.resolveClusterName(cluster),
Result:     audit.Result{Status: status, MessageCount: count},
LatencyMs:  latency.Milliseconds(),
}
if err != nil {
entry.Result.Error = categorizeError(err)
}
s.auditLogger.Log(entry)
safeAgent := metrics.SanitizeAgentLabel(agentID)
s.metrics.ToolCallsTotal.WithLabelValues(tool, safeAgent, entry.Cluster, status).Inc()
s.metrics.ToolLatencySeconds.WithLabelValues(tool, safeAgent, entry.Cluster).Observe(latency.Seconds())

// Record for agent profiling
if s.agentProfiler != nil {
	s.agentProfiler.RecordCall(tool, agentID, nil, latency.Milliseconds(), status)
}
}

// categorizeError maps raw errors to safe category strings to prevent
// leaking internal details (broker IPs, TLS info, connection strings)
// in audit logs and dashboard responses.
func categorizeError(err error) string {
msg := err.Error()
lower := strings.ToLower(msg)

switch {
case strings.Contains(lower, "context deadline exceeded") || strings.Contains(lower, "timeout") || strings.Contains(lower, "timed out"):
	return "timeout"
case strings.Contains(lower, "connection refused") || strings.Contains(lower, "connection reset") || strings.Contains(lower, "broken pipe") || strings.Contains(lower, "no such host") || strings.Contains(lower, "dial tcp"):
	return "connection_error"
case strings.Contains(lower, "sasl") || strings.Contains(lower, "authentication") || strings.Contains(lower, "unauthorized") || strings.Contains(lower, "credential"):
	return "auth_error"
case strings.Contains(lower, "tls") || strings.Contains(lower, "certificate") || strings.Contains(lower, "x509"):
	return "tls_error"
case strings.Contains(lower, "permission") || strings.Contains(lower, "forbidden") || strings.Contains(lower, "not authorized") || strings.Contains(lower, "acl"):
	return "permission_denied"
case strings.Contains(lower, "not found") || strings.Contains(lower, "unknown topic") || strings.Contains(lower, "does not exist"):
	return "not_found"
case strings.Contains(lower, "already exists") || strings.Contains(lower, "duplicate"):
	return "already_exists"
case strings.Contains(lower, "invalid") || strings.Contains(lower, "malformed"):
	return "invalid_request"
default:
	return "internal_error"
}
}

// sanitizeAuditParams redacts sensitive data from audit log parameters.
// It applies tool-specific redactions and deep-inspects all parameters for
// sensitive key patterns (password, secret, token, key, credential).
func sanitizeAuditParams(tool string, params map[string]any) map[string]any {
if params == nil {
	return nil
}

sensitiveTools := map[string][]string{
	"kafka_produce":                  {"messages"},
	"kafka_consume":                  {"messages"},
	"kafka_alter_topic":              {"configs"},
	"kafka_create_topic":             {"configs"},
	"kafka_check_schema_compatibility": {"schema"},
	"kafka_describe_connector":       {"name"},
}

sanitized := make(map[string]any, len(params))
for k, v := range params {
	sanitized[k] = v
}

// Tool-specific redactions
if keys, ok := sensitiveTools[tool]; ok {
	for _, key := range keys {
		if _, exists := sanitized[key]; exists {
			sanitized[key] = "**REDACTED**"
		}
	}
}

// Deep-redact any parameter whose key matches sensitive patterns
deepRedactSensitiveKeys(sanitized)

return sanitized
}

// sensitiveKeyPatterns contains substrings that indicate a parameter key holds sensitive data.
var sensitiveKeyPatterns = []string{"password", "secret", "token", "key", "credential", "api_key", "apikey", "webhook_url", "sasl", "bearer", "jwt", "auth", "connection_string"}

// deepRedactSensitiveKeys recursively inspects map values and redacts any key
// matching sensitive patterns.
func deepRedactSensitiveKeys(m map[string]any) {
for k, v := range m {
	lower := strings.ToLower(k)
	isSensitive := false
	for _, pat := range sensitiveKeyPatterns {
		if strings.Contains(lower, pat) {
			isSensitive = true
			break
		}
	}
	if isSensitive {
		m[k] = "**REDACTED**"
		continue
	}
	// Recurse into nested maps
	if nested, ok := v.(map[string]any); ok {
		deepRedactSensitiveKeys(nested)
	}
}
}

// toolError logs the raw error server-side and returns a generic error message
// to the client, preventing leakage of internal details like broker IPs,
// TLS certificate info, connection strings, and file system paths.
func (s *Server) toolError(msg string, err error) *mcp.CallToolResult {
	s.logger.Error(msg, "error", err)
	return mcp.NewToolResultError(msg)
}

// redactURL masks a URL for safe inclusion in responses and audit logs,
// preserving only the scheme and host.
func redactURL(rawURL string) string {
	if rawURL == "" {
		return ""
	}
	// Show scheme + host, redact path/query
	if idx := strings.Index(rawURL, "://"); idx >= 0 {
		rest := rawURL[idx+3:]
		if slashIdx := strings.Index(rest, "/"); slashIdx >= 0 {
			return rawURL[:idx+3+slashIdx] + "/***"
		}
		return rawURL + "/***"
	}
	return "***"
}

// marshalResult serializes a value as indented JSON and wraps it in an MCP tool result.
func marshalResult(v any) (*mcp.CallToolResult, error) {
data, err := json.MarshalIndent(v, "", "  ")
if err != nil {
slog.Error("failed to marshal tool result", "error", err)
return mcp.NewToolResultError("internal error: failed to format response"), nil
}
return mcp.NewToolResultText(string(data)), nil
}

// marshalResourceJSON serializes a value as JSON resource contents for MCP resource responses.
func marshalResourceJSON(uri string, v any) ([]mcp.ResourceContents, error) {
data, err := json.MarshalIndent(v, "", "  ")
if err != nil {
return nil, fmt.Errorf("marshaling resource: %w", err)
}
return []mcp.ResourceContents{
mcp.TextResourceContents{URI: uri, MIMEType: "application/json", Text: string(data)},
}, nil
}

// validateResourceAgentID validates agent identity for resource handlers.
func (s *Server) validateResourceAgentID(agentID string) error {
	if !s.policyEngine.DefaultDeny() {
		return nil
	}
	if agentID == "anonymous" || !s.policyEngine.AgentExists(agentID) {
		return fmt.Errorf("authentication required: valid X-Agent-ID header must be provided")
	}
	return nil
}

// matchTopicFilter checks whether a topic name matches a glob pattern.
func matchTopicFilter(pattern, name string) bool {
	return match.Glob(pattern, name)
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

// validateStringLength checks that a string parameter doesn't exceed the maximum length.
func validateStringLength(name, value string) *mcp.CallToolResult {
	if len(value) > maxInputLength {
		return mcp.NewToolResultError(fmt.Sprintf("%s exceeds maximum length of %d characters", name, maxInputLength))
	}
	return nil
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
