package server

import (
"context"
"encoding/json"
"fmt"
"log/slog"
"sync"
"time"

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
"github.com/kafkamcp/kafkamcp/internal/match"
"github.com/kafkamcp/kafkamcp/internal/metrics"
"github.com/kafkamcp/kafkamcp/internal/migration"
"github.com/kafkamcp/kafkamcp/internal/nlquery"
"github.com/kafkamcp/kafkamcp/internal/platform"
"github.com/kafkamcp/kafkamcp/internal/plugin"
"github.com/kafkamcp/kafkamcp/internal/ratelimit"
"github.com/kafkamcp/kafkamcp/internal/replication"
"github.com/kafkamcp/kafkamcp/internal/schema"
"github.com/kafkamcp/kafkamcp/internal/selfheal"
"github.com/kafkamcp/kafkamcp/internal/telemetry"
"github.com/kafkamcp/kafkamcp/internal/watch"
"github.com/kafkamcp/kafkamcp/internal/workflow"
"github.com/kafkamcp/kafkamcp/internal/persona"
"github.com/mark3labs/mcp-go/mcp"
"github.com/mark3labs/mcp-go/server"
otelTrace "go.opentelemetry.io/otel/trace"
)

// Server is the KafkaMCP MCP server.
type Server struct {
config       *config.Config
kafkaManager kafka.KafkaManager
schemaClients map[string]*schema.Client // per-cluster schema clients
connectMu      sync.RWMutex
connectClients map[string]*kafka.ConnectClient // cached per-cluster Connect clients
policyEngine *auth.PolicyEngine
auditLogger  *audit.Logger
rateLimiter  *ratelimit.Limiter
metrics      *metrics.Metrics
logger       *slog.Logger
mcpServer    *server.MCPServer
expensiveOps chan struct{} // semaphore for expensive operations (aggregate, join, search)
watchManager   *watch.Manager       // event-driven watch subscriptions
maskingEngine  *masking.Engine       // PII redaction engine
dlqRemediator  *dlq.Remediator       // DLQ auto-remediation
replicationMon *replication.Monitor   // cross-cluster replication monitoring
pluginRegistry *plugin.Registry      // plugin/extension system
agentRegistry  *kafka.AgentRegistry  // multi-agent collaboration
otelProvider   *telemetry.Provider   // OpenTelemetry trace provider
anomalyDetector *anomaly.Detector    // AI-powered anomaly detection
nlQueryEngine  *nlquery.Engine       // natural language query engine
workflowEngine *workflow.Engine      // agent workflow templates
costAnalyzer   *cost.Analyzer        // Kafka cost analysis
gitopsManager  *gitops.Manager       // GitOps configuration management
knowledgeGraph *graph.KnowledgeGraph // embedded knowledge graph
platformMgr    *platform.Manager     // multi-platform abstraction
contractEngine *contracts.Engine     // schema-aware data contracts
lagAnalyzer    *lagdiag.Analyzer     // consumer lag root cause analysis
migrationPlanner *migration.Planner  // kafka migration assistant
lineageTracker *lineage.Tracker      // topic data lineage
selfHealMon    *selfheal.Monitor     // self-healing consumer groups
personaEngine  *persona.Engine       // declarative agent personas
forecaster     *forecast.Forecaster  // intelligent capacity forecasting
}

// New creates a new KafkaMCP server. Functional options override default
// subsystem instances (e.g., WithMetrics, WithAuditLogger).
func New(cfg *config.Config, kafkaManager kafka.KafkaManager, logger *slog.Logger, version string, opts ...Option) (*Server, error) {
// Per-cluster schema registry clients
schemaClients := make(map[string]*schema.Client)
for _, cl := range cfg.Clusters {
	if cl.SchemaRegistry.URL != "" {
		schemaClients[cl.Name] = schema.NewClient(
			cl.SchemaRegistry.URL,
			cl.SchemaRegistry.Auth.Username,
			cl.SchemaRegistry.Auth.Password,
			cfg.SchemaCache.MaxEntries,
			cfg.SchemaCache.TTLSeconds,
		)
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
policyEngine:   policyEngine,
auditLogger:    auditLogger,
rateLimiter:    rl,
metrics:        metrics.New(),
logger:         logger,
expensiveOps:   make(chan struct{}, 5),
watchManager:   watch.NewManager(logger),
maskingEngine:  masking.NewEngine(cfg.Masking, logger),
dlqRemediator:  dlq.NewRemediator(logger),
replicationMon: replication.NewMonitor(logger),
pluginRegistry: plugin.NewRegistry(logger),
agentRegistry:  kafka.NewAgentRegistry(),
otelProvider:   nil,
anomalyDetector: anomaly.NewDetector(anomaly.DefaultConfig(), logger),
nlQueryEngine:  nlquery.NewEngine(logger),
workflowEngine: workflow.NewEngine(logger),
costAnalyzer:   cost.NewAnalyzer(logger),
gitopsManager:  gitops.NewManager(logger),
knowledgeGraph: graph.NewKnowledgeGraph(logger),
platformMgr:    platform.NewManager(logger),
contractEngine: contracts.NewEngine(),
lagAnalyzer:    lagdiag.NewAnalyzer(),
migrationPlanner: migration.NewPlanner(),
lineageTracker: lineage.NewTracker(),
selfHealMon:    selfheal.NewMonitor(),
personaEngine:  persona.NewEngine(),
forecaster:     forecast.NewForecaster(30),
}

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

// Close shuts down the server.
func (s *Server) Close() error {
s.kafkaManager.Close()
if s.watchManager != nil {
	s.watchManager.Close()
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
entry.Result.Error = err.Error()
}
s.auditLogger.Log(entry)
safeAgent := metrics.SanitizeAgentLabel(agentID)
s.metrics.ToolCallsTotal.WithLabelValues(tool, safeAgent, entry.Cluster, status).Inc()
s.metrics.ToolLatencySeconds.WithLabelValues(tool, safeAgent, entry.Cluster).Observe(latency.Seconds())
}

// sanitizeAuditParams redacts sensitive data from audit log parameters.
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
keys, ok := sensitiveTools[tool]
if !ok {
	return params
}
sanitized := make(map[string]any, len(params))
for k, v := range params {
	sanitized[k] = v
}
for _, key := range keys {
	if _, exists := sanitized[key]; exists {
		sanitized[key] = "**REDACTED**"
	}
}
return sanitized
}

// marshalResult serializes a value as indented JSON and wraps it in an MCP tool result.
func marshalResult(v any) (*mcp.CallToolResult, error) {
data, err := json.MarshalIndent(v, "", "  ")
if err != nil {
return mcp.NewToolResultError(fmt.Sprintf("Failed to marshal result: %v", err)), nil
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
