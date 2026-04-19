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

func (s *Server) registerTools() {
// P0 Tools
s.mcpServer.AddTool(mcp.NewTool("kafka_list_topics",
mcp.WithDescription("List all Kafka topics in the cluster with metadata including partition count, replication factor, and estimated message count."),
mcp.WithString("filter", mcp.Description("Glob pattern to filter topic names (e.g., 'orders.*', '*-dlq')")),
mcp.WithBoolean("include_internal", mcp.Description("Include internal topics (e.g., __consumer_offsets). Default: false")),
mcp.WithString("cluster", mcp.Description("Cluster name for multi-cluster setups.")),
), s.handleListTopics)

s.mcpServer.AddTool(mcp.NewTool("kafka_describe_topic",
mcp.WithDescription("Get detailed metadata for a specific topic including partition details, replicas, ISR, offsets, and configuration."),
mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
mcp.WithString("cluster", mcp.Description("Cluster name for multi-cluster setups.")),
), s.handleDescribeTopic)

s.mcpServer.AddTool(mcp.NewTool("kafka_consume",
mcp.WithDescription("Read messages from a Kafka topic with flexible positioning. Supports earliest, latest, numeric offset, or ISO 8601 timestamp."),
mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
mcp.WithNumber("partition", mcp.Description("Specific partition (-1 for all). Default: all")),
mcp.WithString("offset", mcp.Description("Start position: 'earliest', 'latest', numeric offset, or ISO 8601 timestamp. Default: 'latest'")),
mcp.WithNumber("limit", mcp.Description("Maximum messages to return (1-1000). Default: 10")),
mcp.WithString("deserializer", mcp.Description("Deserialization format: auto, avro, protobuf, json, string, bytes. Default: auto")),
mcp.WithNumber("timeout_ms", mcp.Description("Maximum wait time in ms. Default: 5000")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleConsume)

s.mcpServer.AddTool(mcp.NewTool("kafka_produce",
mcp.WithDescription("Publish one or more messages to a Kafka topic. Returns delivery confirmation with partition, offset, and timestamp."),
mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
mcp.WithObject("messages", mcp.Required(), mcp.Description("Array of messages: [{key, value, headers, partition}]. Max 100.")),
mcp.WithString("serializer", mcp.Description("Serialization format: auto, avro, protobuf, json, string. Default: auto")),
mcp.WithNumber("schema_id", mcp.Description("Explicit Schema Registry schema ID for serialization")),
mcp.WithString("acks", mcp.Description("Acknowledgment level: 0, 1, all. Default: all")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleProduce)

if s.hasSchemaRegistry() {
s.mcpServer.AddTool(mcp.NewTool("kafka_list_schemas",
mcp.WithDescription("List all subjects in the Schema Registry with latest version, type, and compatibility level."),
mcp.WithString("filter", mcp.Description("Glob pattern to filter subject names")),
), s.handleListSchemas)

s.mcpServer.AddTool(mcp.NewTool("kafka_get_schema",
mcp.WithDescription("Get the full schema definition for a subject. Returns parsed schema, field summary, compatibility, and references."),
mcp.WithString("subject", mcp.Required(), mcp.Description("Schema subject name (e.g., 'orders.created-value')")),
mcp.WithString("version", mcp.Description("Version: 'latest' or number. Default: 'latest'")),
), s.handleGetSchema)

// P1: Schema compatibility check
s.mcpServer.AddTool(mcp.NewTool("kafka_check_schema_compatibility",
mcp.WithDescription("Check if a proposed schema is compatible with the subject's compatibility policy."),
mcp.WithString("subject", mcp.Required(), mcp.Description("Schema subject name")),
mcp.WithString("schema", mcp.Required(), mcp.Description("The proposed schema definition as a JSON string")),
mcp.WithString("schema_type", mcp.Description("Schema type: AVRO, PROTOBUF, JSON. Default: AVRO")),
mcp.WithString("version", mcp.Description("Version to check against. Default: 'latest'")),
), s.handleCheckSchemaCompatibility)
}

s.mcpServer.AddTool(mcp.NewTool("kafka_describe_consumer_group",
mcp.WithDescription("Get the state, members, partition assignments, and per-partition lag of a consumer group."),
mcp.WithString("group_id", mcp.Required(), mcp.Description("Consumer group ID")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleDescribeConsumerGroup)

s.mcpServer.AddTool(mcp.NewTool("kafka_list_consumer_groups",
mcp.WithDescription("List all consumer groups with summary state, member count, and total lag."),
mcp.WithString("state_filter", mcp.Description("Filter by state (Stable, Empty, Dead)")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleListConsumerGroups)

// P1 Tools
s.mcpServer.AddTool(mcp.NewTool("kafka_create_topic",
mcp.WithDescription("Create a new Kafka topic with specified partitions, replication factor, and configuration."),
mcp.WithString("name", mcp.Required(), mcp.Description("Topic name")),
mcp.WithNumber("partitions", mcp.Description("Number of partitions. Default: 1")),
mcp.WithNumber("replication_factor", mcp.Description("Replication factor. Default: 1")),
mcp.WithObject("configs", mcp.Description("Topic configs (e.g., {\"retention.ms\": \"604800000\"})")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleCreateTopic)

s.mcpServer.AddTool(mcp.NewTool("kafka_alter_topic",
mcp.WithDescription("Modify configuration of an existing Kafka topic."),
mcp.WithString("name", mcp.Required(), mcp.Description("Topic name")),
mcp.WithObject("configs", mcp.Required(), mcp.Description("Configs to change (e.g., {\"retention.ms\": \"86400000\"})")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleAlterTopic)

s.mcpServer.AddTool(mcp.NewTool("kafka_delete_topic",
mcp.WithDescription("Delete a Kafka topic. This is a destructive operation."),
mcp.WithString("name", mcp.Required(), mcp.Description("Topic name to delete")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleDeleteTopic)

s.mcpServer.AddTool(mcp.NewTool("kafka_search",
mcp.WithDescription("Search/filter messages in a topic by key, header, value content, or time range. Bounded scan."),
mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
mcp.WithString("key_filter", mcp.Description("Filter messages where key contains this string")),
mcp.WithString("header_key", mcp.Description("Filter messages that have this header key")),
mcp.WithString("header_value", mcp.Description("Filter messages where the header matches this value")),
mcp.WithString("value_contains", mcp.Description("Filter messages where value contains this string")),
mcp.WithString("start_time", mcp.Description("Start time (ISO 8601) for time range filter")),
mcp.WithString("end_time", mcp.Description("End time (ISO 8601) for time range filter")),
mcp.WithNumber("max_scan", mcp.Description("Max messages to scan. Default: 10000")),
mcp.WithNumber("limit", mcp.Description("Max matching messages to return. Default: 10")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleSearch)

s.mcpServer.AddTool(mcp.NewTool("kafka_reset_offsets",
mcp.WithDescription("Reset consumer group offsets. Supports earliest, latest, or timestamp strategies with dry-run mode."),
mcp.WithString("group_id", mcp.Required(), mcp.Description("Consumer group ID")),
mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
mcp.WithString("strategy", mcp.Required(), mcp.Description("Reset strategy: earliest, latest, timestamp")),
mcp.WithString("value", mcp.Description("Timestamp value for 'timestamp' strategy (ISO 8601)")),
mcp.WithBoolean("dry_run", mcp.Description("Preview without applying. Default: false")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleResetOffsets)

s.mcpServer.AddTool(mcp.NewTool("kafka_analyze_dlq",
mcp.WithDescription("Analyze a dead-letter queue topic: consume recent messages, parse error headers, categorize failure types, and return a statistical summary with samples."),
mcp.WithString("topic", mcp.Required(), mcp.Description("DLQ topic name")),
mcp.WithNumber("limit", mcp.Description("Max messages to analyze. Default: 100")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleAnalyzeDLQ)

// FR-11: Streaming Subscription
s.mcpServer.AddTool(mcp.NewTool("kafka_subscribe",
mcp.WithDescription("Subscribe to a Kafka topic using consumer group semantics. Polls for new messages for the specified duration and returns them as a batch. Supports auto-commit for offset management."),
mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name to subscribe to")),
mcp.WithString("group_id", mcp.Description("Consumer group ID. If omitted, an ephemeral group is created.")),
mcp.WithBoolean("auto_commit", mcp.Description("Automatically commit offsets after consuming. Default: true")),
mcp.WithNumber("max_messages", mcp.Description("Maximum messages to return per poll (1-1000). Default: 100")),
mcp.WithNumber("timeout_ms", mcp.Description("How long to poll for messages in ms (max 30000). Default: 10000")),
mcp.WithString("deserializer", mcp.Description("Deserialization format: auto, json, string, bytes. Default: auto")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleSubscribe)

// P2 Tools
s.mcpServer.AddTool(mcp.NewTool("kafka_aggregate",
mcp.WithDescription("Compute count, sum, avg, min, or max over a time window on a numeric field in topic messages. Lightweight stream processing."),
mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
mcp.WithString("field", mcp.Description("Dot-notation path to the numeric field (e.g., 'amount', 'metrics.latency_ms'). Empty for the entire value.")),
mcp.WithString("function", mcp.Required(), mcp.Description("Aggregation function: count, sum, avg, min, max")),
mcp.WithNumber("window_ms", mcp.Description("Time window in milliseconds. Default: 300000 (5 min)")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleAggregate)

s.mcpServer.AddTool(mcp.NewTool("kafka_suggest_schema_evolution",
mcp.WithDescription("Given a current schema subject and a desired change description, suggest compatible evolution strategies with step-by-step instructions."),
mcp.WithString("subject", mcp.Required(), mcp.Description("Schema subject name")),
mcp.WithString("change", mcp.Required(), mcp.Description("Description of desired change (e.g., 'add a new field for user email', 'rename field from name to full_name', 'remove deprecated field')")),
), s.handleSuggestSchemaEvolution)

s.mcpServer.AddTool(mcp.NewTool("kafka_list_connectors",
mcp.WithDescription("List all Kafka Connect connectors with their status, type, and worker assignment."),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleListConnectors)

s.mcpServer.AddTool(mcp.NewTool("kafka_describe_connector",
mcp.WithDescription("Get full details for a Kafka Connect connector including config, task status, and errors."),
mcp.WithString("name", mcp.Required(), mcp.Description("Connector name")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleDescribeConnector)

s.mcpServer.AddTool(mcp.NewTool("kafka_join",
mcp.WithDescription("Perform a time-windowed join across two topics on a key field. Returns matched messages from both topics."),
mcp.WithString("left_topic", mcp.Required(), mcp.Description("Left topic name")),
mcp.WithString("right_topic", mcp.Required(), mcp.Description("Right topic name")),
mcp.WithString("key_field", mcp.Description("Dot-notation field to join on. Empty uses the message key.")),
mcp.WithNumber("window_ms", mcp.Description("Time window in ms. Default: 60000 (1 min)")),
mcp.WithNumber("limit", mcp.Description("Max joined results. Default: 100")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleJoin)

s.mcpServer.AddTool(mcp.NewTool("kafka_query",
mcp.WithDescription("Accept a natural language description of desired data and translate it to a topic lookup, filter, and consume call. Example: 'last 10 failed payments'"),
mcp.WithString("query", mcp.Required(), mcp.Description("Natural language description of the data you want")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleNaturalLanguageQuery)

// P3 Tools
s.mcpServer.AddTool(mcp.NewTool("kafka_watch",
mcp.WithDescription("Register or manage event-driven watch subscriptions on topic patterns. When matching messages arrive, notifications are pushed via webhooks or MCP sampling."),
mcp.WithString("action", mcp.Description("Action: subscribe, unsubscribe, list. Default: subscribe")),
mcp.WithString("topic_pattern", mcp.Description("Glob pattern to match topics (e.g., 'orders.*', '*-dlq'). Required for subscribe.")),
mcp.WithString("key_filter", mcp.Description("Filter messages where key contains this string")),
mcp.WithString("value_filter", mcp.Description("Filter messages where value contains this string")),
mcp.WithString("webhook_url", mcp.Description("URL to POST notifications to")),
mcp.WithString("subscription_id", mcp.Description("Subscription ID (required for unsubscribe)")),
), s.handleWatch)

s.mcpServer.AddTool(mcp.NewTool("kafka_remediate_dlq",
mcp.WithDescription("Auto-remediate DLQ messages: retry with backoff, route to correction topic, or generate fix suggestions using schema context."),
mcp.WithString("topic", mcp.Required(), mcp.Description("DLQ topic name")),
mcp.WithString("action", mcp.Description("Remediation action: analyze, retry, route, suggest. Default: analyze")),
mcp.WithString("target_topic", mcp.Description("Target topic for routing remediation")),
mcp.WithNumber("limit", mcp.Description("Max messages to process. Default: 100")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleRemediateDLQ)

if s.hasSchemaRegistry() {
s.mcpServer.AddTool(mcp.NewTool("kafka_schema_diff",
mcp.WithDescription("Compute a field-by-field diff between two schema versions showing additions, removals, and changes."),
mcp.WithString("subject", mcp.Required(), mcp.Description("Schema subject name")),
mcp.WithString("version_from", mcp.Required(), mcp.Description("Source version number")),
mcp.WithString("version_to", mcp.Description("Target version number. Default: 'latest'")),
), s.handleSchemaDiff)
}

// Streaming consume
s.mcpServer.AddTool(mcp.NewTool("kafka_stream",
mcp.WithDescription("Manage streaming consumer sessions for real-time message delivery. Actions: start (create session), poll (get messages), pause/resume/stop (control), list (show sessions)."),
mcp.WithString("action", mcp.Description("Action: start, poll, pause, resume, stop, list. Default: start")),
mcp.WithString("topic", mcp.Description("Topic name (required for start)")),
mcp.WithString("group_id", mcp.Description("Consumer group ID. Auto-generated if omitted.")),
mcp.WithString("session_id", mcp.Description("Session ID (required for poll/pause/resume/stop)")),
mcp.WithNumber("batch_size", mcp.Description("Messages per batch (1-100). Default: 10")),
mcp.WithNumber("max_msg_per_sec", mcp.Description("Throttle: max messages/sec. Default: 1000")),
mcp.WithNumber("max_messages", mcp.Description("Max total messages before auto-stop. Default: 1000")),
mcp.WithNumber("timeout_ms", mcp.Description("Session timeout in ms (max 60000). Default: 30000")),
mcp.WithNumber("poll_timeout_ms", mcp.Description("Poll wait time in ms. Default: 5000")),
mcp.WithString("deserializer", mcp.Description("Deserialization: auto, json, string, bytes. Default: auto")),
mcp.WithBoolean("auto_commit", mcp.Description("Auto-commit offsets. Default: true")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleStream)

// Multi-agent collaboration
s.mcpServer.AddTool(mcp.NewTool("kafka_agent_registry",
mcp.WithDescription("Multi-agent collaboration: register agents, claim partitions, share cursors, and handoff messages. Actions: register, heartbeat, claim, release, share_cursor, handoff, list_agents."),
mcp.WithString("action", mcp.Required(), mcp.Description("Action: register, heartbeat, claim, release, share_cursor, handoff, list_agents")),
mcp.WithString("agent_name", mcp.Description("Agent name for registration")),
mcp.WithString("topic", mcp.Description("Topic for partition operations")),
mcp.WithNumber("partition", mcp.Description("Partition number to claim/release")),
mcp.WithNumber("offset", mcp.Description("Offset for cursor sharing")),
mcp.WithString("target_agent", mcp.Description("Target agent for handoff")),
mcp.WithString("message_data", mcp.Description("Message data for handoff (JSON string)")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleAgentRegistry)

// Kafka Connect management
s.mcpServer.AddTool(mcp.NewTool("kafka_create_connector",
mcp.WithDescription("Create a new Kafka Connect connector with the specified configuration."),
mcp.WithString("name", mcp.Required(), mcp.Description("Connector name")),
mcp.WithObject("config", mcp.Required(), mcp.Description("Connector configuration map")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleCreateConnector)

s.mcpServer.AddTool(mcp.NewTool("kafka_update_connector",
mcp.WithDescription("Update an existing Kafka Connect connector configuration."),
mcp.WithString("name", mcp.Required(), mcp.Description("Connector name")),
mcp.WithObject("config", mcp.Required(), mcp.Description("New connector configuration map")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleUpdateConnector)

s.mcpServer.AddTool(mcp.NewTool("kafka_delete_connector",
mcp.WithDescription("Delete a Kafka Connect connector."),
mcp.WithString("name", mcp.Required(), mcp.Description("Connector name")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleDeleteConnector)

s.mcpServer.AddTool(mcp.NewTool("kafka_restart_connector",
mcp.WithDescription("Restart a Kafka Connect connector or specific tasks. Supports bulk restart of failed tasks."),
mcp.WithString("name", mcp.Required(), mcp.Description("Connector name")),
mcp.WithNumber("task_id", mcp.Description("Specific task ID to restart (-1 for all). Default: -1")),
mcp.WithBoolean("failed_only", mcp.Description("Only restart failed tasks. Default: false")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleRestartConnector)

s.mcpServer.AddTool(mcp.NewTool("kafka_connector_health",
mcp.WithDescription("Get health score and diagnostics for a Kafka Connect connector including task states, error rates, and recommendations."),
mcp.WithString("name", mcp.Required(), mcp.Description("Connector name")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleConnectorHealth)

// Time-travel query engine
s.mcpServer.AddTool(mcp.NewTool("kafka_time_travel",
mcp.WithDescription("Point-in-time topic queries: snapshot state at a timestamp, reconstruct compacted topic state, or diff state between two timestamps."),
mcp.WithString("action", mcp.Required(), mcp.Description("Action: snapshot, diff")),
mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
mcp.WithString("timestamp", mcp.Description("Point-in-time timestamp (ISO 8601). Required for snapshot.")),
mcp.WithString("timestamp_from", mcp.Description("Start timestamp for diff (ISO 8601)")),
mcp.WithString("timestamp_to", mcp.Description("End timestamp for diff (ISO 8601)")),
mcp.WithNumber("max_keys", mcp.Description("Max unique keys to track. Default: 10000")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleTimeTravel)

// Intelligent topic recommendations
s.mcpServer.AddTool(mcp.NewTool("kafka_recommend",
mcp.WithDescription("Get intelligent recommendations for topic optimization: detect under-replicated partitions, idle topics, consumer lag trends, partition hotspots, retention tuning, and compression analysis."),
mcp.WithString("topic", mcp.Description("Specific topic to analyze (optional, analyzes all if empty)")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleRecommend)

// End-to-end message tracing
s.mcpServer.AddTool(mcp.NewTool("kafka_trace_message",
mcp.WithDescription("Trace a message across topics using correlation IDs. Auto-detects correlation-id, trace-id, x-request-id headers and builds a message flow graph."),
mcp.WithString("correlation_id", mcp.Required(), mcp.Description("Correlation ID to trace")),
mcp.WithString("header_name", mcp.Description("Header name containing correlation ID. Default: auto-detect")),
mcp.WithObject("topics", mcp.Description("List of topic names to search. Default: all topics.")),
mcp.WithNumber("max_scan", mcp.Description("Max messages to scan per topic. Default: 10000")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleTraceMessage)

// ACL & Security audit
s.mcpServer.AddTool(mcp.NewTool("kafka_list_acls",
mcp.WithDescription("List all Kafka ACLs with principal, resource, and operation breakdown. Includes security analysis for over-permissioned principals and wildcard exposure."),
mcp.WithString("principal", mcp.Description("Filter by principal (e.g., 'User:alice')")),
mcp.WithString("resource_type", mcp.Description("Filter by resource type: topic, group, cluster, transactional_id")),
mcp.WithBoolean("include_analysis", mcp.Description("Include security analysis. Default: true")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleListACLs)

// Configuration drift detection
s.mcpServer.AddTool(mcp.NewTool("kafka_config_drift",
mcp.WithDescription("Detect configuration drift across clusters. Compare topic configs between clusters, check against golden templates, and generate remediation commands."),
mcp.WithString("action", mcp.Description("Action: compare, audit, template. Default: compare")),
mcp.WithString("topic", mcp.Description("Specific topic to compare")),
mcp.WithString("source_cluster", mcp.Description("Source cluster for comparison")),
mcp.WithString("target_cluster", mcp.Description("Target cluster for comparison")),
mcp.WithObject("template", mcp.Description("Golden template configs for audit")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleConfigDrift)

// Interactive Kafka playground
s.mcpServer.AddTool(mcp.NewTool("kafka_playground",
mcp.WithDescription("Sandboxed Kafka environment for safe experimentation. Create temporary topics, generate sample data, and auto-cleanup. Actions: create, produce_sample, list, cleanup."),
mcp.WithString("action", mcp.Required(), mcp.Description("Action: create, produce_sample, list, cleanup")),
mcp.WithString("name", mcp.Description("Playground topic name suffix (auto-prefixed with _playground_)")),
mcp.WithNumber("partitions", mcp.Description("Partitions for playground topic. Default: 3")),
mcp.WithString("data_pattern", mcp.Description("Sample data pattern: ecommerce, iot, user_activity, logs. Default: ecommerce")),
mcp.WithNumber("count", mcp.Description("Number of sample messages to produce. Default: 10")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handlePlayground)

// ===================== NEW FEATURE TOOLS =====================

// Anomaly Detection
s.mcpServer.AddTool(mcp.NewTool("kafka_anomalies",
mcp.WithDescription("Get AI-detected anomalies in consumer lag, throughput, and error rates. Uses z-score and IQR statistical models with configurable sensitivity."),
mcp.WithString("severity", mcp.Description("Filter by severity: warning, critical")),
mcp.WithString("metric", mcp.Description("Filter by metric type: consumer_lag, throughput, error_rate")),
mcp.WithNumber("limit", mcp.Description("Max alerts to return. Default: 50")),
mcp.WithNumber("since_hours", mcp.Description("Look back period in hours. Default: 24")),
), s.handleAnomalies)

s.mcpServer.AddTool(mcp.NewTool("kafka_anomaly_ingest",
mcp.WithDescription("Manually ingest a data point for anomaly detection analysis."),
mcp.WithString("metric", mcp.Required(), mcp.Description("Metric type: consumer_lag, throughput, error_rate")),
mcp.WithString("source", mcp.Required(), mcp.Description("Source name (topic or consumer group)")),
mcp.WithNumber("value", mcp.Required(), mcp.Description("Metric value")),
), s.handleIngestAnomaly)

// Natural Language Query
s.mcpServer.AddTool(mcp.NewTool("kafka_nl_query",
mcp.WithDescription("Query Kafka with plain English. Parses natural language into consume + filter + aggregate pipelines. Examples: 'show me all failed orders in the last hour', 'count errors in logs', 'average latency for payments'."),
mcp.WithString("query", mcp.Required(), mcp.Description("Natural language query")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleNLQuery)

// Cost Analysis
s.mcpServer.AddTool(mcp.NewTool("kafka_cost_analyze",
mcp.WithDescription("Analyze storage and compute costs for a Kafka topic. Estimates monthly cost based on retention, partitions, throughput, and replication."),
mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
mcp.WithString("provider", mcp.Description("Cost model: aws-msk, confluent-cloud, self-hosted. Default: aws-msk")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleCostAnalyze)

s.mcpServer.AddTool(mcp.NewTool("kafka_cost_waste",
mcp.WithDescription("Detect cost waste across all topics: idle topics, over-replication, compression opportunities, excessive retention."),
mcp.WithString("provider", mcp.Description("Cost model: aws-msk, confluent-cloud, self-hosted. Default: aws-msk")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleCostWaste)

s.mcpServer.AddTool(mcp.NewTool("kafka_cost_whatif",
mcp.WithDescription("Simulate configuration changes and see cost impact. What-if analysis for retention, compression, partitions, and replication changes."),
mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
mcp.WithNumber("new_retention_ms", mcp.Description("New retention period in ms")),
mcp.WithString("new_compression", mcp.Description("New compression type: gzip, snappy, lz4, zstd, none")),
mcp.WithNumber("new_partitions", mcp.Description("New partition count")),
mcp.WithNumber("new_replication_factor", mcp.Description("New replication factor")),
mcp.WithString("provider", mcp.Description("Cost model. Default: aws-msk")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleCostWhatIf)

// Workflow Templates
s.mcpServer.AddTool(mcp.NewTool("kafka_workflows",
mcp.WithDescription("Manage pre-built workflow templates for common Kafka operations. Actions: list (browse templates), describe (get template details), execute (start a workflow)."),
mcp.WithString("action", mcp.Required(), mcp.Description("Action: list, describe, execute")),
mcp.WithString("template", mcp.Description("Template name (required for describe/execute)")),
mcp.WithString("category", mcp.Description("Filter by category: operations, planning, development, monitoring, discovery, quality, debugging")),
mcp.WithObject("params", mcp.Description("Parameters for template execution")),
mcp.WithString("topic", mcp.Description("Topic parameter for workflow")),
mcp.WithString("group_id", mcp.Description("Consumer group parameter for workflow")),
mcp.WithString("subject", mcp.Description("Schema subject parameter for workflow")),
), s.handleWorkflows)

// GitOps
s.mcpServer.AddTool(mcp.NewTool("kafka_gitops_diff",
mcp.WithDescription("Compare desired Kafka configuration (declarative YAML) against live cluster state. Generates a diff and migration plan with dry-run support."),
mcp.WithObject("topics", mcp.Description("Array of desired topic specs: [{name, partitions, replication_factor, configs}]")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleGitOpsDiff)

// Knowledge Graph
s.mcpServer.AddTool(mcp.NewTool("kafka_knowledge_graph",
mcp.WithDescription("Query the embedded knowledge graph of topic relationships, consumer group topologies, and schema dependencies. Supports: 'depends_on <node>', 'impacts <node>', 'path <from> to <to>', 'neighbors <node>', 'type <node_type>', or free-text search."),
mcp.WithString("query", mcp.Required(), mcp.Description("Graph query (e.g., 'depends_on orders', 'impacts user-events', 'type consumer_group')")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleKnowledgeGraphQuery)

// Platform Info
s.mcpServer.AddTool(mcp.NewTool("kafka_platform_info",
mcp.WithDescription("Get information about configured Kafka-compatible platforms (Apache Kafka, Redpanda, Confluent Cloud, MSK Serverless) and their capabilities."),
), s.handlePlatformInfo)

// Data Contracts
s.mcpServer.AddTool(mcp.NewTool("kafka_validate_contract",
mcp.WithDescription("Validate messages against data contracts, register contracts, or get compliance reports. Actions: validate, register, get, list, compliance."),
mcp.WithString("action", mcp.Required(), mcp.Description("Action: validate, register, get, list, compliance")),
mcp.WithString("topic", mcp.Description("Topic name")),
mcp.WithObject("contract", mcp.Description("Contract definition for register action")),
mcp.WithObject("message", mcp.Description("Message value to validate")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleDataContract)

// Consumer Lag Diagnosis
s.mcpServer.AddTool(mcp.NewTool("kafka_diagnose_lag",
mcp.WithDescription("Deep diagnostic analysis of consumer group lag. Traces root cause: slow consumers, producer spikes, partition imbalance, or GC pressure."),
mcp.WithString("group_id", mcp.Required(), mcp.Description("Consumer group ID")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleDiagnoseLag)

// Migration Planning
s.mcpServer.AddTool(mcp.NewTool("kafka_migration_plan",
mcp.WithDescription("Plan and validate Kafka migrations between clusters. Actions: plan (diff clusters), validate (check plan), list (show plans)."),
mcp.WithString("action", mcp.Required(), mcp.Description("Action: plan, validate, list")),
mcp.WithString("source_cluster", mcp.Description("Source cluster name")),
mcp.WithString("target_cluster", mcp.Description("Target cluster name")),
), s.handleMigrationPlan)

// Data Lineage
s.mcpServer.AddTool(mcp.NewTool("kafka_trace_lineage",
mcp.WithDescription("Track data lineage through Kafka topics. Shows producer→topic→consumer relationships, PII annotations, and GDPR/CCPA compliance reports."),
mcp.WithString("action", mcp.Required(), mcp.Description("Action: trace, annotate_pii, compliance_report, detect_pii")),
mcp.WithString("topic", mcp.Description("Topic name")),
mcp.WithString("framework", mcp.Description("Compliance framework: GDPR, CCPA")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleTraceLineage)

// Consumer Group Health
s.mcpServer.AddTool(mcp.NewTool("kafka_group_health",
mcp.WithDescription("Evaluate consumer group health with scoring (0-100), detect zombies, rebalance storms, stuck partitions, and suggest auto-remediation."),
mcp.WithString("group_id", mcp.Required(), mcp.Description("Consumer group ID")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleGroupHealth)

// Agent Personas
s.mcpServer.AddTool(mcp.NewTool("kafka_persona",
mcp.WithDescription("Manage declarative agent personas. Actions: list (all personas), get (persona details), activate (set active persona)."),
mcp.WithString("action", mcp.Required(), mcp.Description("Action: list, get, activate")),
mcp.WithString("persona", mcp.Description("Persona name: sre, data-engineer, compliance, developer, admin")),
), s.handlePersona)

// Capacity Forecasting
s.mcpServer.AddTool(mcp.NewTool("kafka_forecast",
mcp.WithDescription("ML-based capacity forecasting using linear regression and seasonal decomposition. Predicts throughput growth and storage needs."),
mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
mcp.WithString("metric", mcp.Description("Metric to forecast: throughput, storage, lag. Default: throughput")),
mcp.WithNumber("horizon_days", mcp.Description("Forecast horizon in days. Default: 7")),
mcp.WithString("cluster", mcp.Description("Cluster name.")),
), s.handleForecast)
}

func (s *Server) registerResources() {
s.mcpServer.AddResource(mcp.NewResource("kafka://cluster/metadata", "Cluster Metadata",
mcp.WithResourceDescription("Kafka cluster information: brokers, controller, topic/partition counts"),
mcp.WithMIMEType("application/json"),
), s.handleClusterMetadata)

s.mcpServer.AddResource(mcp.NewResource("kafka://clusters", "Cluster List",
mcp.WithResourceDescription("All configured Kafka clusters for multi-cluster discovery"),
mcp.WithMIMEType("application/json"),
), s.handleClusterList)

s.mcpServer.AddResource(mcp.NewResource("kafka://topics", "Topic Catalog",
mcp.WithResourceDescription("List of all topics with partition counts and retention"),
mcp.WithMIMEType("application/json"),
), s.handleTopicCatalog)

s.mcpServer.AddResource(mcp.NewResource("kafka://schemas", "Schema Catalog",
mcp.WithResourceDescription("All Schema Registry subjects with latest versions"),
mcp.WithMIMEType("application/json"),
), s.handleSchemaCatalog)

s.mcpServer.AddResource(mcp.NewResource("kafka://groups", "Consumer Group Catalog",
mcp.WithResourceDescription("All consumer groups with state and total lag"),
mcp.WithMIMEType("application/json"),
), s.handleGroupCatalog)

s.mcpServer.AddResource(mcp.NewResource("kafka://audit/log", "Agent Audit Trail",
mcp.WithResourceDescription("Recent agent interactions with KafkaMCP (last 1000 entries)"),
mcp.WithMIMEType("application/json"),
), s.handleAuditLogResource)

s.mcpServer.AddResource(mcp.NewResource("kafka://topic_graph", "Topic Topology Graph",
mcp.WithResourceDescription("Producer -> topic -> consumer group relationship graph"),
mcp.WithMIMEType("application/json"),
), s.handleTopicGraphResource)

// Resource templates for individual items
s.mcpServer.AddResourceTemplate(mcp.NewResourceTemplate("kafka://topics/{name}", "Topic Detail",
mcp.WithTemplateDescription("Full config, partition map, associated schemas for a topic"),
mcp.WithTemplateMIMEType("application/json"),
), s.handleTopicResource)

s.mcpServer.AddResourceTemplate(mcp.NewResourceTemplate("kafka://schemas/{subject}", "Schema Detail",
mcp.WithTemplateDescription("Full schema definition with field summary"),
mcp.WithTemplateMIMEType("application/json"),
), s.handleSchemaResource)

s.mcpServer.AddResourceTemplate(mcp.NewResourceTemplate("kafka://groups/{id}", "Consumer Group Detail",
mcp.WithTemplateDescription("Members, offsets, lag per partition for a consumer group"),
mcp.WithTemplateMIMEType("application/json"),
), s.handleGroupResource)

s.mcpServer.AddResource(mcp.NewResource("kafka://replication/status", "Replication Status",
mcp.WithResourceDescription("Cross-cluster replication status with lag per topic-partition"),
mcp.WithMIMEType("application/json"),
), s.handleReplicationStatus)

s.mcpServer.AddResource(mcp.NewResource("kafka://plugins", "Plugin Registry",
mcp.WithResourceDescription("Registered plugins and extensions"),
mcp.WithMIMEType("application/json"),
), s.handlePluginRegistry)

s.mcpServer.AddResource(mcp.NewResource("kafka://watch/subscriptions", "Watch Subscriptions",
mcp.WithResourceDescription("Active watch subscriptions for event-driven workflows"),
mcp.WithMIMEType("application/json"),
), s.handleWatchSubscriptionsResource)

// New Feature Resources
s.mcpServer.AddResource(mcp.NewResource("kafka://anomalies", "Anomaly Alerts",
mcp.WithResourceDescription("AI-detected anomalies in consumer lag, throughput, and error rates (last 24h)"),
mcp.WithMIMEType("application/json"),
), s.handleAnomalyResource)

s.mcpServer.AddResource(mcp.NewResource("kafka://workflows", "Workflow Templates",
mcp.WithResourceDescription("Pre-built workflow templates for common Kafka operations"),
mcp.WithMIMEType("application/json"),
), s.handleWorkflowsResource)

s.mcpServer.AddResource(mcp.NewResource("kafka://knowledge_graph", "Knowledge Graph",
mcp.WithResourceDescription("Topic relationships, schema dependencies, and consumer group topology graph"),
mcp.WithMIMEType("application/json"),
), s.handleKnowledgeGraphResource)

s.mcpServer.AddResource(mcp.NewResource("kafka://flow_graph", "Message Flow Graph",
mcp.WithResourceDescription("Real-time message flow visualization with throughput and latency data"),
mcp.WithMIMEType("application/json"),
), s.handleFlowGraphResource)

s.mcpServer.AddResource(mcp.NewResource("kafka://platforms", "Platform Info",
mcp.WithResourceDescription("Configured Kafka-compatible platforms and their capabilities"),
mcp.WithMIMEType("application/json"),
), s.handlePlatformResource)
}

// --- Auth & Rate Limit helpers ---

// toolAuthConfig defines auth requirements for a tool handler.
type toolAuthConfig struct {
	tool         string
	resourceType auth.ResourceType
	action       auth.Action
	// resourceName extracts the resource name from the request.
	// If nil, defaults to "*".
	resourceName func(mcp.CallToolRequest) string
}

// toolHandler is a handler function that receives pre-validated context.
type toolHandler func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error)

// withToolAuth wraps a tool handler with agent ID extraction, rate limiting,
// auth checking, and audit logging. It eliminates repeated boilerplate across handlers.
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

func (s *Server) releaseExpensiveOp() {
<-s.expensiveOps
}

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

func marshalResult(v any) (*mcp.CallToolResult, error) {
data, err := json.MarshalIndent(v, "", "  ")
if err != nil {
return mcp.NewToolResultError(fmt.Sprintf("Failed to marshal result: %v", err)), nil
}
return mcp.NewToolResultText(string(data)), nil
}

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
// - tools_p0.go  (P0 handlers: list topics, describe, consume, produce, schemas, consumer groups)
// - tools_p1.go  (P1 handlers: create/alter/delete topic, search, reset offsets, DLQ, subscribe)
// - tools_p2.go  (P2 handlers: aggregate, schema evolution, connectors, join, NL query)
// - resources.go (Resource handlers: cluster, topics, schemas, groups, audit, graph)
