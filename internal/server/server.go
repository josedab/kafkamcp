package server

import (
"encoding/json"
"fmt"
"log/slog"
"time"

"github.com/kafkamcp/kafkamcp/internal/audit"
"github.com/kafkamcp/kafkamcp/internal/auth"
"github.com/kafkamcp/kafkamcp/internal/config"
"github.com/kafkamcp/kafkamcp/internal/kafka"
"github.com/kafkamcp/kafkamcp/internal/match"
"github.com/kafkamcp/kafkamcp/internal/metrics"
"github.com/kafkamcp/kafkamcp/internal/ratelimit"
"github.com/kafkamcp/kafkamcp/internal/schema"
"github.com/mark3labs/mcp-go/mcp"
"github.com/mark3labs/mcp-go/server"
)

// Server is the KafkaMCP MCP server.
type Server struct {
config       *config.Config
kafkaManager *kafka.Manager
schemaClients map[string]*schema.Client // per-cluster schema clients
policyEngine *auth.PolicyEngine
auditLogger  *audit.Logger
rateLimiter  *ratelimit.Limiter
metrics      *metrics.Metrics
logger       *slog.Logger
mcpServer    *server.MCPServer
}

// New creates a new KafkaMCP server.
func New(cfg *config.Config, kafkaManager *kafka.Manager, logger *slog.Logger, version string) (*Server, error) {
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
config:        cfg,
kafkaManager:  kafkaManager,
schemaClients: schemaClients,
policyEngine:  policyEngine,
auditLogger:   auditLogger,
rateLimiter:   rl,
metrics:       metrics.New(),
logger:        logger,
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

// Close shuts down the server.
func (s *Server) Close() error {
s.kafkaManager.Close()
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
if agentID == "anonymous" {
	return mcp.NewToolResultError("Authentication required: X-Agent-ID header must be provided")
}
if !s.policyEngine.AgentExists(agentID) {
	return mcp.NewToolResultError(fmt.Sprintf("Unknown agent %q: not configured in policy", agentID))
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
}

// --- Auth & Rate Limit helpers ---

func (s *Server) checkAuth(agentID, tool string, resource auth.ResourceType, resourceName string, action auth.Action) *mcp.CallToolResult {
if deny := s.validateAgentID(agentID); deny != nil {
	return deny
}
if !s.policyEngine.Authorize(agentID, resource, resourceName, action) {
s.metrics.AuthzDenials.WithLabelValues(agentID, tool, string(action)).Inc()
s.logger.Warn("authorization denied", "agent", agentID, "tool", tool, "resource", resourceName, "action", action)
return mcp.NewToolResultError(fmt.Sprintf("Unauthorized: agent %q lacks %s permission on %s", agentID, action, resourceName))
}
return nil
}

func (s *Server) checkRateLimit(agentID, tool string) *mcp.CallToolResult {
if !s.rateLimiter.Allow(agentID) {
s.metrics.RateLimitDenials.WithLabelValues(agentID, tool).Inc()
return mcp.NewToolResultError("Rate limit exceeded. Please retry later.")
}
return nil
}

func (s *Server) resolveClusterName(cluster string) string {
if cluster != "" {
	// Only return the cluster name if it's a valid configured cluster
	if _, err := s.kafkaManager.GetClusterConfig(cluster); err == nil {
		return cluster
	}
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
s.metrics.ToolCallsTotal.WithLabelValues(tool, agentID, entry.Cluster, status).Inc()
s.metrics.ToolLatencySeconds.WithLabelValues(tool, agentID, entry.Cluster).Observe(latency.Seconds())
}

// sanitizeAuditParams redacts sensitive data from audit log parameters.
func sanitizeAuditParams(tool string, params map[string]any) map[string]any {
if params == nil {
	return nil
}
sensitiveTools := map[string][]string{
	"kafka_produce": {"messages"},
	"kafka_consume": {"messages"},
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

// Tool and resource handler implementations are in:
// - tools_p0.go  (P0 handlers: list topics, describe, consume, produce, schemas, consumer groups)
// - tools_p1.go  (P1 handlers: create/alter/delete topic, search, reset offsets, DLQ, subscribe)
// - tools_p2.go  (P2 handlers: aggregate, schema evolution, connectors, join, NL query)
// - resources.go (Resource handlers: cluster, topics, schemas, groups, audit, graph)
