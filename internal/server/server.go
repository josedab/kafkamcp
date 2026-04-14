package server

import (
"context"
"encoding/json"
"fmt"
"log/slog"
"path"
"strings"
"time"

"github.com/kafkamcp/kafkamcp/internal/audit"
"github.com/kafkamcp/kafkamcp/internal/auth"
"github.com/kafkamcp/kafkamcp/internal/config"
"github.com/kafkamcp/kafkamcp/internal/kafka"
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
func New(cfg *config.Config, kafkaManager *kafka.Manager, logger *slog.Logger) (*Server, error) {
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
"0.1.0",
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
return cluster
}
return s.kafkaManager.DefaultClusterName()
}

func (s *Server) logAudit(agentID, tool string, params map[string]any, cluster, status string, count int, err error, start time.Time) {
latency := time.Since(start)
entry := audit.Entry{
AgentID:    agentID,
Tool:       tool,
Parameters: params,
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
if pattern == "*" {
return true
}
if pattern == name {
return true
}
if matched, err := path.Match(pattern, name); err == nil && matched {
return true
}
if strings.Contains(pattern, ".") {
parts := strings.Split(pattern, ".")
nameParts := strings.Split(name, ".")
if len(parts) <= len(nameParts) {
allMatch := true
for i, p := range parts {
m, err := path.Match(p, nameParts[i])
if err != nil || !m {
allMatch = false
break
}
}
return allMatch
}
}
return false
}

// ===================== P0 TOOL HANDLERS =====================

func (s *Server) handleListTopics(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
start := time.Now()
agentID := extractAgentID(request)
filter := request.GetString("filter", "")
includeInternal := request.GetBool("include_internal", false)
cluster := request.GetString("cluster", "")
args := request.GetArguments()

if deny := s.checkRateLimit(agentID, "kafka_list_topics"); deny != nil {
return deny, nil
}
if deny := s.checkAuth(agentID, "kafka_list_topics", auth.ResourceTopic, "*", auth.ActionRead); deny != nil {
return deny, nil
}

topics, err := s.kafkaManager.ListTopics(ctx, cluster, includeInternal)
if err != nil {
s.logAudit(agentID, "kafka_list_topics", args, cluster, "error", 0, err, start)
return mcp.NewToolResultError(fmt.Sprintf("Failed to list topics: %v", err)), nil
}

if filter != "" {
var filtered []kafka.TopicSummary
for _, t := range topics {
if matchTopicFilter(filter, t.Name) {
filtered = append(filtered, t)
}
}
topics = filtered
}

// Filter by auth: only return topics the agent can read
if s.config.Policies.DefaultDeny {
var allowed []kafka.TopicSummary
for _, t := range topics {
if s.policyEngine.Authorize(agentID, auth.ResourceTopic, t.Name, auth.ActionRead) {
allowed = append(allowed, t)
}
}
topics = allowed
}

result := map[string]any{"topics": topics, "total_count": len(topics), "cluster": s.resolveClusterName(cluster)}
s.logAudit(agentID, "kafka_list_topics", args, cluster, "success", len(topics), nil, start)
return marshalResult(result)
}

func (s *Server) handleDescribeTopic(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
start := time.Now()
agentID := extractAgentID(request)
topic := request.GetString("topic", "")
cluster := request.GetString("cluster", "")
args := request.GetArguments()

if topic == "" {
return mcp.NewToolResultError("topic is required"), nil
}
if deny := s.checkRateLimit(agentID, "kafka_describe_topic"); deny != nil {
return deny, nil
}
if deny := s.checkAuth(agentID, "kafka_describe_topic", auth.ResourceTopic, topic, auth.ActionRead); deny != nil {
return deny, nil
}

detail, err := s.kafkaManager.DescribeTopic(ctx, cluster, topic)
if err != nil {
s.logAudit(agentID, "kafka_describe_topic", args, cluster, "error", 0, err, start)
return mcp.NewToolResultError(fmt.Sprintf("Failed to describe topic: %v", err)), nil
}

s.logAudit(agentID, "kafka_describe_topic", args, cluster, "success", 0, nil, start)
return marshalResult(detail)
}

func (s *Server) handleConsume(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
start := time.Now()
agentID := extractAgentID(request)
topic := request.GetString("topic", "")
cluster := request.GetString("cluster", "")
offset := request.GetString("offset", "latest")
limit := request.GetInt("limit", 10)
timeoutMs := request.GetInt("timeout_ms", 5000)
args := request.GetArguments()

if topic == "" {
return mcp.NewToolResultError("topic is required"), nil
}
if deny := s.checkRateLimit(agentID, "kafka_consume"); deny != nil {
return deny, nil
}
if deny := s.checkAuth(agentID, "kafka_consume", auth.ResourceTopic, topic, auth.ActionRead); deny != nil {
return deny, nil
}

partition := int32(-1)
if p := request.GetFloat("partition", -1); p >= 0 {
partition = int32(p)
}
if limit < 1 {
limit = 1
}
if limit > 1000 {
limit = 1000
}

deserializer := request.GetString("deserializer", "auto")
messages, err := s.kafkaManager.Consume(ctx, cluster, topic, partition, offset, limit, timeoutMs, deserializer)
if err != nil {
s.logAudit(agentID, "kafka_consume", args, cluster, "error", 0, err, start)
return mcp.NewToolResultError(fmt.Sprintf("Failed to consume: %v", err)), nil
}

s.metrics.MessagesConsumed.WithLabelValues(topic, agentID, s.resolveClusterName(cluster)).Add(float64(len(messages)))
s.logAudit(agentID, "kafka_consume", args, cluster, "success", len(messages), nil, start)
return marshalResult(map[string]any{"messages": messages, "count": len(messages), "topic": topic, "deserialization": deserializer})
}

func (s *Server) handleProduce(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
start := time.Now()
agentID := extractAgentID(request)
topic := request.GetString("topic", "")
cluster := request.GetString("cluster", "")
args := request.GetArguments()

if topic == "" {
return mcp.NewToolResultError("topic is required"), nil
}
if deny := s.checkRateLimit(agentID, "kafka_produce"); deny != nil {
return deny, nil
}
if deny := s.checkAuth(agentID, "kafka_produce", auth.ResourceTopic, topic, auth.ActionWrite); deny != nil {
return deny, nil
}

rawArgs := request.GetRawArguments()
rawMap, ok := rawArgs.(map[string]any)
if !ok {
return mcp.NewToolResultError("invalid arguments"), nil
}
msgData, ok := rawMap["messages"]
if !ok {
return mcp.NewToolResultError("messages is required"), nil
}

var messages []kafka.ProduceMessage
msgBytes, err := json.Marshal(msgData)
if err != nil {
return mcp.NewToolResultError(fmt.Sprintf("Invalid messages: %v", err)), nil
}
if err := json.Unmarshal(msgBytes, &messages); err != nil {
var single kafka.ProduceMessage
if err := json.Unmarshal(msgBytes, &single); err != nil {
return mcp.NewToolResultError(fmt.Sprintf("Invalid messages format: %v", err)), nil
}
messages = []kafka.ProduceMessage{single}
}
if len(messages) == 0 {
return mcp.NewToolResultError("at least one message required"), nil
}
if len(messages) > 100 {
return mcp.NewToolResultError("max 100 messages per call"), nil
}

serializer := request.GetString("serializer", "auto")
acks := request.GetString("acks", "all")
schemaID := request.GetInt("schema_id", -1)

deliveries, err := s.kafkaManager.Produce(ctx, cluster, topic, messages, kafka.ProduceOptions{
Acks: acks, Serializer: serializer, SchemaID: schemaID,
})
if err != nil {
s.logAudit(agentID, "kafka_produce", args, cluster, "error", 0, err, start)
return mcp.NewToolResultError(fmt.Sprintf("Failed to produce: %v", err)), nil
}

s.metrics.MessagesProduced.WithLabelValues(topic, agentID, s.resolveClusterName(cluster)).Add(float64(len(deliveries)))
s.logAudit(agentID, "kafka_produce", args, cluster, "success", len(deliveries), nil, start)
return marshalResult(map[string]any{"deliveries": deliveries, "count": len(deliveries), "topic": topic, "serialization": serializer, "acks": acks})
}

func (s *Server) handleListSchemas(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
start := time.Now()
agentID := extractAgentID(request)
filter := request.GetString("filter", "")
args := request.GetArguments()

if deny := s.checkRateLimit(agentID, "kafka_list_schemas"); deny != nil {
return deny, nil
}
if deny := s.checkAuth(agentID, "kafka_list_schemas", auth.ResourceSchema, "", auth.ActionRead); deny != nil {
return deny, nil
}

subjects, err := s.getSchemaClient("").ListSubjectsDetailed(ctx)
if err != nil {
s.logAudit(agentID, "kafka_list_schemas", args, "", "error", 0, err, start)
return mcp.NewToolResultError(fmt.Sprintf("Failed to list schemas: %v", err)), nil
}

if filter != "" {
var filtered []schema.Subject
for _, subj := range subjects {
if matchTopicFilter(filter, subj.SubjectName) {
filtered = append(filtered, subj)
}
}
subjects = filtered
}

s.logAudit(agentID, "kafka_list_schemas", args, "", "success", len(subjects), nil, start)
return marshalResult(map[string]any{"subjects": subjects, "total_count": len(subjects)})
}

func (s *Server) handleGetSchema(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
start := time.Now()
agentID := extractAgentID(request)
subject := request.GetString("subject", "")
version := request.GetString("version", "latest")
args := request.GetArguments()

if subject == "" {
return mcp.NewToolResultError("subject is required"), nil
}
if deny := s.checkRateLimit(agentID, "kafka_get_schema"); deny != nil {
return deny, nil
}
if deny := s.checkAuth(agentID, "kafka_get_schema", auth.ResourceSchema, "", auth.ActionRead); deny != nil {
return deny, nil
}

info, err := s.getSchemaClient("").GetSchema(ctx, subject, version)
if err != nil {
s.logAudit(agentID, "kafka_get_schema", args, "", "error", 0, err, start)
return mcp.NewToolResultError(fmt.Sprintf("Failed to get schema: %v", err)), nil
}

s.logAudit(agentID, "kafka_get_schema", args, "", "success", 0, nil, start)
return marshalResult(info)
}

func (s *Server) handleDescribeConsumerGroup(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
start := time.Now()
agentID := extractAgentID(request)
groupID := request.GetString("group_id", "")
cluster := request.GetString("cluster", "")
args := request.GetArguments()

if groupID == "" {
return mcp.NewToolResultError("group_id is required"), nil
}
if deny := s.checkRateLimit(agentID, "kafka_describe_consumer_group"); deny != nil {
return deny, nil
}
if deny := s.checkAuth(agentID, "kafka_describe_consumer_group", auth.ResourceConsumerGroup, "", auth.ActionDescribe); deny != nil {
return deny, nil
}

detail, err := s.kafkaManager.DescribeConsumerGroup(ctx, cluster, groupID)
if err != nil {
s.logAudit(agentID, "kafka_describe_consumer_group", args, cluster, "error", 0, err, start)
return mcp.NewToolResultError(fmt.Sprintf("Failed to describe group: %v", err)), nil
}

s.logAudit(agentID, "kafka_describe_consumer_group", args, cluster, "success", 0, nil, start)
return marshalResult(detail)
}

func (s *Server) handleListConsumerGroups(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
start := time.Now()
agentID := extractAgentID(request)
stateFilter := request.GetString("state_filter", "")
cluster := request.GetString("cluster", "")
args := request.GetArguments()

if deny := s.checkRateLimit(agentID, "kafka_list_consumer_groups"); deny != nil {
return deny, nil
}
if deny := s.checkAuth(agentID, "kafka_list_consumer_groups", auth.ResourceConsumerGroup, "", auth.ActionDescribe); deny != nil {
return deny, nil
}

groups, err := s.kafkaManager.ListConsumerGroups(ctx, cluster)
if err != nil {
s.logAudit(agentID, "kafka_list_consumer_groups", args, cluster, "error", 0, err, start)
return mcp.NewToolResultError(fmt.Sprintf("Failed to list groups: %v", err)), nil
}

if stateFilter != "" {
var filtered []kafka.ConsumerGroupSummary
for _, g := range groups {
if strings.EqualFold(g.State, stateFilter) {
filtered = append(filtered, g)
}
}
groups = filtered
}

s.logAudit(agentID, "kafka_list_consumer_groups", args, cluster, "success", len(groups), nil, start)
return marshalResult(map[string]any{"groups": groups, "total_count": len(groups), "cluster": s.resolveClusterName(cluster)})
}

// ===================== P1 TOOL HANDLERS =====================

func (s *Server) handleCreateTopic(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
start := time.Now()
agentID := extractAgentID(request)
name := request.GetString("name", "")
cluster := request.GetString("cluster", "")
args := request.GetArguments()

if name == "" {
return mcp.NewToolResultError("name is required"), nil
}
if deny := s.checkRateLimit(agentID, "kafka_create_topic"); deny != nil {
return deny, nil
}
if deny := s.checkAuth(agentID, "kafka_create_topic", auth.ResourceTopic, name, auth.ActionCreate); deny != nil {
return deny, nil
}

partitions := int32(request.GetInt("partitions", 1))
replFactor := int16(request.GetInt("replication_factor", 1))

var configs map[string]string
if rawArgs := request.GetRawArguments(); rawArgs != nil {
if m, ok := rawArgs.(map[string]any); ok {
if cfgRaw, ok := m["configs"]; ok {
b, err := json.Marshal(cfgRaw)
if err == nil {
	if err := json.Unmarshal(b, &configs); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Invalid configs format: %v", err)), nil
	}
}
}
}
}

err := s.kafkaManager.CreateTopic(ctx, cluster, kafka.CreateTopicRequest{
Name: name, Partitions: partitions, ReplicationFactor: replFactor, Configs: configs,
})
if err != nil {
s.logAudit(agentID, "kafka_create_topic", args, cluster, "error", 0, err, start)
return mcp.NewToolResultError(fmt.Sprintf("Failed to create topic: %v", err)), nil
}

s.logAudit(agentID, "kafka_create_topic", args, cluster, "success", 0, nil, start)
return marshalResult(map[string]any{"status": "created", "topic": name, "partitions": partitions, "replication_factor": replFactor})
}

func (s *Server) handleAlterTopic(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
start := time.Now()
agentID := extractAgentID(request)
name := request.GetString("name", "")
cluster := request.GetString("cluster", "")
args := request.GetArguments()

if name == "" {
return mcp.NewToolResultError("name is required"), nil
}
if deny := s.checkRateLimit(agentID, "kafka_alter_topic"); deny != nil {
return deny, nil
}
if deny := s.checkAuth(agentID, "kafka_alter_topic", auth.ResourceTopic, name, auth.ActionAdmin); deny != nil {
return deny, nil
}

var configs map[string]string
if rawArgs := request.GetRawArguments(); rawArgs != nil {
if m, ok := rawArgs.(map[string]any); ok {
if cfgRaw, ok := m["configs"]; ok {
b, err := json.Marshal(cfgRaw)
if err == nil {
	if err := json.Unmarshal(b, &configs); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Invalid configs format: %v", err)), nil
	}
}
}
}
}
if len(configs) == 0 {
return mcp.NewToolResultError("configs is required"), nil
}

err := s.kafkaManager.AlterTopic(ctx, cluster, kafka.AlterTopicRequest{Name: name, Configs: configs})
if err != nil {
s.logAudit(agentID, "kafka_alter_topic", args, cluster, "error", 0, err, start)
return mcp.NewToolResultError(fmt.Sprintf("Failed to alter topic: %v", err)), nil
}

s.logAudit(agentID, "kafka_alter_topic", args, cluster, "success", 0, nil, start)
return marshalResult(map[string]any{"status": "altered", "topic": name, "configs": configs})
}

func (s *Server) handleDeleteTopic(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
start := time.Now()
agentID := extractAgentID(request)
name := request.GetString("name", "")
cluster := request.GetString("cluster", "")
args := request.GetArguments()

if name == "" {
return mcp.NewToolResultError("name is required"), nil
}
if deny := s.checkRateLimit(agentID, "kafka_delete_topic"); deny != nil {
return deny, nil
}
if deny := s.checkAuth(agentID, "kafka_delete_topic", auth.ResourceTopic, name, auth.ActionDelete); deny != nil {
return deny, nil
}

err := s.kafkaManager.DeleteTopic(ctx, cluster, name)
if err != nil {
s.logAudit(agentID, "kafka_delete_topic", args, cluster, "error", 0, err, start)
return mcp.NewToolResultError(fmt.Sprintf("Failed to delete topic: %v", err)), nil
}

s.logAudit(agentID, "kafka_delete_topic", args, cluster, "success", 0, nil, start)
return marshalResult(map[string]any{"status": "deleted", "topic": name})
}

func (s *Server) handleSearch(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
start := time.Now()
agentID := extractAgentID(request)
topic := request.GetString("topic", "")
cluster := request.GetString("cluster", "")
args := request.GetArguments()

if topic == "" {
return mcp.NewToolResultError("topic is required"), nil
}
if deny := s.checkRateLimit(agentID, "kafka_search"); deny != nil {
return deny, nil
}
if deny := s.checkAuth(agentID, "kafka_search", auth.ResourceTopic, topic, auth.ActionRead); deny != nil {
return deny, nil
}

req := kafka.SearchRequest{
Topic:         topic,
KeyFilter:     request.GetString("key_filter", ""),
HeaderKey:     request.GetString("header_key", ""),
HeaderValue:   request.GetString("header_value", ""),
ValueContains: request.GetString("value_contains", ""),
StartTime:     request.GetString("start_time", ""),
EndTime:       request.GetString("end_time", ""),
MaxScan:       request.GetInt("max_scan", 10000),
Limit:         request.GetInt("limit", 10),
}

matched, scanned, err := s.kafkaManager.SearchMessages(ctx, cluster, req)
if err != nil {
s.logAudit(agentID, "kafka_search", args, cluster, "error", 0, err, start)
return mcp.NewToolResultError(fmt.Sprintf("Failed to search: %v", err)), nil
}

s.logAudit(agentID, "kafka_search", args, cluster, "success", len(matched), nil, start)
return marshalResult(map[string]any{
"messages":         matched,
"matched_count":    len(matched),
"scanned_count":    scanned,
"topic":            topic,
})
}

func (s *Server) handleResetOffsets(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
start := time.Now()
agentID := extractAgentID(request)
groupID := request.GetString("group_id", "")
topic := request.GetString("topic", "")
strategy := request.GetString("strategy", "")
value := request.GetString("value", "")
dryRun := request.GetBool("dry_run", false)
cluster := request.GetString("cluster", "")
args := request.GetArguments()

if groupID == "" || topic == "" || strategy == "" {
return mcp.NewToolResultError("group_id, topic, and strategy are required"), nil
}
if deny := s.checkRateLimit(agentID, "kafka_reset_offsets"); deny != nil {
return deny, nil
}
if deny := s.checkAuth(agentID, "kafka_reset_offsets", auth.ResourceConsumerGroup, "", auth.ActionReset); deny != nil {
return deny, nil
}

result, err := s.kafkaManager.ResetOffsets(ctx, cluster, kafka.ResetOffsetsRequest{
GroupID: groupID, Topic: topic, Strategy: strategy, Value: value, DryRun: dryRun,
})
if err != nil {
s.logAudit(agentID, "kafka_reset_offsets", args, cluster, "error", 0, err, start)
return mcp.NewToolResultError(fmt.Sprintf("Failed to reset offsets: %v", err)), nil
}

s.logAudit(agentID, "kafka_reset_offsets", args, cluster, "success", len(result.Offsets), nil, start)
return marshalResult(result)
}

func (s *Server) handleCheckSchemaCompatibility(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
start := time.Now()
agentID := extractAgentID(request)
subject := request.GetString("subject", "")
schemaStr := request.GetString("schema", "")
schemaType := request.GetString("schema_type", "AVRO")
version := request.GetString("version", "latest")
args := request.GetArguments()

if subject == "" || schemaStr == "" {
return mcp.NewToolResultError("subject and schema are required"), nil
}
if deny := s.checkRateLimit(agentID, "kafka_check_schema_compatibility"); deny != nil {
return deny, nil
}
if deny := s.checkAuth(agentID, "kafka_check_schema_compatibility", auth.ResourceSchema, "", auth.ActionRead); deny != nil {
return deny, nil
}

result, err := s.getSchemaClient("").CheckCompatibility(ctx, subject, version, schemaStr, schemaType)
if err != nil {
s.logAudit(agentID, "kafka_check_schema_compatibility", args, "", "error", 0, err, start)
return mcp.NewToolResultError(fmt.Sprintf("Failed to check compatibility: %v", err)), nil
}

s.logAudit(agentID, "kafka_check_schema_compatibility", args, "", "success", 0, nil, start)
return marshalResult(result)
}

func (s *Server) handleAnalyzeDLQ(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
start := time.Now()
agentID := extractAgentID(request)
topic := request.GetString("topic", "")
limit := request.GetInt("limit", 100)
cluster := request.GetString("cluster", "")
args := request.GetArguments()

if topic == "" {
return mcp.NewToolResultError("topic is required"), nil
}
if deny := s.checkRateLimit(agentID, "kafka_analyze_dlq"); deny != nil {
return deny, nil
}
if deny := s.checkAuth(agentID, "kafka_analyze_dlq", auth.ResourceTopic, topic, auth.ActionRead); deny != nil {
return deny, nil
}

result, err := s.kafkaManager.AnalyzeDLQ(ctx, cluster, topic, limit)
if err != nil {
s.logAudit(agentID, "kafka_analyze_dlq", args, cluster, "error", 0, err, start)
return mcp.NewToolResultError(fmt.Sprintf("Failed to analyze DLQ: %v", err)), nil
}

s.logAudit(agentID, "kafka_analyze_dlq", args, cluster, "success", result.TotalMessages, nil, start)
return marshalResult(result)
}

func (s *Server) handleSubscribe(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
start := time.Now()
agentID := extractAgentID(request)
topic := request.GetString("topic", "")
cluster := request.GetString("cluster", "")
args := request.GetArguments()

if topic == "" {
return mcp.NewToolResultError("topic is required"), nil
}
if deny := s.checkRateLimit(agentID, "kafka_subscribe"); deny != nil {
return deny, nil
}
if deny := s.checkAuth(agentID, "kafka_subscribe", auth.ResourceTopic, topic, auth.ActionRead); deny != nil {
return deny, nil
}

result, err := s.kafkaManager.Subscribe(ctx, cluster, kafka.SubscribeRequest{
Topic:        topic,
GroupID:      request.GetString("group_id", ""),
AutoCommit:   request.GetBool("auto_commit", true),
MaxMessages:  request.GetInt("max_messages", 100),
TimeoutMs:    request.GetInt("timeout_ms", 10000),
Deserializer: request.GetString("deserializer", "auto"),
})
if err != nil {
s.logAudit(agentID, "kafka_subscribe", args, cluster, "error", 0, err, start)
return mcp.NewToolResultError(fmt.Sprintf("Failed to subscribe: %v", err)), nil
}

s.metrics.MessagesConsumed.WithLabelValues(topic, agentID, s.resolveClusterName(cluster)).Add(float64(result.Count))
s.logAudit(agentID, "kafka_subscribe", args, cluster, "success", result.Count, nil, start)
return marshalResult(result)
}

// ===================== P2 TOOL HANDLERS =====================

func (s *Server) handleAggregate(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
start := time.Now()
agentID := extractAgentID(request)
topic := request.GetString("topic", "")
cluster := request.GetString("cluster", "")
args := request.GetArguments()

if topic == "" {
return mcp.NewToolResultError("topic is required"), nil
}
if deny := s.checkRateLimit(agentID, "kafka_aggregate"); deny != nil {
return deny, nil
}
if deny := s.checkAuth(agentID, "kafka_aggregate", auth.ResourceTopic, topic, auth.ActionRead); deny != nil {
return deny, nil
}

result, err := s.kafkaManager.Aggregate(ctx, cluster, kafka.AggregateRequest{
Topic:    topic,
Field:    request.GetString("field", ""),
Function: request.GetString("function", "count"),
WindowMs: int64(request.GetInt("window_ms", 300000)),
})
if err != nil {
s.logAudit(agentID, "kafka_aggregate", args, cluster, "error", 0, err, start)
return mcp.NewToolResultError(fmt.Sprintf("Failed to aggregate: %v", err)), nil
}

s.logAudit(agentID, "kafka_aggregate", args, cluster, "success", result.MessageCount, nil, start)
return marshalResult(result)
}

func (s *Server) handleSuggestSchemaEvolution(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
start := time.Now()
agentID := extractAgentID(request)
subject := request.GetString("subject", "")
change := request.GetString("change", "")
args := request.GetArguments()

if subject == "" || change == "" {
return mcp.NewToolResultError("subject and change are required"), nil
}
if deny := s.checkRateLimit(agentID, "kafka_suggest_schema_evolution"); deny != nil {
return deny, nil
}

// Fetch current schema for context
var currentSchema map[string]any
sc := s.getSchemaClient("")
if sc != nil {
info, err := sc.GetSchema(ctx, subject, "latest")
if err == nil && info != nil {
	if m, ok := info.Schema.(map[string]any); ok {
		currentSchema = m
	}
}
}

suggestions := kafka.SuggestSchemaEvolution(currentSchema, change)
s.logAudit(agentID, "kafka_suggest_schema_evolution", args, "", "success", len(suggestions), nil, start)
return marshalResult(map[string]any{
"subject":     subject,
"change":      change,
"suggestions": suggestions,
"count":       len(suggestions),
})
}

func (s *Server) handleListConnectors(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
start := time.Now()
agentID := extractAgentID(request)
cluster := request.GetString("cluster", "")
args := request.GetArguments()

if deny := s.checkRateLimit(agentID, "kafka_list_connectors"); deny != nil {
return deny, nil
}

clCfg, err := s.kafkaManager.GetClusterConfig(cluster)
if err != nil {
return mcp.NewToolResultError(fmt.Sprintf("Cluster error: %v", err)), nil
}

client := kafka.NewConnectClient(clCfg.ConnectURL)
if client == nil {
return mcp.NewToolResultError("Kafka Connect URL not configured for this cluster. Set connect_url in cluster config."), nil
}

connectors, err := client.ListConnectors(ctx)
if err != nil {
s.logAudit(agentID, "kafka_list_connectors", args, cluster, "error", 0, err, start)
return mcp.NewToolResultError(fmt.Sprintf("Failed to list connectors: %v", err)), nil
}

s.logAudit(agentID, "kafka_list_connectors", args, cluster, "success", len(connectors), nil, start)
return marshalResult(map[string]any{"connectors": connectors, "total_count": len(connectors)})
}

func (s *Server) handleDescribeConnector(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
start := time.Now()
agentID := extractAgentID(request)
name := request.GetString("name", "")
cluster := request.GetString("cluster", "")
args := request.GetArguments()

if name == "" {
return mcp.NewToolResultError("name is required"), nil
}
if deny := s.checkRateLimit(agentID, "kafka_describe_connector"); deny != nil {
return deny, nil
}

clCfg, err := s.kafkaManager.GetClusterConfig(cluster)
if err != nil {
return mcp.NewToolResultError(fmt.Sprintf("Cluster error: %v", err)), nil
}

client := kafka.NewConnectClient(clCfg.ConnectURL)
if client == nil {
return mcp.NewToolResultError("Kafka Connect URL not configured for this cluster."), nil
}

detail, err := client.DescribeConnector(ctx, name)
if err != nil {
s.logAudit(agentID, "kafka_describe_connector", args, cluster, "error", 0, err, start)
return mcp.NewToolResultError(fmt.Sprintf("Failed to describe connector: %v", err)), nil
}

s.logAudit(agentID, "kafka_describe_connector", args, cluster, "success", 0, nil, start)
return marshalResult(detail)
}

func (s *Server) handleJoin(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
start := time.Now()
agentID := extractAgentID(request)
leftTopic := request.GetString("left_topic", "")
rightTopic := request.GetString("right_topic", "")
cluster := request.GetString("cluster", "")
args := request.GetArguments()

if leftTopic == "" || rightTopic == "" {
return mcp.NewToolResultError("left_topic and right_topic are required"), nil
}
if deny := s.checkRateLimit(agentID, "kafka_join"); deny != nil {
return deny, nil
}
if deny := s.checkAuth(agentID, "kafka_join", auth.ResourceTopic, leftTopic, auth.ActionRead); deny != nil {
return deny, nil
}
if deny := s.checkAuth(agentID, "kafka_join", auth.ResourceTopic, rightTopic, auth.ActionRead); deny != nil {
return deny, nil
}

result, err := s.kafkaManager.JoinTopics(ctx, cluster, kafka.JoinRequest{
LeftTopic:  leftTopic,
RightTopic: rightTopic,
KeyField:   request.GetString("key_field", ""),
WindowMs:   int64(request.GetInt("window_ms", 60000)),
Limit:      request.GetInt("limit", 100),
})
if err != nil {
s.logAudit(agentID, "kafka_join", args, cluster, "error", 0, err, start)
return mcp.NewToolResultError(fmt.Sprintf("Failed to join: %v", err)), nil
}

s.logAudit(agentID, "kafka_join", args, cluster, "success", result.MatchCount, nil, start)
return marshalResult(result)
}

func (s *Server) handleNaturalLanguageQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
start := time.Now()
agentID := extractAgentID(request)
query := request.GetString("query", "")
cluster := request.GetString("cluster", "")
args := request.GetArguments()

if query == "" {
return mcp.NewToolResultError("query is required"), nil
}
if deny := s.checkRateLimit(agentID, "kafka_query"); deny != nil {
return deny, nil
}

// Get topic list for matching
topics, err := s.kafkaManager.ListTopics(ctx, cluster, false)
if err != nil {
s.logAudit(agentID, "kafka_query", args, cluster, "error", 0, err, start)
return mcp.NewToolResultError(fmt.Sprintf("Failed to list topics: %v", err)), nil
}

result, err := s.kafkaManager.NaturalLanguageQuery(ctx, cluster, query, topics)
if err != nil {
s.logAudit(agentID, "kafka_query", args, cluster, "error", 0, err, start)
return mcp.NewToolResultError(fmt.Sprintf("Failed to execute query: %v", err)), nil
}

s.logAudit(agentID, "kafka_query", args, cluster, "success", result.Count, nil, start)
return marshalResult(result)
}

// ===================== RESOURCE HANDLERS =====================

func (s *Server) handleClusterMetadata(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
start := time.Now()
agentID := extractAgentIDFromResource(request)
meta, err := s.kafkaManager.GetClusterMetadata(ctx, "")
if err != nil {
return nil, fmt.Errorf("getting cluster metadata: %w", err)
}
s.logAudit(agentID, "resource:kafka://cluster/metadata", nil, "", "success", 0, nil, start)
return marshalResourceJSON("kafka://cluster/metadata", meta)
}

func (s *Server) handleClusterList(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
start := time.Now()
agentID := extractAgentIDFromResource(request)
type clusterSummary struct {
	Name             string `json:"name"`
	BootstrapServers string `json:"bootstrap_servers"`
	HasSchemaRegistry bool  `json:"has_schema_registry"`
	IsDefault        bool   `json:"is_default"`
}
var clusters []clusterSummary
for _, cl := range s.config.Clusters {
	clusters = append(clusters, clusterSummary{
		Name:             cl.Name,
		BootstrapServers: cl.BootstrapServers,
		HasSchemaRegistry: cl.SchemaRegistry.URL != "",
		IsDefault:        cl.Default,
	})
}
s.logAudit(agentID, "resource:kafka://clusters", nil, "", "success", len(clusters), nil, start)
return marshalResourceJSON("kafka://clusters", map[string]any{"clusters": clusters, "total_count": len(clusters)})
}

func (s *Server) handleTopicCatalog(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
start := time.Now()
agentID := extractAgentIDFromResource(request)
topics, err := s.kafkaManager.ListTopics(ctx, "", false)
if err != nil {
return nil, fmt.Errorf("listing topics: %w", err)
}
// Filter by policy
if s.config.Policies.DefaultDeny {
var allowed []kafka.TopicSummary
for _, t := range topics {
	if s.policyEngine.Authorize(agentID, auth.ResourceTopic, t.Name, auth.ActionRead) {
		allowed = append(allowed, t)
	}
}
topics = allowed
}
s.logAudit(agentID, "resource:kafka://topics", nil, "", "success", len(topics), nil, start)
return marshalResourceJSON("kafka://topics", map[string]any{"topics": topics, "total_count": len(topics)})
}

func (s *Server) handleSchemaCatalog(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
start := time.Now()
agentID := extractAgentIDFromResource(request)
sc := s.getSchemaClient("")
if sc == nil {
return marshalResourceJSON("kafka://schemas", map[string]any{"error": "Schema Registry not configured"})
}
subjects, err := sc.ListSubjectsDetailed(ctx)
if err != nil {
return nil, fmt.Errorf("listing schemas: %w", err)
}
s.logAudit(agentID, "resource:kafka://schemas", nil, "", "success", len(subjects), nil, start)
return marshalResourceJSON("kafka://schemas", map[string]any{"subjects": subjects, "total_count": len(subjects)})
}

func (s *Server) handleGroupCatalog(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
start := time.Now()
agentID := extractAgentIDFromResource(request)
groups, err := s.kafkaManager.ListConsumerGroups(ctx, "")
if err != nil {
return nil, fmt.Errorf("listing consumer groups: %w", err)
}
s.logAudit(agentID, "resource:kafka://groups", nil, "", "success", len(groups), nil, start)
return marshalResourceJSON("kafka://groups", map[string]any{"groups": groups, "total_count": len(groups)})
}

func (s *Server) handleAuditLogResource(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
entries := s.auditLogger.RecentEntries(1000)
return marshalResourceJSON("kafka://audit/log", map[string]any{"entries": entries, "count": len(entries)})
}

func (s *Server) handleTopicGraphResource(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
start := time.Now()
agentID := extractAgentIDFromResource(request)
graph, err := s.kafkaManager.GetTopicGraph(ctx, "")
if err != nil {
return nil, fmt.Errorf("building topic graph: %w", err)
}
s.logAudit(agentID, "resource:kafka://topic_graph", nil, "", "success", len(graph.Nodes), nil, start)
return marshalResourceJSON("kafka://topic_graph", graph)
}

func (s *Server) handleTopicResource(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
start := time.Now()
agentID := extractAgentIDFromResource(request)
uri := request.Params.URI
name := strings.TrimPrefix(uri, "kafka://topics/")
if name == "" || name == uri {
return nil, fmt.Errorf("invalid topic URI")
}
if s.config.Policies.DefaultDeny && !s.policyEngine.Authorize(agentID, auth.ResourceTopic, name, auth.ActionRead) {
return nil, fmt.Errorf("unauthorized: agent %q cannot read topic %q", agentID, name)
}
detail, err := s.kafkaManager.DescribeTopic(ctx, "", name)
if err != nil {
return nil, fmt.Errorf("describing topic: %w", err)
}
s.logAudit(agentID, "resource:kafka://topics/"+name, nil, "", "success", 0, nil, start)
return marshalResourceJSON(uri, detail)
}

func (s *Server) handleSchemaResource(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
start := time.Now()
agentID := extractAgentIDFromResource(request)
sc := s.getSchemaClient("")
if sc == nil {
return nil, fmt.Errorf("Schema Registry not configured")
}
uri := request.Params.URI
subject := strings.TrimPrefix(uri, "kafka://schemas/")
if subject == "" || subject == uri {
return nil, fmt.Errorf("invalid schema URI")
}
if s.config.Policies.DefaultDeny && !s.policyEngine.Authorize(agentID, auth.ResourceSchema, "", auth.ActionRead) {
return nil, fmt.Errorf("unauthorized: agent %q cannot read schemas", agentID)
}
info, err := sc.GetSchema(ctx, subject, "latest")
if err != nil {
return nil, fmt.Errorf("getting schema: %w", err)
}
s.logAudit(agentID, "resource:kafka://schemas/"+subject, nil, "", "success", 0, nil, start)
return marshalResourceJSON(uri, info)
}

func (s *Server) handleGroupResource(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
start := time.Now()
agentID := extractAgentIDFromResource(request)
uri := request.Params.URI
groupID := strings.TrimPrefix(uri, "kafka://groups/")
if groupID == "" || groupID == uri {
return nil, fmt.Errorf("invalid group URI")
}
if s.config.Policies.DefaultDeny && !s.policyEngine.Authorize(agentID, auth.ResourceConsumerGroup, "", auth.ActionDescribe) {
return nil, fmt.Errorf("unauthorized: agent %q cannot describe consumer groups", agentID)
}
detail, err := s.kafkaManager.DescribeConsumerGroup(ctx, "", groupID)
if err != nil {
return nil, fmt.Errorf("describing group: %w", err)
}
s.logAudit(agentID, "resource:kafka://groups/"+groupID, nil, "", "success", 0, nil, start)
return marshalResourceJSON(uri, detail)
}
