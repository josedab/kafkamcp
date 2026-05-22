package server

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/config"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

// newFullServer builds a Server via the real New() constructor so that
// every sub-engine (codec, dataquality, gateway, gitops, etc.) is wired
// up. The Kafka manager is a no-op mock so handlers that fan out to a
// broker fail fast with a benign error.
func newFullServer(t *testing.T) *Server {
	t.Helper()
	cfg := &config.Config{
		Server:      config.ServerConfig{Transport: "stdio"},
		Clusters:    []config.ClusterConfig{},
		SchemaCache: config.CacheConfig{MaxEntries: 100, TTLSeconds: 60},
		Audit:       config.AuditConfig{Enabled: false, MaxEntries: 100},
		Policies:    config.PolicyConfig{DefaultDeny: false},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mock := &kafka.MockKafkaManager{}
	srv, err := New(cfg, mock, logger, "test")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srv
}

func callHandler(
	t *testing.T,
	s *Server,
	fn func(context.Context, mcp.CallToolRequest, string, map[string]any, time.Time) (*mcp.CallToolResult, error),
	args map[string]any,
) *mcp.CallToolResult {
	t.Helper()
	req := makeToolRequest(args)
	res, err := fn(context.Background(), req, "test-agent", args, time.Now())
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if res == nil {
		t.Fatal("handler returned nil result")
	}
	return res
}

// =====================================================================
// Codec handlers
// =====================================================================

func TestHandleCodecDetectFormat(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	callHandler(t, s, s.handleCodecDetectFormat, map[string]any{"data": "hello"})
	// Missing arg path:
	r := callHandler(t, s, s.handleCodecDetectFormat, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when data missing")
	}
}

func TestHandleCodecDecode_MissingArg(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	r := callHandler(t, s, s.handleCodecDecode, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when data missing")
	}
}

func TestHandleCodecDecode_InvalidPayload(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	// Likely invalid payload — codec engine returns error.
	_ = callHandler(t, s, s.handleCodecDecode, map[string]any{"data": "rawtext"})
}

func TestHandleCodecEncode_MissingArg(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	r := callHandler(t, s, s.handleCodecEncode, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when value missing")
	}
}

func TestHandleCodecEncode_Json(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	_ = callHandler(t, s, s.handleCodecEncode, map[string]any{
		"value":     `{"x":1}`,
		"schema_id": float64(0),
		"format":    "json",
	})
}

// =====================================================================
// Data quality handlers
// =====================================================================

func TestHandleQualityScore_MissingTopic(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	r := callHandler(t, s, s.handleQualityScore, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when topic missing")
	}
}

func TestHandleQualityScore_FreshTopic(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	_ = callHandler(t, s, s.handleQualityScore, map[string]any{"topic": "orders"})
	// Second call should hit cached path.
	_ = callHandler(t, s, s.handleQualityScore, map[string]any{"topic": "orders"})
}

func TestHandleQualityReport(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	r := callHandler(t, s, s.handleQualityReport, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when topic missing")
	}
	_ = callHandler(t, s, s.handleQualityReport, map[string]any{"topic": "orders"})
}

func TestHandleSLAStatus_All(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	_ = callHandler(t, s, s.handleSLAStatus, map[string]any{})
	_ = callHandler(t, s, s.handleSLAStatus, map[string]any{"topic": "unknown"})
}

func TestHandleSLAConfigure(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	r := callHandler(t, s, s.handleSLAConfigure, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when topic missing")
	}
	_ = callHandler(t, s, s.handleSLAConfigure, map[string]any{
		"topic":            "orders",
		"min_freshness":    float64(0.5),
		"min_completeness": float64(0.95),
		"min_overall":      float64(0.7),
	})
	// Read it back via SLA status.
	_ = callHandler(t, s, s.handleSLAStatus, map[string]any{"topic": "orders"})
}

// =====================================================================
// Collaboration handlers
// =====================================================================

func TestHandleLocks(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	r := callHandler(t, s, s.handleLockAcquire, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when resource missing")
	}
	_ = callHandler(t, s, s.handleLockAcquire, map[string]any{
		"resource":    "topic:orders",
		"ttl_seconds": float64(10),
	})
	_ = callHandler(t, s, s.handleLockList, map[string]any{})
	_ = callHandler(t, s, s.handleLockRelease, map[string]any{"resource": "topic:orders"})

	r = callHandler(t, s, s.handleLockRelease, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when resource missing")
	}
}

func TestHandleClocks(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	r := callHandler(t, s, s.handleClockGet, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when resource missing")
	}
	_ = callHandler(t, s, s.handleClockGet, map[string]any{"resource": "orders"})

	r = callHandler(t, s, s.handleClockIncrement, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when resource missing")
	}
	_ = callHandler(t, s, s.handleClockIncrement, map[string]any{"resource": "orders"})
}

func TestHandleMessages(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	r := callHandler(t, s, s.handleMessageSend, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when to/subject missing")
	}
	_ = callHandler(t, s, s.handleMessageSend, map[string]any{
		"to":      "agent-2",
		"subject": "hi",
		"body":    "hello",
	})
	_ = callHandler(t, s, s.handleMessageInbox, map[string]any{"unread_only": true})
}

func TestHandleAgents(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	_ = callHandler(t, s, s.handleAgentRegister, map[string]any{
		"capabilities": []any{"reader", "writer"},
	})
	_ = callHandler(t, s, s.handleAgentList, map[string]any{})
}

func TestHandleTopicLeases(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	r := callHandler(t, s, s.handleTopicLeaseAcquire, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when topic missing")
	}
	_ = callHandler(t, s, s.handleTopicLeaseAcquire, map[string]any{"topic": "orders"})
	_ = callHandler(t, s, s.handleTopicLeaseList, map[string]any{})
	_ = callHandler(t, s, s.handleTopicLeaseRenew, map[string]any{"topic": "orders"})
	_ = callHandler(t, s, s.handleTopicLeaseRelease, map[string]any{"topic": "orders"})
}

// =====================================================================
// Onboarding handlers
// =====================================================================

func TestHandleOnboarding(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	_ = callHandler(t, s, s.handleQuickstart, map[string]any{})
	_ = callHandler(t, s, s.handleDiscoverClusters, map[string]any{})
	_ = callHandler(t, s, s.handleSuggestRBAC, map[string]any{})
}

// =====================================================================
// Incident handlers
// =====================================================================

func TestHandleIncident_List(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	_ = callHandler(t, s, s.handleIncidentList, map[string]any{})
	_ = callHandler(t, s, s.handleIncidentStats, map[string]any{})
}

// =====================================================================
// OPA / policy handlers
// =====================================================================

func TestHandlePolicy(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	_ = callHandler(t, s, s.handlePolicyEvaluate, map[string]any{
		"input": map[string]any{"action": "read"},
	})
	_ = callHandler(t, s, s.handlePolicySimulate, map[string]any{
		"input": map[string]any{"action": "write"},
	})
	_ = callHandler(t, s, s.handlePolicyManage, map[string]any{"operation": "list"})
}

// =====================================================================
// GitOps handlers
// =====================================================================

func TestHandleGitOps(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	_ = callHandler(t, s, s.handleGitOpsHistory, map[string]any{})
	_ = callHandler(t, s, s.handleGitOpsSetState, map[string]any{"state": "synced"})
	_ = callHandler(t, s, s.handleGitOpsReconcile, map[string]any{})
}

// =====================================================================
// LLM pipeline handlers
// =====================================================================

func TestHandleLLMPipeline_List(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	_ = callHandler(t, s, s.handleLLMPipelineList, map[string]any{})
}

func TestHandleLLMPipeline_Status(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	r := callHandler(t, s, s.handleLLMPipelineStatus, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when pipeline_id missing")
	}
	_ = callHandler(t, s, s.handleLLMPipelineStatus, map[string]any{"pipeline_id": "nonexistent"})
}

func TestHandleLLMPipeline_Create(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	r := callHandler(t, s, s.handleLLMPipelineCreate, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when topics missing")
	}
	_ = callHandler(t, s, s.handleLLMPipelineCreate, map[string]any{
		"topics": "orders,events",
	})
}

// =====================================================================
// Event sourcing handlers
// =====================================================================

func TestHandleActionReplay(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	r := callHandler(t, s, s.handleActionReplay, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when session_id missing")
	}
	_ = callHandler(t, s, s.handleActionReplay, map[string]any{"session_id": "unknown"})
}

func TestHandleActionAnalytics(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	_ = callHandler(t, s, s.handleActionAnalytics, map[string]any{})
}

func TestHandleActionExport(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	_ = callHandler(t, s, s.handleActionExport, map[string]any{})
	_ = callHandler(t, s, s.handleActionExport, map[string]any{
		"agent_id": "x",
		"tool":     "y",
		"status":   "success",
		"limit":    float64(10),
	})
}

func TestHandleActionStats(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	_ = callHandler(t, s, s.handleActionStats, map[string]any{})
}

// =====================================================================
// Gateway handlers (no backends configured, errors expected)
// =====================================================================

func TestHandleGateway_ListBackends(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	_ = callHandler(t, s, s.handleGatewayListBackends, map[string]any{})
}

func TestHandleGateway_Validation(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	r := callHandler(t, s, s.handleGatewayProduce, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError")
	}
	r = callHandler(t, s, s.handleGatewayConsume, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError")
	}
	r = callHandler(t, s, s.handleGatewayListTopics, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError")
	}
	r = callHandler(t, s, s.handleGatewayCreateTopic, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError")
	}
	// With unknown backend — exercises non-validation branch.
	_ = callHandler(t, s, s.handleGatewayProduce, map[string]any{
		"backend": "missing",
		"topic":   "t",
		"value":   "v",
	})
	_ = callHandler(t, s, s.handleGatewayConsume, map[string]any{
		"backend": "missing",
		"topic":   "t",
		"limit":   float64(5),
	})
	_ = callHandler(t, s, s.handleGatewayListTopics, map[string]any{
		"backend": "missing",
	})
	_ = callHandler(t, s, s.handleGatewayCreateTopic, map[string]any{
		"backend":    "missing",
		"topic":      "t",
		"partitions": float64(3),
	})
}

// =====================================================================
// Retention handlers
// =====================================================================

func TestHandleRetention(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	r := callHandler(t, s, s.handleRetentionPolicy, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when topic missing")
	}
	// Set policy.
	_ = callHandler(t, s, s.handleRetentionPolicy, map[string]any{
		"topic":        "orders",
		"retention_ms": float64(86400000),
		"compliance":   "gdpr",
		"criticality":  "high",
	})
	// Get policy.
	_ = callHandler(t, s, s.handleRetentionPolicy, map[string]any{"topic": "orders"})
	// Unknown topic.
	_ = callHandler(t, s, s.handleRetentionPolicy, map[string]any{"topic": "no-policy"})

	_ = callHandler(t, s, s.handleRetentionRecommendations, map[string]any{})

	r = callHandler(t, s, s.handleRetentionChangeRequest, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when required args missing")
	}
	_ = callHandler(t, s, s.handleRetentionChangeRequest, map[string]any{
		"topic":               "orders",
		"target_retention_ms": float64(3600000),
		"reason":              "compaction",
	})

	r = callHandler(t, s, s.handleRetentionApprove, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when request_id missing")
	}
	_ = callHandler(t, s, s.handleRetentionApprove, map[string]any{"request_id": "nonexistent"})
}

// =====================================================================
// Semantic handlers
// =====================================================================

func TestHandleSemantic_Validation(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	r := callHandler(t, s, s.handleSemanticIndex, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when topic missing")
	}
	r = callHandler(t, s, s.handleSemanticSearch, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when query missing")
	}
	r = callHandler(t, s, s.handleRAGContext, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when query missing")
	}
}

func TestHandleSemantic_Stats(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	_ = callHandler(t, s, s.handleSemanticStats, map[string]any{})
	// Search/RAG against an empty index — should not error.
	_ = callHandler(t, s, s.handleSemanticSearch, map[string]any{
		"query": "hello",
		"limit": float64(5),
	})
	_ = callHandler(t, s, s.handleRAGContext, map[string]any{
		"query":      "hello",
		"max_tokens": float64(1000),
	})
}

// =====================================================================
// Threat detection handlers
// =====================================================================

func TestHandleThreat(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	_ = callHandler(t, s, s.handleThreatAlerts, map[string]any{"limit": float64(5)})
	_ = callHandler(t, s, s.handleThreatStats, map[string]any{})

	r := callHandler(t, s, s.handleThreatProfile, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when agent_id missing")
	}
	_ = callHandler(t, s, s.handleThreatProfile, map[string]any{"agent_id": "unknown"})
}

// =====================================================================
// Topic portal handlers
// =====================================================================

func TestHandleTopicPortal(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	r := callHandler(t, s, s.handleTopicRequest, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError")
	}
	_ = callHandler(t, s, s.handleTopicRequest, map[string]any{
		"topic_name":         "orders.v2",
		"justification":      "new event stream",
		"owner":              "data-team",
		"partitions":         float64(6),
		"replication_factor": float64(3),
		"retention_ms":       float64(86400000),
	})
	_ = callHandler(t, s, s.handleTopicRequestStatus, map[string]any{})
	_ = callHandler(t, s, s.handleTopicRequestStatus, map[string]any{
		"status": "pending",
		"limit":  float64(10),
	})
	_ = callHandler(t, s, s.handleTopicRequestStatus, map[string]any{
		"request_id": "nonexistent",
	})

	r = callHandler(t, s, s.handleTopicApprove, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError")
	}
	_ = callHandler(t, s, s.handleTopicApprove, map[string]any{"request_id": "nonexistent"})

	r = callHandler(t, s, s.handleTopicReject, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError")
	}
	_ = callHandler(t, s, s.handleTopicReject, map[string]any{
		"request_id": "nonexistent",
		"reason":     "too many partitions",
	})
}

// =====================================================================
// Pipeline handlers
// =====================================================================

func TestHandlePipeline(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	r := callHandler(t, s, s.handlePipelineCreate, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError")
	}
	_ = callHandler(t, s, s.handlePipelineCreate, map[string]any{
		"name":         "p1",
		"source_topic": "src",
		"sink_topic":   "sink",
	})

	r = callHandler(t, s, s.handlePipelineExecute, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError")
	}
	_ = callHandler(t, s, s.handlePipelineExecute, map[string]any{
		"pipeline_id": "nonexistent",
	})

	_ = callHandler(t, s, s.handlePipelineManage, map[string]any{"action": "list"})
	_ = callHandler(t, s, s.handlePipelineManage, map[string]any{
		"action":      "get",
		"pipeline_id": "nonexistent",
	})
	_ = callHandler(t, s, s.handlePipelineManage, map[string]any{"action": "get"})
	_ = callHandler(t, s, s.handlePipelineManage, map[string]any{"action": "unknown"})
}

// =====================================================================
// Operator handlers
// =====================================================================

func TestHandleOperator(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	r := callHandler(t, s, s.handleOperatorManage, map[string]any{"action": "create"})
	if !r.IsError {
		t.Error("expected IsError when name missing")
	}
	_ = callHandler(t, s, s.handleOperatorManage, map[string]any{
		"action":    "create",
		"name":      "my-mcp",
		"namespace": "kafka-mcp",
		"replicas":  float64(2),
		"image":     "ghcr.io/kafkamcp/kafkamcp",
		"version":   "v1",
		"transport": "stdio",
		"port":      float64(8080),
	})
	_ = callHandler(t, s, s.handleOperatorManage, map[string]any{
		"action":    "get",
		"name":      "my-mcp",
		"namespace": "kafka-mcp",
	})
	_ = callHandler(t, s, s.handleOperatorManage, map[string]any{
		"action":    "list",
		"namespace": "kafka-mcp",
	})
	_ = callHandler(t, s, s.handleOperatorManage, map[string]any{
		"action":    "scale",
		"name":      "my-mcp",
		"namespace": "kafka-mcp",
		"replicas":  float64(5),
	})
	_ = callHandler(t, s, s.handleOperatorManage, map[string]any{
		"action":    "manifest",
		"name":      "my-mcp",
		"namespace": "kafka-mcp",
	})
	_ = callHandler(t, s, s.handleOperatorManage, map[string]any{"action": "crd"})
	_ = callHandler(t, s, s.handleOperatorManage, map[string]any{
		"action":    "delete",
		"name":      "my-mcp",
		"namespace": "kafka-mcp",
	})
	_ = callHandler(t, s, s.handleOperatorManage, map[string]any{"action": "unknown"})
}

// =====================================================================
// Incident handlers (extended)
// =====================================================================

func TestHandleIncident_Full(t *testing.T) {
	t.Parallel()
	s := newFullServer(t)
	r := callHandler(t, s, s.handleIncidentReport, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when incident_id missing")
	}
	_ = callHandler(t, s, s.handleIncidentReport, map[string]any{"incident_id": "nonexistent"})

	r = callHandler(t, s, s.handleIncidentIngest, map[string]any{})
	if !r.IsError {
		t.Error("expected IsError when cluster/type missing")
	}
	_ = callHandler(t, s, s.handleIncidentIngest, map[string]any{
		"cluster":  "primary",
		"type":     "lag_spike",
		"severity": "warning",
		"topic":    "orders",
		"value":    float64(99.5),
	})
}
