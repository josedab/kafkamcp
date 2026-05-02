//go:build integration

package server

import (
	"context"
	"testing"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/config"
	"github.com/mark3labs/mcp-go/mcp"
)

// ===================== validateAgentID Tests =====================

func TestValidateAgentID_DefaultDenyDisabled(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	if result := s.validateAgentID("anonymous"); result != nil {
		t.Error("expected nil when default_deny is false")
	}
	if result := s.validateAgentID("any-agent"); result != nil {
		t.Error("expected nil for any agent when default_deny is false")
	}
}

func TestValidateAgentID_DefaultDenyEnabled(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)

	if result := s.validateAgentID("anonymous"); result == nil {
		t.Error("expected error for anonymous with default_deny")
	}
	if result := s.validateAgentID("unknown-agent"); result == nil {
		t.Error("expected error for unknown agent with default_deny")
	}
	if result := s.validateAgentID("allowed-agent"); result != nil {
		t.Error("expected nil for known agent with default_deny")
	}
}

// ===================== checkAuth Tests =====================

func TestCheckAuth_NoDefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	result := s.checkAuth("anonymous", "kafka_list_topics", auth.ResourceTopic, "*", auth.ActionRead)
	if result != nil {
		t.Error("expected nil (allowed) when default_deny is false")
	}
}

func TestCheckAuth_DefaultDeny_AllowedAgent(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	result := s.checkAuth("allowed-agent", "kafka_list_topics", auth.ResourceTopic, "my-topic", auth.ActionRead)
	if result != nil {
		t.Error("expected nil for allowed agent")
	}
}

func TestCheckAuth_DefaultDeny_UnknownAgent(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	result := s.checkAuth("unknown", "kafka_list_topics", auth.ResourceTopic, "*", auth.ActionRead)
	if result == nil {
		t.Error("expected deny for unknown agent")
	}
}

func TestCheckAuth_DefaultDeny_Anonymous(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	result := s.checkAuth("anonymous", "kafka_list_topics", auth.ResourceTopic, "*", auth.ActionRead)
	if result == nil {
		t.Error("expected deny for anonymous with default_deny")
	}
}

// ===================== checkRateLimit Tests =====================

func TestCheckRateLimit_NoLimit(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	result := s.checkRateLimit("anonymous", "kafka_list_topics")
	if result != nil {
		t.Error("expected nil when no rate limit set")
	}
}

func TestCheckRateLimit_WithLimit(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withRateLimit)
	// First request should succeed
	if result := s.checkRateLimit("rate-limited-agent", "kafka_list_topics"); result != nil {
		t.Error("first request should be allowed")
	}
	// Exhaust the limit
	var denied bool
	for i := 0; i < 100; i++ {
		if result := s.checkRateLimit("rate-limited-agent", "kafka_list_topics"); result != nil {
			denied = true
			break
		}
	}
	if !denied {
		t.Error("expected rate limit to be hit after many requests")
	}
}

// ===================== checkResourceRateLimit Tests =====================

func TestCheckResourceRateLimit_NoLimit(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	err := s.checkResourceRateLimit("anonymous", "kafka://cluster/health")
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

// ===================== hasSchemaRegistry Tests =====================

func TestHasSchemaRegistry_Empty(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	if s.hasSchemaRegistry() {
		t.Error("expected false with no schema clients")
	}
}

func TestHasSchemaRegistry_WithClient(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	sc, _ := schema.NewClient("http://localhost:8081", "", "", 100, 60)
	s.schemaClients["test-cluster"] = sc
	if !s.hasSchemaRegistry() {
		t.Error("expected true with a schema client")
	}
}

// ===================== getSchemaClient Tests =====================

func TestGetSchemaClient_NoClients(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	// kafkaManager is nil, so calling with a cluster name that triggers
	// DefaultClusterName will panic. Test with direct map access.
	if c := s.schemaClients["nonexistent"]; c != nil {
		t.Error("expected nil for nonexistent cluster")
	}
}

// ===================== acquireExpensiveOp / releaseExpensiveOp Tests =====================

func TestAcquireReleaseExpensiveOp(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()

	// Should succeed immediately
	if err := s.acquireExpensiveOp(ctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	s.releaseExpensiveOp()
}

func TestAcquireExpensiveOp_CancelledContext(t *testing.T) {
	t.Parallel()
	s := &Server{expensiveOps: make(chan struct{}, 1)}
	// Fill the semaphore
	s.expensiveOps <- struct{}{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := s.acquireExpensiveOp(ctx)
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

func TestAcquireExpensiveOp_FillAndDrain(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()

	// Fill all 5 slots
	for i := 0; i < 5; i++ {
		if err := s.acquireExpensiveOp(ctx); err != nil {
			t.Fatalf("slot %d: unexpected error: %v", i, err)
		}
	}
	// Release all
	for i := 0; i < 5; i++ {
		s.releaseExpensiveOp()
	}
	// Should succeed again
	if err := s.acquireExpensiveOp(ctx); err != nil {
		t.Fatalf("post-drain: unexpected error: %v", err)
	}
	s.releaseExpensiveOp()
}

// ===================== validateResourceAgentID Tests =====================

func TestValidateResourceAgentID_NoDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	if err := s.validateResourceAgentID("anonymous"); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestValidateResourceAgentID_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)

	if err := s.validateResourceAgentID("anonymous"); err == nil {
		t.Error("expected error for anonymous")
	}
	if err := s.validateResourceAgentID("unknown"); err == nil {
		t.Error("expected error for unknown agent")
	}
	if err := s.validateResourceAgentID("allowed-agent"); err != nil {
		t.Errorf("expected nil for allowed-agent, got %v", err)
	}
}

// ===================== Handler Validation Path Tests =====================
// These test the early-return paths before kafkaManager is accessed.

func TestHandleDescribeTopic_MissingTopic(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDescribeTopic(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	assertIsError(t, result)
}

func TestHandleConsume_MissingTopic(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleConsume(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleCreateTopic_MissingName(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleCreateTopic(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleCreateTopic_ExcessivePartitions(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleCreateTopic(ctx, makeToolRequest(map[string]any{
		"name": "test-topic", "partitions": float64(MaxPartitions + 1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleCreateTopic_ExcessiveReplication(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleCreateTopic(ctx, makeToolRequest(map[string]any{
		"name": "test-topic", "replication_factor": float64(MaxReplicationFactor + 1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleAlterTopic_MissingName(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleAlterTopic(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleDeleteTopic_MissingName(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDeleteTopic(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleSearch_MissingTopic(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleSearch(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleResetOffsets_MissingFields(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleResetOffsets(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleDescribeConsumerGroup_MissingGroupID(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDescribeConsumerGroup(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleSubscribe_MissingTopic(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleSubscribe(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleAnalyzeDLQ_MissingTopic(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleAnalyzeDLQ(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== Default Deny Handler Tests =====================

func TestHandleDescribeTopic_DefaultDeny_Anonymous(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleDescribeTopic(ctx, makeToolRequest(map[string]any{"topic": "test"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result) // anonymous denied
}

func TestHandleConsume_DefaultDeny_Anonymous(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleConsume(ctx, makeToolRequest(map[string]any{"topic": "test"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result) // anonymous denied
}

func TestHandleCreateTopic_DefaultDeny_Anonymous(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleCreateTopic(ctx, makeToolRequest(map[string]any{"name": "test"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleListTopics_DefaultDeny_Anonymous(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleListTopics(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result) // anonymous denied by withToolAuth
}

// ===================== New Feature Handler Tests =====================

func TestHandleDataContract_UnknownAction(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDataContract(ctx, makeToolRequest(map[string]any{"action": "bad"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleDataContract_EmptyAction(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDataContract(ctx, makeToolRequest(map[string]any{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result) // empty action falls through to default
}

func TestHandleDataContract_ValidateMissingTopic(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDataContract(ctx, makeToolRequest(map[string]any{"action": "validate"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleDataContract_ValidateMissingMessage(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDataContract(ctx, makeToolRequest(map[string]any{
		"action": "validate", "topic": "test-topic",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleDataContract_ValidateInvalidMessage(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDataContract(ctx, makeToolRequest(map[string]any{
		"action": "validate", "topic": "test-topic", "message": "not-an-object",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleDataContract_RegisterMissingContract(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDataContract(ctx, makeToolRequest(map[string]any{"action": "register"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleDataContract_GetMissingTopic(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDataContract(ctx, makeToolRequest(map[string]any{"action": "get"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleDataContract_ComplianceMissingTopic(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDataContract(ctx, makeToolRequest(map[string]any{"action": "compliance"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlePersona_GetNonExistentPersona(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handlePersona(ctx, makeToolRequest(map[string]any{"action": "get", "persona": "nonexistent"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlePersona_ActivateNonExistent(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handlePersona(ctx, makeToolRequest(map[string]any{"action": "activate", "persona": "nonexistent"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlePersona_GetMissingName(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handlePersona(ctx, makeToolRequest(map[string]any{"action": "get"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlePersona_ActivateMissingName(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handlePersona(ctx, makeToolRequest(map[string]any{"action": "activate"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlePersona_UnknownAction(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handlePersona(ctx, makeToolRequest(map[string]any{"action": "bad"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleMigrationPlan_PlanMissingSource(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleMigrationPlan(ctx, makeToolRequest(map[string]any{
		"action": "plan", "source_cluster": "src",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleMigrationPlan_ValidateNoPlan(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleMigrationPlan(ctx, makeToolRequest(map[string]any{"action": "validate"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleMigrationPlan_PlanMissingClusters(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleMigrationPlan(ctx, makeToolRequest(map[string]any{"action": "plan"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleMigrationPlan_UnknownAction(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleMigrationPlan(ctx, makeToolRequest(map[string]any{"action": "bad"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleTraceLineage_UnknownAction(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleTraceLineage(ctx, makeToolRequest(map[string]any{"action": "bad"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleTraceLineage_AnnotatePII_MissingTopic(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleTraceLineage(ctx, makeToolRequest(map[string]any{"action": "annotate_pii"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleTraceLineage_AnnotatePII_MissingField(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleTraceLineage(ctx, makeToolRequest(map[string]any{
		"action": "annotate_pii", "topic": "test",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleTraceLineage_DetectPII_MissingTopic(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleTraceLineage(ctx, makeToolRequest(map[string]any{"action": "detect_pii"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleTraceLineage_TraceMissingTopic(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleTraceLineage(ctx, makeToolRequest(map[string]any{"action": "trace"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleGroupHealth_MissingGroupID(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleGroupHealth(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleDiagnoseLag_MissingGroupID(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDiagnoseLag(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleForecast_MissingTopic(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleForecast(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== withToolAuth middleware Tests =====================

func TestWithToolAuth_RateLimitDenied(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withRateLimit)
	ctx := context.Background()

	handler := s.withToolAuth(toolAuthConfig{
		tool: "kafka_list_topics", resourceType: auth.ResourceTopic, action: auth.ActionRead,
	}, func(ctx context.Context, req mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText("ok"), nil
	})

	// Exhaust rate limit
	req := makeToolRequest(nil)
	req.Header = map[string][]string{"X-Agent-Id": {"rate-limited-agent"}}
	var denied bool
	for i := 0; i < 100; i++ {
		result, _ := handler(ctx, req)
		if result != nil && result.IsError {
			denied = true
			break
		}
	}
	if !denied {
		t.Error("expected withToolAuth to deny after rate limit exhausted")
	}
}

func TestWithToolAuth_AuthDenied(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()

	handler := s.withToolAuth(toolAuthConfig{
		tool: "kafka_list_topics", resourceType: auth.ResourceTopic, action: auth.ActionRead,
	}, func(ctx context.Context, req mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText("ok"), nil
	})

	// Anonymous should be denied
	result, err := handler(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestWithToolAuth_AllowedAgent(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()

	handler := s.withToolAuth(toolAuthConfig{
		tool: "kafka_list_topics", resourceType: auth.ResourceTopic, action: auth.ActionRead,
	}, func(ctx context.Context, req mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText("ok"), nil
	})

	req := makeToolRequest(nil)
	req.Header = map[string][]string{"X-Agent-Id": {"allowed-agent"}}
	result, err := handler(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Error("expected success for allowed agent")
	}
}

// ===================== Advanced/Feature Handler Validation Tests =====================

func TestHandleAgentRegistry_UnknownAction(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleAgentRegistry(ctx, makeToolRequest(map[string]any{"action": "bad"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleAgentRegistry_ClaimMissingFields(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleAgentRegistry(ctx, makeToolRequest(map[string]any{"action": "claim"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleAgentRegistry_ReleaseMissingFields(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleAgentRegistry(ctx, makeToolRequest(map[string]any{"action": "release"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleAgentRegistry_ShareCursorMissingFields(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleAgentRegistry(ctx, makeToolRequest(map[string]any{"action": "share_cursor"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleAgentRegistry_HandoffMissingTarget(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleAgentRegistry(ctx, makeToolRequest(map[string]any{"action": "handoff"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleWorkflows_ExecuteMissingTemplate(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleWorkflows(ctx, makeToolRequest(map[string]any{"action": "execute"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleWorkflows_UnknownAction(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleWorkflows(ctx, makeToolRequest(map[string]any{"action": "bad"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlePlatformInfo_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handlePlatformInfo(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result) // anonymous denied
}

func TestHandleAnomalies_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleAnomalies(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result) // anonymous denied
}
