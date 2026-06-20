package server

import (
	"context"
	"strings"
	"testing"
)

// ===================== Resource handler tests =====================

// Note: Most resource handlers call logAudit or getSchemaClient which requires kafkaManager.
// We only test paths that return before reaching kafkaManager-dependent code.

// ===================== Watch subscribe/unsubscribe validation tests =====================

// ===================== Stream handler additional tests =====================

// ===================== RemediateDLQ additional tests =====================

// ===================== SchemaDiff additional tests =====================

// ===================== handleNaturalLanguageQuery additional tests =====================

// ===================== handleJoin additional tests =====================

// ===================== handleSuggestSchemaEvolution additional tests =====================

// ===================== handleListConnectors additional tests =====================

// ===================== handleDescribeConnector additional tests =====================

// ===================== hasSchemaRegistry =====================

func TestHasSchemaRegistry_EmptyClients(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	if s.hasSchemaRegistry() {
		t.Error("expected hasSchemaRegistry=false with no schema clients")
	}
}

// ===================== Connector handler validation tests =====================

func TestHandleCreateConnector_NameTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleCreateConnector(ctx, makeToolRequest(map[string]any{
		"name": strings.Repeat("c", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleUpdateConnector_NameTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleUpdateConnector(ctx, makeToolRequest(map[string]any{
		"name": strings.Repeat("u", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleRestartConnector_NameTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleRestartConnector(ctx, makeToolRequest(map[string]any{
		"name": strings.Repeat("r", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleTraceMessage additional tests =====================

// ===================== handleConfigDrift additional tests =====================

// ===================== handlePlayground additional tests =====================

// ===================== handleCostWhatIf additional tests =====================

// ===================== handleWorkflows additional tests =====================

// ===================== handleKnowledgeGraphQuery additional tests =====================

// ===================== handlePlatformInfo additional tests =====================

// ===================== handleAgentRegistry additional tests =====================

// ===================== handleIngestAnomaly additional tests =====================

// ===================== handleListTopics tests =====================

func TestHandleListTopics_FilterTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleListTopics(ctx, makeToolRequest(map[string]any{
		"filter": strings.Repeat("f", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== Anomaly nil detector tests =====================

// ===================== handleDescribeConsumerGroup additional tests =====================

func TestHandleDescribeConsumerGroup_DefaultDenyAnonymous(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleDescribeConsumerGroup(ctx, makeToolRequest(map[string]any{
		"group_id": "test-group",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleDiagnoseLag additional tests =====================

func TestHandleDiagnoseLag_DefaultDenyExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleDiagnoseLag(ctx, makeToolRequest(map[string]any{
		"group_id": "my-consumer-group",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleMigrationPlan additional tests =====================

// ===================== handleGroupHealth additional tests =====================

// ===================== handleForecast additional tests =====================

// ===================== handlePersona additional tests =====================

// ===================== handleTraceLineage additional tests =====================

// ===================== handleAggregate additional tests =====================

// ===================== handleSearch additional tests =====================

func TestHandleSearch_MissingTopicExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleSearch(ctx, makeToolRequest(map[string]any{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleSearch_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleSearch(ctx, makeToolRequest(map[string]any{
		"topic": "test-topic",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleResetOffsets additional tests =====================

func TestHandleResetOffsets_MissingGroupID(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleResetOffsets(ctx, makeToolRequest(map[string]any{
		"topic": "test-topic",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleResetOffsets_MissingTopic(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleResetOffsets(ctx, makeToolRequest(map[string]any{
		"group_id": "test-group",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleSubscribe additional tests =====================

// ===================== handleAnalyzeDLQ additional tests =====================

func TestHandleAnalyzeDLQ_MissingTopicExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleAnalyzeDLQ(ctx, makeToolRequest(map[string]any{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleCheckSchemaCompatibility additional tests =====================

func TestHandleCheckSchemaCompatibility_MissingSubject(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleCheckSchemaCompatibility(ctx, makeToolRequest(map[string]any{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleCostAnalyze additional tests =====================
