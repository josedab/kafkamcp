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

func TestHandleWatch_UnsubscribeNilWatchManager(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	s.watchManager = nil
	ctx := context.Background()
	result, err := s.handleWatch(ctx, makeToolRequest(map[string]any{
		"action":          "unsubscribe",
		"subscription_id": "some-id",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result) // "watch manager not initialized"
}

func TestHandleWatch_ListNilWatchManager(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	s.watchManager = nil
	ctx := context.Background()
	result, err := s.handleWatch(ctx, makeToolRequest(map[string]any{
		"action": "list",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result) // "watch manager not initialized"
}

func TestHandleWatch_SubscribeNilWatchManager(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	s.watchManager = nil
	ctx := context.Background()
	result, err := s.handleWatch(ctx, makeToolRequest(map[string]any{
		"action":        "subscribe",
		"topic_pattern": "orders.*",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result) // "watch manager not initialized"
}

func TestHandleWatch_SubscribeDefaultDenyExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleWatch(ctx, makeToolRequest(map[string]any{
		"action":        "subscribe",
		"topic_pattern": "orders.*",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== Stream handler additional tests =====================

func TestHandleStream_StartGroupIDTooLongExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleStream(ctx, makeToolRequest(map[string]any{
		"action":   "start",
		"topic":    "test-topic",
		"group_id": strings.Repeat("g", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleStream_ResumeMissingSessionID(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleStream(ctx, makeToolRequest(map[string]any{
		"action": "resume",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleStream_StopMissingSessionID(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleStream(ctx, makeToolRequest(map[string]any{
		"action": "stop",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== RemediateDLQ additional tests =====================

func TestHandleRemediateDLQ_DefaultDeny_AnonymousExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleRemediateDLQ(ctx, makeToolRequest(map[string]any{
		"topic": "orders-dlq",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleRemediateDLQ_NilRemediator(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	s.dlqRemediator = nil
	ctx := context.Background()
	result, err := s.handleRemediateDLQ(ctx, makeToolRequest(map[string]any{
		"topic":  "orders-dlq",
		"action": "analyze",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result) // "DLQ remediator not initialized"
}

// ===================== SchemaDiff additional tests =====================

func TestHandleSchemaDiff_DefaultDenyExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleSchemaDiff(ctx, makeToolRequest(map[string]any{
		"subject":      "orders-value",
		"version_from": "1",
		"version_to":   "2",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleNaturalLanguageQuery additional tests =====================

func TestHandleNLQuery_QueryTooLongExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleNLQuery(ctx, makeToolRequest(map[string]any{
		"query": strings.Repeat("q", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleNLQuery_DefaultDenyExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleNLQuery(ctx, makeToolRequest(map[string]any{
		"query": "show me recent orders",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleJoin additional tests =====================

func TestHandleJoin_LeftTopicTooLongExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleJoin(ctx, makeToolRequest(map[string]any{
		"left_topic":  strings.Repeat("l", maxInputLength+1),
		"right_topic": "payments",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleJoin_RightTopicTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleJoin(ctx, makeToolRequest(map[string]any{
		"left_topic":  "orders",
		"right_topic": strings.Repeat("r", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleJoin_DefaultDenyExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleJoin(ctx, makeToolRequest(map[string]any{
		"left_topic":  "orders",
		"right_topic": "payments",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleJoin_KeyFieldTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleJoin(ctx, makeToolRequest(map[string]any{
		"left_topic":  "orders",
		"right_topic": "payments",
		"key_field":   strings.Repeat("k", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleSuggestSchemaEvolution additional tests =====================

func TestHandleSuggestSchemaEvolution_ChangeTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleSuggestSchemaEvolution(ctx, makeToolRequest(map[string]any{
		"subject": "orders-value",
		"change":  strings.Repeat("c", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleListConnectors additional tests =====================

func TestHandleListConnectors_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleListConnectors(ctx, makeToolRequest(map[string]any{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleDescribeConnector additional tests =====================

func TestHandleDescribeConnector_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleDescribeConnector(ctx, makeToolRequest(map[string]any{
		"name": "my-connector",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleDescribeConnector_NameTooLongExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDescribeConnector(ctx, makeToolRequest(map[string]any{
		"name": strings.Repeat("n", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

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

func TestHandleTraceMessage_CorrelationIDTooLongExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleTraceMessage(ctx, makeToolRequest(map[string]any{
		"correlation_id": strings.Repeat("t", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleConfigDrift additional tests =====================

func TestHandleConfigDrift_AuditDefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleConfigDrift(ctx, makeToolRequest(map[string]any{
		"action":  "audit",
		"cluster": "test-cluster",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handlePlayground additional tests =====================

func TestHandlePlayground_CreateDefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handlePlayground(ctx, makeToolRequest(map[string]any{
		"action": "create",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleCostWhatIf additional tests =====================

func TestHandleCostWhatIf_MissingTopicExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleCostWhatIf(ctx, makeToolRequest(map[string]any{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleWorkflows additional tests =====================

func TestHandleWorkflows_ListDefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleWorkflows(ctx, makeToolRequest(map[string]any{
		"action": "list",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleKnowledgeGraphQuery additional tests =====================

func TestHandleKnowledgeGraph_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleKnowledgeGraphQuery(ctx, makeToolRequest(map[string]any{
		"query": "show topology",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handlePlatformInfo additional tests =====================

func TestHandlePlatformInfo_DefaultDenyExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handlePlatformInfo(ctx, makeToolRequest(map[string]any{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleAgentRegistry additional tests =====================

func TestHandleAgentRegistry_NilRegistry(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	s.agentRegistry = nil
	ctx := context.Background()
	result, err := s.handleAgentRegistry(ctx, makeToolRequest(map[string]any{
		"action": "register",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result) // "agent registry not initialized"
}

// ===================== handleIngestAnomaly additional tests =====================

func TestHandleIngestAnomaly_NilDetector(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	s.anomalyDetector = nil
	ctx := context.Background()
	result, err := s.handleIngestAnomaly(ctx, makeToolRequest(map[string]any{
		"metric": "cpu_usage",
		"value":  85.5,
		"source": "broker-1",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result) // "Anomaly detection is not configured"
}

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

func TestHandleAnomalies_NilDetector(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	s.anomalyDetector = nil
	ctx := context.Background()
	result, err := s.handleAnomalies(ctx, makeToolRequest(map[string]any{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result) // "Anomaly detection is not configured"
}

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

func TestHandleMigrationPlan_ValidateNoPlans(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleMigrationPlan(ctx, makeToolRequest(map[string]any{
		"action": "validate",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result) // "no migration plans exist"
}

// ===================== handleGroupHealth additional tests =====================

func TestHandleGroupHealth_MissingGroupIDExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleGroupHealth(ctx, makeToolRequest(map[string]any{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleGroupHealth_DefaultDenyExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleGroupHealth(ctx, makeToolRequest(map[string]any{
		"group_id": "test-group",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleForecast additional tests =====================

func TestHandleForecast_DefaultDenyExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleForecast(ctx, makeToolRequest(map[string]any{
		"topic": "test-topic",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleForecast_MissingTopicExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleForecast(ctx, makeToolRequest(map[string]any{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handlePersona additional tests =====================

func TestHandlePersona_EmptyActionExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handlePersona(ctx, makeToolRequest(map[string]any{
		"action": "",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result) // unknown action
}

// ===================== handleTraceLineage additional tests =====================

func TestHandleTraceLineage_EmptyActionExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleTraceLineage(ctx, makeToolRequest(map[string]any{
		"action": "invalid",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleAggregate additional tests =====================

func TestHandleAggregate_FieldTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleAggregate(ctx, makeToolRequest(map[string]any{
		"topic": "test-topic",
		"field": strings.Repeat("f", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleAggregate_FunctionTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleAggregate(ctx, makeToolRequest(map[string]any{
		"topic":    "test-topic",
		"function": strings.Repeat("f", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

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

func TestHandleSubscribe_MissingTopicExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleSubscribe(ctx, makeToolRequest(map[string]any{
		"group_id": "test-group",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleSubscribe_DefaultDenyExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleSubscribe(ctx, makeToolRequest(map[string]any{
		"topic":    "test-topic",
		"group_id": "test-group",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

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

func TestHandleCostAnalyze_DefaultDenyExtended(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleCostAnalyze(ctx, makeToolRequest(map[string]any{
		"topic": "test-topic",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}
