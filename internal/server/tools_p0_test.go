package server

import (
	"context"
	"strings"
	"testing"

	"github.com/josedab/kafkamcp/internal/config"
)

// ===================== handleProduce =====================

func TestHandleProduce_MissingTopic(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleProduce(ctx, makeToolRequest(map[string]any{
		"messages": []any{map[string]any{"value": "hello"}},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleProduce_MissingMessages(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleProduce(ctx, makeToolRequest(map[string]any{
		"topic": "test-topic",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleProduce_InvalidMessagesFormat(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleProduce(ctx, makeToolRequest(map[string]any{
		"topic":    "test-topic",
		"messages": "not-an-array",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleProduce_EmptyMessages(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleProduce(ctx, makeToolRequest(map[string]any{
		"topic":    "test-topic",
		"messages": []any{},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleProduce_TooManyMessages(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	msgs := make([]any, MaxProduceMessages+1)
	for i := range msgs {
		msgs[i] = map[string]any{"value": "msg"}
	}
	result, err := s.handleProduce(ctx, makeToolRequest(map[string]any{
		"topic":    "test-topic",
		"messages": msgs,
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleProduce_TopicTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	longTopic := strings.Repeat("t", maxInputLength+1)
	result, err := s.handleProduce(ctx, makeToolRequest(map[string]any{
		"topic":    longTopic,
		"messages": []any{map[string]any{"value": "hello"}},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleProduce_DefaultDeny_Anonymous(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleProduce(ctx, makeToolRequest(map[string]any{
		"topic":    "test-topic",
		"messages": []any{map[string]any{"value": "hello"}},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleProduce_MessageSizeExceeded(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, func(cfg *config.Config) {
		cfg.Server.MaxMessageSize = 10 // 10 bytes max
	})
	ctx := context.Background()
	result, err := s.handleProduce(ctx, makeToolRequest(map[string]any{
		"topic":    "test-topic",
		"messages": []any{map[string]any{"value": strings.Repeat("x", 100)}},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleListConsumerGroups =====================

func TestHandleListConsumerGroups_DefaultDeny_Anonymous(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleListConsumerGroups(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleListConsumerGroups_StateFilterTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	longFilter := strings.Repeat("s", maxInputLength+1)
	result, err := s.handleListConsumerGroups(ctx, makeToolRequest(map[string]any{
		"state_filter": longFilter,
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleListConsumerGroups_RateLimit(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withRateLimit)
	ctx := context.Background()
	req := makeToolRequest(nil)
	req.Header = map[string][]string{"X-Agent-Id": {"rate-limited-agent"}}

	var denied bool
	for i := 0; i < 100; i++ {
		result, _ := s.handleListConsumerGroups(ctx, req)
		if result != nil && result.IsError {
			denied = true
			break
		}
	}
	if !denied {
		t.Error("expected rate limit to be hit")
	}
}

// ===================== handleListSchemas =====================

func TestHandleListSchemas_DefaultDeny_Anonymous(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleListSchemas(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleListSchemas_FilterTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	longFilter := strings.Repeat("f", maxInputLength+1)
	result, err := s.handleListSchemas(ctx, makeToolRequest(map[string]any{
		"filter": longFilter,
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleListSchemas_RateLimit(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withRateLimit)
	ctx := context.Background()
	req := makeToolRequest(nil)
	req.Header = map[string][]string{"X-Agent-Id": {"rate-limited-agent"}}

	var denied bool
	for i := 0; i < 100; i++ {
		result, _ := s.handleListSchemas(ctx, req)
		if result != nil && result.IsError {
			denied = true
			break
		}
	}
	if !denied {
		t.Error("expected rate limit to be hit")
	}
}

// ===================== handleGetSchema =====================

func TestHandleGetSchema_MissingSubject(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleGetSchema(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleGetSchema_SubjectTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleGetSchema(ctx, makeToolRequest(map[string]any{
		"subject": strings.Repeat("s", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleGetSchema_DefaultDeny_Anonymous(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleGetSchema(ctx, makeToolRequest(map[string]any{
		"subject": "test-subject",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleAlterTopic =====================

func TestHandleAlterTopic_MissingConfigs(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleAlterTopic(ctx, makeToolRequest(map[string]any{
		"name": "test-topic",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleAlterTopic_DefaultDeny_Anonymous(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleAlterTopic(ctx, makeToolRequest(map[string]any{
		"name": "test-topic", "configs": map[string]any{"retention.ms": "86400000"},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleAlterTopic_NameTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleAlterTopic(ctx, makeToolRequest(map[string]any{
		"name": strings.Repeat("n", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleDeleteTopic =====================

func TestHandleDeleteTopic_DefaultDeny_Anonymous(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleDeleteTopic(ctx, makeToolRequest(map[string]any{
		"name": "test-topic",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleDeleteTopic_NameTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDeleteTopic(ctx, makeToolRequest(map[string]any{
		"name": strings.Repeat("d", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleSearch =====================

func TestHandleSearch_DefaultDeny_Anonymous(t *testing.T) {
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

func TestHandleSearch_TopicTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleSearch(ctx, makeToolRequest(map[string]any{
		"topic": strings.Repeat("s", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleSearch_KeyFilterTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleSearch(ctx, makeToolRequest(map[string]any{
		"topic":      "test-topic",
		"key_filter": strings.Repeat("k", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleResetOffsets =====================

func TestHandleResetOffsets_GroupIDTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleResetOffsets(ctx, makeToolRequest(map[string]any{
		"group_id": strings.Repeat("g", maxInputLength+1),
		"topic":    "test-topic",
		"strategy": "earliest",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleResetOffsets_DefaultDeny_Anonymous(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleResetOffsets(ctx, makeToolRequest(map[string]any{
		"group_id": "my-group",
		"topic":    "test-topic",
		"strategy": "earliest",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleCheckSchemaCompatibility =====================

func TestHandleCheckSchemaCompatibility_MissingFields(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleCheckSchemaCompatibility(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleCheckSchemaCompatibility_SubjectTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleCheckSchemaCompatibility(ctx, makeToolRequest(map[string]any{
		"subject": strings.Repeat("c", maxInputLength+1),
		"schema":  `{"type":"record"}`,
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleCheckSchemaCompatibility_SchemaTooLarge(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleCheckSchemaCompatibility(ctx, makeToolRequest(map[string]any{
		"subject": "test-subject",
		"schema":  strings.Repeat("x", MaxSchemaSize+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleCheckSchemaCompatibility_DefaultDeny_Anonymous(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleCheckSchemaCompatibility(ctx, makeToolRequest(map[string]any{
		"subject": "test-subject",
		"schema":  `{"type":"record"}`,
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleAggregate =====================

// ===================== handleDescribeConnector =====================

// ===================== handleListConnectors =====================

// ===================== handleRecommend =====================

// ===================== handleListACLs =====================

// ===================== handleTraceMessage =====================

// ===================== handleStream =====================

// ===================== connectTLSOpts =====================

func TestConnectTLSOpts_Empty(t *testing.T) {
	t.Parallel()
	cfg := &config.ClusterConfig{}
	opts := connectTLSOpts(cfg)
	if len(opts) != 0 {
		t.Errorf("expected 0 opts, got %d", len(opts))
	}
}

func TestConnectTLSOpts_WithCA(t *testing.T) {
	t.Parallel()
	cfg := &config.ClusterConfig{ConnectTLS: config.ConnectTLSConfig{CAFile: "/tmp/ca.pem"}}
	opts := connectTLSOpts(cfg)
	if len(opts) != 1 {
		t.Errorf("expected 1 opt for CA, got %d", len(opts))
	}
}

func TestConnectTLSOpts_WithInsecureSkipVerify(t *testing.T) {
	t.Parallel()
	cfg := &config.ClusterConfig{ConnectTLS: config.ConnectTLSConfig{InsecureSkipVerify: true}}
	opts := connectTLSOpts(cfg)
	if len(opts) != 1 {
		t.Errorf("expected 1 opt for insecure, got %d", len(opts))
	}
}

func TestConnectTLSOpts_WithBoth(t *testing.T) {
	t.Parallel()
	cfg := &config.ClusterConfig{ConnectTLS: config.ConnectTLSConfig{CAFile: "/tmp/ca.pem", InsecureSkipVerify: true}}
	opts := connectTLSOpts(cfg)
	if len(opts) != 2 {
		t.Errorf("expected 2 opts, got %d", len(opts))
	}
}

// ===================== handleDeleteConnector =====================

func TestHandleDeleteConnector_MissingName(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDeleteConnector(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleDeleteConnector_NameTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDeleteConnector(ctx, makeToolRequest(map[string]any{
		"name": strings.Repeat("c", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleDeleteConnector_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleDeleteConnector(ctx, makeToolRequest(map[string]any{
		"name": "my-connector",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleConnectorHealth =====================

// ===================== handleSchemaDiff =====================

// ===================== handleWatch =====================

// ===================== handleNLQuery =====================

// ===================== handleGroupHealth =====================

// ===================== handleDiagnoseLag =====================

func TestHandleDiagnoseLag_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleDiagnoseLag(ctx, makeToolRequest(map[string]any{
		"group_id": "my-group",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleCostWhatIf =====================

// ===================== handleIngestAnomaly =====================

// ===================== handleRemediateDLQ =====================

// ===================== handlePlatformInfo =====================

// ===================== handleCostWaste =====================

// ===================== handleGitOpsDiff =====================

// ===================== handleKnowledgeGraphQuery =====================

// ===================== handleCostAnalyze =====================

// ===================== handleSubscribe =====================

// ===================== handleDescribeTopic =====================

func TestHandleDescribeTopic_TopicTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDescribeTopic(ctx, makeToolRequest(map[string]any{
		"topic": strings.Repeat("t", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleConsume =====================

func TestHandleConsume_TopicTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleConsume(ctx, makeToolRequest(map[string]any{
		"topic": strings.Repeat("t", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleAnalyzeDLQ =====================

func TestHandleAnalyzeDLQ_TopicTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleAnalyzeDLQ(ctx, makeToolRequest(map[string]any{
		"topic": strings.Repeat("d", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleAnalyzeDLQ_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleAnalyzeDLQ(ctx, makeToolRequest(map[string]any{
		"topic": "dlq-topic",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleDescribeConsumerGroup =====================

func TestHandleDescribeConsumerGroup_GroupIDTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDescribeConsumerGroup(ctx, makeToolRequest(map[string]any{
		"group_id": strings.Repeat("g", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleCreateConnector =====================

func TestHandleCreateConnector_MissingName(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleCreateConnector(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleCreateConnector_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleCreateConnector(ctx, makeToolRequest(map[string]any{
		"name": "my-conn",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleUpdateConnector =====================

func TestHandleUpdateConnector_MissingName(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleUpdateConnector(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleUpdateConnector_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleUpdateConnector(ctx, makeToolRequest(map[string]any{
		"name": "my-conn",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleRestartConnector =====================

func TestHandleRestartConnector_MissingName(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleRestartConnector(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleRestartConnector_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleRestartConnector(ctx, makeToolRequest(map[string]any{
		"name": "my-conn",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleTimeTravel =====================

// ===================== handleConfigDrift =====================

// ===================== handlePlayground =====================

// ===================== handleForecast =====================

// ===================== handleSubscribe =====================

// ===================== handleRemediateDLQ =====================

// ===================== handleConfigDrift compare missing clusters =====================

// ===================== handlePlayground unknown action =====================

// ===================== handleCostAnalyze missing topic =====================

// ===================== handleMigrationPlan =====================
// handleMigrationPlan doesn't have checkAuth so default deny doesn't help.

// ===================== handleJoin =====================

// ===================== handleNaturalLanguageQuery =====================

// ===================== handleSuggestSchemaEvolution =====================

// ===================== handleConsume =====================

func TestHandleConsume_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleConsume(ctx, makeToolRequest(map[string]any{
		"topic": "test-topic",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleDescribeTopic =====================

func TestHandleDescribeTopic_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleDescribeTopic(ctx, makeToolRequest(map[string]any{
		"topic": "test-topic",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleCreateTopic =====================

func TestHandleCreateTopic_NameTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleCreateTopic(ctx, makeToolRequest(map[string]any{
		"name": strings.Repeat("n", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// handleSchemaDiff version_from check is after getSchemaClient which panics with nil kafkaManager

// handleStream rate limit exhaustion test removed - first request passes through and hits kafkaManager

// ===================== handleStream start with groupId too long =====================

// ===================== handleConsume offset too long =====================

func TestHandleConsume_OffsetTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleConsume(ctx, makeToolRequest(map[string]any{
		"topic":  "test-topic",
		"offset": strings.Repeat("o", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleAgentRegistry more coverage =====================

// handleAgentRegistry all actions call logAudit which needs kafkaManager

// ===================== handleTraceLineage =====================
// annotate_pii, detect_pii, compliance_report don't use kafkaManager
// but they call logAudit which panics with nil kafkaManager.
// Existing tests in server_integration_test.go cover validation paths.
