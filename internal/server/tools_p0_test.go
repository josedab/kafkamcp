package server

import (
	"context"
	"strings"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/config"
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

func TestHandleAggregate_MissingTopic(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleAggregate(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleAggregate_TopicTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleAggregate(ctx, makeToolRequest(map[string]any{
		"topic": strings.Repeat("a", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleAggregate_DefaultDeny_Anonymous(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleAggregate(ctx, makeToolRequest(map[string]any{
		"topic": "test-topic",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleDescribeConnector =====================

func TestHandleDescribeConnector_MissingName(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDescribeConnector(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleDescribeConnector_NameTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDescribeConnector(ctx, makeToolRequest(map[string]any{
		"name": strings.Repeat("c", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleDescribeConnector_DefaultDeny_Anonymous(t *testing.T) {
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

// ===================== handleListConnectors =====================

func TestHandleListConnectors_DefaultDeny_Anonymous(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleListConnectors(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleRecommend =====================

func TestHandleRecommend_DefaultDeny_Anonymous(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleRecommend(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleListACLs =====================

func TestHandleListACLs_DefaultDeny_Anonymous(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleListACLs(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleTraceMessage =====================

func TestHandleTraceMessage_MissingCorrelationID(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleTraceMessage(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleTraceMessage_CorrelationIDTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleTraceMessage(ctx, makeToolRequest(map[string]any{
		"correlation_id": strings.Repeat("c", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleTraceMessage_DefaultDeny_Anonymous(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleTraceMessage(ctx, makeToolRequest(map[string]any{
		"correlation_id": "abc-123",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleStream =====================

func TestHandleStream_UnknownAction(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleStream(ctx, makeToolRequest(map[string]any{
		"action": "invalid",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleStream_StartMissingTopic(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleStream(ctx, makeToolRequest(map[string]any{
		"action": "start",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleStream_StartTopicTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleStream(ctx, makeToolRequest(map[string]any{
		"action": "start",
		"topic":  strings.Repeat("t", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleStream_StartDefaultDeny_Anonymous(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleStream(ctx, makeToolRequest(map[string]any{
		"action": "start",
		"topic":  "test-topic",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleStream_PollMissingSessionID(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleStream(ctx, makeToolRequest(map[string]any{
		"action": "poll",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleStream_ControlMissingSessionID(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleStream(ctx, makeToolRequest(map[string]any{
		"action": "pause",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

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

func TestHandleConnectorHealth_MissingName(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleConnectorHealth(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleConnectorHealth_NameTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleConnectorHealth(ctx, makeToolRequest(map[string]any{
		"name": strings.Repeat("h", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleConnectorHealth_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleConnectorHealth(ctx, makeToolRequest(map[string]any{
		"name": "my-connector",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleSchemaDiff =====================

func TestHandleSchemaDiff_MissingSubject(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleSchemaDiff(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleSchemaDiff_SubjectTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleSchemaDiff(ctx, makeToolRequest(map[string]any{
		"subject": strings.Repeat("s", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleSchemaDiff_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleSchemaDiff(ctx, makeToolRequest(map[string]any{
		"subject": "test-subject",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleWatch =====================

func TestHandleWatch_UnknownAction(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleWatch(ctx, makeToolRequest(map[string]any{
		"action": "invalid",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleWatch_SubscribeMissingPattern(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleWatch(ctx, makeToolRequest(map[string]any{
		"action": "subscribe",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleWatch_SubscribePatternTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleWatch(ctx, makeToolRequest(map[string]any{
		"action":        "subscribe",
		"topic_pattern": strings.Repeat("w", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleWatch_SubscribeDefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleWatch(ctx, makeToolRequest(map[string]any{
		"action":        "subscribe",
		"topic_pattern": "test-*",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleWatch_UnsubscribeMissingID(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleWatch(ctx, makeToolRequest(map[string]any{
		"action": "unsubscribe",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleNLQuery =====================

func TestHandleNLQuery_MissingQuery(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleNLQuery(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleNLQuery_QueryTooLong(t *testing.T) {
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

func TestHandleNLQuery_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleNLQuery(ctx, makeToolRequest(map[string]any{
		"query": "show all topics",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleGroupHealth =====================

func TestHandleGroupHealth_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleGroupHealth(ctx, makeToolRequest(map[string]any{
		"group_id": "my-group",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

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

func TestHandleCostWhatIf_MissingTopic(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleCostWhatIf(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleCostWhatIf_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleCostWhatIf(ctx, makeToolRequest(map[string]any{
		"topic": "test-topic",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleIngestAnomaly =====================

func TestHandleIngestAnomaly_MissingFields(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleIngestAnomaly(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleIngestAnomaly_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleIngestAnomaly(ctx, makeToolRequest(map[string]any{
		"metric": "throughput",
		"source": "topic-a",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleRemediateDLQ =====================

func TestHandleRemediateDLQ_MissingTopic(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleRemediateDLQ(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handlePlatformInfo =====================

func TestHandlePlatformInfo_DefaultDeny_Anon(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handlePlatformInfo(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleCostWaste =====================

func TestHandleCostWaste_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleCostWaste(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleGitOpsDiff =====================

func TestHandleGitOpsDiff_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleGitOpsDiff(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleKnowledgeGraphQuery =====================

func TestHandleKnowledgeGraphQuery_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleKnowledgeGraphQuery(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleCostAnalyze =====================

func TestHandleCostAnalyze_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleCostAnalyze(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleSubscribe =====================

func TestHandleSubscribe_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleSubscribe(ctx, makeToolRequest(map[string]any{
		"topic": "test-topic",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

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

func TestHandleTimeTravel_MissingTopic(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleTimeTravel(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleTimeTravel_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleTimeTravel(ctx, makeToolRequest(map[string]any{
		"topic": "test-topic",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleConfigDrift =====================

func TestHandleConfigDrift_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleConfigDrift(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handlePlayground =====================

func TestHandlePlayground_DefaultDeny(t *testing.T) {
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

// ===================== handleForecast =====================

func TestHandleForecast_DefaultDeny(t *testing.T) {
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

// ===================== handleSubscribe =====================

func TestHandleSubscribe_TopicTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleSubscribe(ctx, makeToolRequest(map[string]any{
		"topic": strings.Repeat("s", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleRemediateDLQ =====================

func TestHandleRemediateDLQ_TopicTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleRemediateDLQ(ctx, makeToolRequest(map[string]any{
		"topic": strings.Repeat("r", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleRemediateDLQ_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleRemediateDLQ(ctx, makeToolRequest(map[string]any{
		"topic": "dlq-topic",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleConfigDrift compare missing clusters =====================

func TestHandleConfigDrift_CompareMissingClusters(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleConfigDrift(ctx, makeToolRequest(map[string]any{
		"action": "compare",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleConfigDrift_UnknownAction(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleConfigDrift(ctx, makeToolRequest(map[string]any{
		"action": "invalid",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handlePlayground unknown action =====================

func TestHandlePlayground_UnknownAction(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handlePlayground(ctx, makeToolRequest(map[string]any{
		"action": "invalid",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleCostAnalyze missing topic =====================

func TestHandleCostAnalyze_MissingTopic(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleCostAnalyze(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleMigrationPlan =====================
// handleMigrationPlan doesn't have checkAuth so default deny doesn't help.

// ===================== handleJoin =====================

func TestHandleJoin_MissingTopics(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleJoin(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleJoin_LeftTopicTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleJoin(ctx, makeToolRequest(map[string]any{
		"left_topic":  strings.Repeat("l", maxInputLength+1),
		"right_topic": "right",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleJoin_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleJoin(ctx, makeToolRequest(map[string]any{
		"left_topic":  "left",
		"right_topic": "right",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleNaturalLanguageQuery =====================

func TestHandleNaturalLanguageQuery_MissingQuery(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleNaturalLanguageQuery(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleNaturalLanguageQuery_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleNaturalLanguageQuery(ctx, makeToolRequest(map[string]any{
		"query": "show all topics",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleSuggestSchemaEvolution =====================

func TestHandleSuggestSchemaEvolution_MissingFields(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleSuggestSchemaEvolution(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleSuggestSchemaEvolution_SubjectTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleSuggestSchemaEvolution(ctx, makeToolRequest(map[string]any{
		"subject": strings.Repeat("s", maxInputLength+1),
		"change":  "add field",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleSuggestSchemaEvolution_DefaultDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	ctx := context.Background()
	result, err := s.handleSuggestSchemaEvolution(ctx, makeToolRequest(map[string]any{
		"subject": "test-subject",
		"change":  "add field",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

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

func TestHandleStream_StartGroupIDTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleStream(ctx, makeToolRequest(map[string]any{
		"action":   "start",
		"topic":    "test",
		"group_id": strings.Repeat("g", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

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
