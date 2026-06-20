package server

import (
	"context"
	"strings"
	"testing"
)

// ===================== tools_p1.go Handler Tests =====================

func TestHandlers_CreateTopic_NameTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleCreateTopic(ctx, makeToolRequest(map[string]any{
		"name": strings.Repeat("x", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_AlterTopic_NameTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleAlterTopic(ctx, makeToolRequest(map[string]any{
		"name": strings.Repeat("x", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_AlterTopic_NoConfigs(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleAlterTopic(ctx, makeToolRequest(map[string]any{
		"name": "test-topic",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result) // "configs is required"
}

func TestHandlers_AlterTopic_EmptyConfigs(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleAlterTopic(ctx, makeToolRequest(map[string]any{
		"name":    "test-topic",
		"configs": map[string]any{},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result) // empty configs still triggers "configs is required"
}

func TestHandlers_DeleteTopic_NameTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDeleteTopic(ctx, makeToolRequest(map[string]any{
		"name": strings.Repeat("x", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_Search_TopicTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleSearch(ctx, makeToolRequest(map[string]any{
		"topic": strings.Repeat("x", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_ResetOffsets_OnlyGroupID(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleResetOffsets(ctx, makeToolRequest(map[string]any{
		"group_id": "my-group",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_ResetOffsets_OnlyTopic(t *testing.T) {
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

func TestHandlers_ResetOffsets_MissingStrategy(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleResetOffsets(ctx, makeToolRequest(map[string]any{
		"group_id": "my-group",
		"topic":    "test-topic",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result) // strategy is missing
}

func TestHandlers_ResetOffsets_GroupIDTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleResetOffsets(ctx, makeToolRequest(map[string]any{
		"group_id": strings.Repeat("x", maxInputLength+1),
		"topic":    "test-topic",
		"strategy": "earliest",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_AnalyzeDLQ_TopicTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleAnalyzeDLQ(ctx, makeToolRequest(map[string]any{
		"topic": strings.Repeat("x", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_CheckSchemaCompat_MissingBoth(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleCheckSchemaCompatibility(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_CheckSchemaCompat_MissingSchema(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleCheckSchemaCompatibility(ctx, makeToolRequest(map[string]any{
		"subject": "test-subject",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_CheckSchemaCompat_SchemaTooLarge(t *testing.T) {
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

// ===================== Kafka Connect Handler Tests =====================

func TestHandlers_CreateConnector_MissingName(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleCreateConnector(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_UpdateConnector_MissingName(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleUpdateConnector(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_DeleteConnector_MissingName(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDeleteConnector(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_RestartConnector_MissingName(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleRestartConnector(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== Diagnosis Handler Tests =====================

func TestHandlers_DiagnoseLag_MissingGroupID(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDiagnoseLag(ctx, makeToolRequest(map[string]any{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== tools_p0.go Handler Tests =====================

func TestHandlers_Produce_MissingTopic(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleProduce(ctx, makeToolRequest(map[string]any{
		"messages": []any{map[string]any{"value": "test"}},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_Produce_TopicTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleProduce(ctx, makeToolRequest(map[string]any{
		"topic":    strings.Repeat("x", maxInputLength+1),
		"messages": []any{map[string]any{"value": "test"}},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_Produce_MissingMessages(t *testing.T) {
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

func TestHandlers_Produce_InvalidMessagesFormat(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleProduce(ctx, makeToolRequest(map[string]any{
		"topic":    "test-topic",
		"messages": "not-a-list",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_Produce_EmptyMessages(t *testing.T) {
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

func TestHandlers_Produce_NilArguments(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleProduce(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_GetSchema_MissingSubject(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleGetSchema(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_GetSchema_SubjectTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleGetSchema(ctx, makeToolRequest(map[string]any{
		"subject": strings.Repeat("x", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_DescribeTopic_TopicTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDescribeTopic(ctx, makeToolRequest(map[string]any{
		"topic": strings.Repeat("x", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_Consume_MissingTopic(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleConsume(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_Consume_TopicTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleConsume(ctx, makeToolRequest(map[string]any{
		"topic": strings.Repeat("x", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_DescribeConsumerGroup_GroupIDTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDescribeConsumerGroup(ctx, makeToolRequest(map[string]any{
		"group_id": strings.Repeat("x", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}
