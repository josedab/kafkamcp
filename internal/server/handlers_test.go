package server

import (
	"context"
	"strings"
	"testing"
)

// ===================== tools_p1.go Handler Tests =====================

func TestHandlers_CreateTopic_NameTooLong(t *testing.T) {
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

func TestHandlers_Subscribe_TopicTooLong(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleSubscribe(ctx, makeToolRequest(map[string]any{
		"topic": strings.Repeat("x", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_CheckSchemaCompat_MissingBoth(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleCheckSchemaCompatibility(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_CheckSchemaCompat_MissingSchema(t *testing.T) {
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

// ===================== tools_p2.go Handler Tests =====================

func TestHandlers_Aggregate_MissingTopic(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleAggregate(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_Aggregate_TopicTooLong(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleAggregate(ctx, makeToolRequest(map[string]any{
		"topic": strings.Repeat("x", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_SchemaEvolution_MissingBoth(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleSuggestSchemaEvolution(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_SchemaEvolution_MissingChange(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleSuggestSchemaEvolution(ctx, makeToolRequest(map[string]any{
		"subject": "test-subject",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_SchemaEvolution_MissingSubject(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleSuggestSchemaEvolution(ctx, makeToolRequest(map[string]any{
		"change": "add email field",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_Join_MissingBothTopics(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleJoin(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_Join_MissingRightTopic(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleJoin(ctx, makeToolRequest(map[string]any{
		"left_topic": "topic-a",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_Join_MissingLeftTopic(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleJoin(ctx, makeToolRequest(map[string]any{
		"right_topic": "topic-b",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_NaturalLanguageQuery_MissingQuery(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleNaturalLanguageQuery(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_NaturalLanguageQuery_QueryTooLong(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleNaturalLanguageQuery(ctx, makeToolRequest(map[string]any{
		"query": strings.Repeat("x", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_DescribeConnector_MissingName(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDescribeConnector(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_CreateConnector_MissingName(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleCreateConnector(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_UpdateConnector_MissingName(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleUpdateConnector(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_DeleteConnector_MissingName(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDeleteConnector(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_RestartConnector_MissingName(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleRestartConnector(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_ConnectorHealth_MissingName(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleConnectorHealth(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== tools_advanced.go Handler Tests =====================

func TestHandlers_AgentRegistry_HandoffMissingTarget(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleAgentRegistry(ctx, makeToolRequest(map[string]any{
		"action": "handoff",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_Playground_InvalidAction(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handlePlayground(ctx, makeToolRequest(map[string]any{
		"action": "invalid_action",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_Playground_EmptyAction(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handlePlayground(ctx, makeToolRequest(map[string]any{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_TimeTravel_MissingTopic(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleTimeTravel(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_TimeTravel_TopicTooLong(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleTimeTravel(ctx, makeToolRequest(map[string]any{
		"topic": strings.Repeat("x", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_TimeTravel_SnapshotMissingTimestamp(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleTimeTravel(ctx, makeToolRequest(map[string]any{
		"topic":  "test-topic",
		"action": "snapshot",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_TimeTravel_DiffMissingTimestamps(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleTimeTravel(ctx, makeToolRequest(map[string]any{
		"topic":  "test-topic",
		"action": "diff",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_TimeTravel_DiffMissingTimestampTo(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleTimeTravel(ctx, makeToolRequest(map[string]any{
		"topic":          "test-topic",
		"action":         "diff",
		"timestamp_from": "2024-01-01T00:00:00Z",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_TimeTravel_InvalidAction(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleTimeTravel(ctx, makeToolRequest(map[string]any{
		"topic":  "test-topic",
		"action": "invalid",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_TraceMessage_MissingCorrelationID(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleTraceMessage(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_TraceMessage_CorrelationIDTooLong(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleTraceMessage(ctx, makeToolRequest(map[string]any{
		"correlation_id": strings.Repeat("x", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_ConfigDrift_CompareMissingClusters(t *testing.T) {
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

func TestHandlers_ConfigDrift_CompareMissingTarget(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleConfigDrift(ctx, makeToolRequest(map[string]any{
		"action":         "compare",
		"source_cluster": "cluster-a",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_ConfigDrift_InvalidAction(t *testing.T) {
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

// ===================== tools_streaming.go Handler Tests =====================

func TestHandlers_Stream_InvalidAction(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleStream(ctx, makeToolRequest(map[string]any{
		"action": "bad_action",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_Stream_StartMissingTopic(t *testing.T) {
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

func TestHandlers_Stream_StartTopicTooLong(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleStream(ctx, makeToolRequest(map[string]any{
		"action": "start",
		"topic":  strings.Repeat("x", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_Stream_PollMissingSessionID(t *testing.T) {
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

func TestHandlers_Stream_PauseMissingSessionID(t *testing.T) {
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

func TestHandlers_Stream_ResumeMissingSessionID(t *testing.T) {
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

func TestHandlers_Stream_StopMissingSessionID(t *testing.T) {
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

// ===================== tools_new_features.go Handler Tests =====================

func TestHandlers_DataContract_GetNonExistent(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDataContract(ctx, makeToolRequest(map[string]any{
		"action": "get",
		"topic":  "nonexistent-topic",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_DataContract_ComplianceNonExistent(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDataContract(ctx, makeToolRequest(map[string]any{
		"action": "compliance",
		"topic":  "nonexistent-topic",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_DataContract_RegisterInvalidContract(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleDataContract(ctx, makeToolRequest(map[string]any{
		"action":   "register",
		"contract": "not-a-json-object",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_Persona_EmptyAction(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handlePersona(ctx, makeToolRequest(map[string]any{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_MigrationPlan_EmptyAction(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleMigrationPlan(ctx, makeToolRequest(map[string]any{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_TraceLineage_EmptyAction(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleTraceLineage(ctx, makeToolRequest(map[string]any{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_GroupHealth_MissingGroupID(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleGroupHealth(ctx, makeToolRequest(map[string]any{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_Forecast_MissingTopic(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleForecast(ctx, makeToolRequest(map[string]any{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_DiagnoseLag_MissingGroupID(t *testing.T) {
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
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleProduce(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_GetSchema_MissingSubject(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleGetSchema(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_GetSchema_SubjectTooLong(t *testing.T) {
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
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleConsume(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_Consume_TopicTooLong(t *testing.T) {
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

// ===================== tools_p3.go Handler Tests =====================

func TestHandlers_Watch_InvalidAction(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleWatch(ctx, makeToolRequest(map[string]any{
		"action": "bad_action",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_Watch_SubscribeMissingPattern(t *testing.T) {
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

func TestHandlers_Watch_SubscribePatternTooLong(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleWatch(ctx, makeToolRequest(map[string]any{
		"action":        "subscribe",
		"topic_pattern": strings.Repeat("x", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_Watch_UnsubscribeMissingID(t *testing.T) {
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

func TestHandlers_RemediateDLQ_MissingTopic(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleRemediateDLQ(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_RemediateDLQ_TopicTooLong(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleRemediateDLQ(ctx, makeToolRequest(map[string]any{
		"topic": strings.Repeat("x", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_SchemaDiff_MissingSubject(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleSchemaDiff(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_SchemaDiff_SubjectTooLong(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleSchemaDiff(ctx, makeToolRequest(map[string]any{
		"subject": strings.Repeat("x", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== tools_features.go Handler Tests =====================

func TestHandlers_NLQuery_MissingQuery(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleNLQuery(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_NLQuery_QueryTooLong(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleNLQuery(ctx, makeToolRequest(map[string]any{
		"query": strings.Repeat("x", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_IngestAnomaly_MissingMetric(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleIngestAnomaly(ctx, makeToolRequest(map[string]any{
		"source": "test-source",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_IngestAnomaly_MissingSource(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleIngestAnomaly(ctx, makeToolRequest(map[string]any{
		"metric": "throughput",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_IngestAnomaly_MissingBoth(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleIngestAnomaly(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_CostAnalyze_MissingTopic(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleCostAnalyze(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_CostAnalyze_TopicTooLong(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleCostAnalyze(ctx, makeToolRequest(map[string]any{
		"topic": strings.Repeat("x", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_CostWhatIf_MissingTopic(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleCostWhatIf(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_KnowledgeGraph_MissingQuery(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleKnowledgeGraphQuery(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_KnowledgeGraph_QueryTooLong(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleKnowledgeGraphQuery(ctx, makeToolRequest(map[string]any{
		"query": strings.Repeat("x", maxInputLength+1),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_Workflows_DescribeMissingTemplate(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleWorkflows(ctx, makeToolRequest(map[string]any{
		"action": "describe",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlers_Workflows_InvalidAction(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	result, err := s.handleWorkflows(ctx, makeToolRequest(map[string]any{
		"action": "invalid",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}
