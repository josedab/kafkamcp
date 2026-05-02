package server

import (
	"context"
	"strings"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

// ===================== handleAgentRegistry: register =====================

func TestHandleAgentRegistry_Register(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	result, err := s.handleAgentRegistry(ctx, makeToolRequest(map[string]any{
		"action":     "register",
		"agent_name": "agent-alpha",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "registered") {
		t.Errorf("expected 'registered' in result, got: %s", text)
	}
}

// ===================== handleAgentRegistry: heartbeat =====================

func TestHandleAgentRegistry_Heartbeat_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	// Register first
	s.handleAgentRegistry(ctx, makeToolRequest(map[string]any{
		"action":     "register",
		"agent_name": "agent-1",
	}))

	result, err := s.handleAgentRegistry(ctx, makeToolRequest(map[string]any{
		"action": "heartbeat",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "heartbeat_updated") {
		t.Errorf("expected 'heartbeat_updated' in result, got: %s", text)
	}
}

func TestHandleAgentRegistry_Heartbeat_UnregisteredAgent(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)

	// Heartbeat without register should error
	result, err := s.handleAgentRegistry(context.Background(), makeToolRequest(map[string]any{
		"action": "heartbeat",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleAgentRegistry: claim =====================

func TestHandleAgentRegistry_Claim_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	// Register first
	s.handleAgentRegistry(ctx, makeToolRequest(map[string]any{
		"action": "register",
	}))

	result, err := s.handleAgentRegistry(ctx, makeToolRequest(map[string]any{
		"action":    "claim",
		"topic":     "orders",
		"partition": float64(0),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "claimed") {
		t.Errorf("expected 'claimed' in result, got: %s", text)
	}
}

func TestHandleAgentRegistry_Claim_MissingParams(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)

	// Missing topic
	result, err := s.handleAgentRegistry(context.Background(), makeToolRequest(map[string]any{
		"action":    "claim",
		"partition": float64(0),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)

	// Missing partition (defaults to -1)
	result, err = s.handleAgentRegistry(context.Background(), makeToolRequest(map[string]any{
		"action": "claim",
		"topic":  "orders",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleAgentRegistry: release =====================

func TestHandleAgentRegistry_Release_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	s.handleAgentRegistry(ctx, makeToolRequest(map[string]any{"action": "register"}))
	s.handleAgentRegistry(ctx, makeToolRequest(map[string]any{
		"action": "claim", "topic": "orders", "partition": float64(0),
	}))

	result, err := s.handleAgentRegistry(ctx, makeToolRequest(map[string]any{
		"action":    "release",
		"topic":     "orders",
		"partition": float64(0),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "released") {
		t.Errorf("expected 'released' in result, got: %s", text)
	}
}

func TestHandleAgentRegistry_Release_MissingParams(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)

	result, err := s.handleAgentRegistry(context.Background(), makeToolRequest(map[string]any{
		"action": "release",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleAgentRegistry: share_cursor =====================

func TestHandleAgentRegistry_ShareCursor_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	s.handleAgentRegistry(ctx, makeToolRequest(map[string]any{"action": "register"}))

	result, err := s.handleAgentRegistry(ctx, makeToolRequest(map[string]any{
		"action":    "share_cursor",
		"topic":     "orders",
		"partition": float64(0),
		"offset":    float64(42),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "cursor_shared") {
		t.Errorf("expected 'cursor_shared' in result, got: %s", text)
	}
}

func TestHandleAgentRegistry_ShareCursor_MissingParams(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)

	result, err := s.handleAgentRegistry(context.Background(), makeToolRequest(map[string]any{
		"action": "share_cursor",
		"topic":  "orders",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleAgentRegistry: handoff =====================

func TestHandleAgentRegistry_Handoff_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	s.handleAgentRegistry(ctx, makeToolRequest(map[string]any{"action": "register"}))
	// Register the target agent too
	s.agentRegistry.RegisterAgent("agent-beta", "beta")

	result, err := s.handleAgentRegistry(ctx, makeToolRequest(map[string]any{
		"action":       "handoff",
		"target_agent": "agent-beta",
		"message_data": "process partition 3",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "handoff_queued") {
		t.Errorf("expected 'handoff_queued' in result, got: %s", text)
	}
}

func TestHandleAgentRegistry_Handoff_MissingTarget(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)

	result, err := s.handleAgentRegistry(context.Background(), makeToolRequest(map[string]any{
		"action": "handoff",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "target_agent") {
		t.Error("expected error mentioning target_agent")
	}
}

// ===================== handleAgentRegistry: list_agents =====================

func TestHandleAgentRegistry_ListAgents(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	s.handleAgentRegistry(ctx, makeToolRequest(map[string]any{
		"action": "register", "agent_name": "alpha",
	}))

	result, err := s.handleAgentRegistry(ctx, makeToolRequest(map[string]any{
		"action": "list_agents",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "total_agents") {
		t.Errorf("expected 'total_agents' in result, got: %s", text)
	}
}

// ===================== handleAgentRegistry: unknown action =====================

func TestHandleAgentRegistry_UnknownAction_Mock(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)

	result, err := s.handleAgentRegistry(context.Background(), makeToolRequest(map[string]any{
		"action": "invalid_action",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "unknown action") {
		t.Error("expected 'unknown action' in error")
	}
}

// ===================== handleAgentRegistry: nil registry =====================

func TestHandleAgentRegistry_NilRegistry_Mock(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	s.agentRegistry = nil

	result, err := s.handleAgentRegistry(context.Background(), makeToolRequest(map[string]any{
		"action": "register",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "not initialized") {
		t.Error("expected 'not initialized' in error")
	}
}

// ===================== handleAgentRegistry: auth denied =====================

func TestHandleAgentRegistry_AuthDenied(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServer(t, withDefaultDeny)
	s.kafkaManager = mock

	result, err := s.handleAgentRegistry(context.Background(), makeToolRequest(map[string]any{
		"action": "register",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}
