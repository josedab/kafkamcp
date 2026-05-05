package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

// ===================== handleOrchestrationCreate =====================

func TestHandleOrchestrationCreate_Success(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{
		"name":        "migration-workflow",
		"description": "Automated migration",
	})

	result, err := s.handleOrchestrationCreate(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "created") {
		t.Error("expected 'created' in response")
	}
	if !strings.Contains(text, "migration-workflow") {
		t.Error("expected orchestration name in response")
	}
}

func TestHandleOrchestrationCreate_EmptyName(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{
		"name": "",
	})

	result, err := s.handleOrchestrationCreate(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "name is required") {
		t.Error("expected 'name is required' error")
	}
}

func TestHandleOrchestrationCreate_MissingName(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{})

	result, err := s.handleOrchestrationCreate(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleOrchestrationStatus =====================

func TestHandleOrchestrationStatus_ListAll(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{})

	result, err := s.handleOrchestrationStatus(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	var resp map[string]any
	json.Unmarshal([]byte(text), &resp)
	if _, ok := resp["orchestrations"]; !ok {
		t.Error("expected 'orchestrations' in response")
	}
}

func TestHandleOrchestrationStatus_GetByID(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	// Create one first
	createReq := makeToolRequest(map[string]any{"name": "test-orch"})
	createResult, _ := s.handleOrchestrationCreate(context.Background(), createReq)
	text := resultText(t, createResult)
	var createResp map[string]any
	json.Unmarshal([]byte(text), &createResp)
	orchMap, _ := createResp["orchestration"].(map[string]any)
	orchID, _ := orchMap["id"].(string)

	// Get by ID
	statusReq := makeToolRequest(map[string]any{
		"orchestration_id": orchID,
		"action":           "status",
	})
	result, err := s.handleOrchestrationStatus(context.Background(), statusReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleOrchestrationStatus_NonExistent(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{
		"orchestration_id": "nonexistent",
		"action":           "status",
	})

	result, err := s.handleOrchestrationStatus(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleOrchestrationStatus_ReadySteps(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	// Create orchestration
	createReq := makeToolRequest(map[string]any{"name": "ready-test"})
	createResult, _ := s.handleOrchestrationCreate(context.Background(), createReq)
	text := resultText(t, createResult)
	var createResp map[string]any
	json.Unmarshal([]byte(text), &createResp)
	orchMap, _ := createResp["orchestration"].(map[string]any)
	orchID, _ := orchMap["id"].(string)

	// Get ready steps
	req := makeToolRequest(map[string]any{
		"orchestration_id": orchID,
		"action":           "ready_steps",
	})
	result, err := s.handleOrchestrationStatus(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleOrchestrationStatus_GetContext(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	createReq := makeToolRequest(map[string]any{"name": "ctx-test"})
	createResult, _ := s.handleOrchestrationCreate(context.Background(), createReq)
	text := resultText(t, createResult)
	var createResp map[string]any
	json.Unmarshal([]byte(text), &createResp)
	orchMap, _ := createResp["orchestration"].(map[string]any)
	orchID, _ := orchMap["id"].(string)

	req := makeToolRequest(map[string]any{
		"orchestration_id": orchID,
		"action":           "context",
	})
	result, err := s.handleOrchestrationStatus(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== handleOrchestrationCompleteStep =====================

func TestHandleOrchestrationCompleteStep_MissingFields(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{
		"orchestration_id": "orch-1",
		"step_id":          "step-1",
		// missing status
	})

	result, err := s.handleOrchestrationCompleteStep(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleOrchestrationCompleteStep_Success(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	// Create orchestration
	createReq := makeToolRequest(map[string]any{"name": "complete-test"})
	createResult, _ := s.handleOrchestrationCreate(context.Background(), createReq)
	text := resultText(t, createResult)
	var createResp map[string]any
	json.Unmarshal([]byte(text), &createResp)
	orchMap, _ := createResp["orchestration"].(map[string]any)
	orchID, _ := orchMap["id"].(string)

	// Complete the step
	req := makeToolRequest(map[string]any{
		"orchestration_id": orchID,
		"step_id":          "step_1",
		"status":           "completed",
	})
	result, err := s.handleOrchestrationCompleteStep(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	if !strings.Contains(resultText(t, result), "step_completed") {
		t.Error("expected 'step_completed' in response")
	}
}

func TestHandleOrchestrationCompleteStep_MarkFailed(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	createReq := makeToolRequest(map[string]any{"name": "fail-test"})
	createResult, _ := s.handleOrchestrationCreate(context.Background(), createReq)
	text := resultText(t, createResult)
	var createResp map[string]any
	json.Unmarshal([]byte(text), &createResp)
	orchMap, _ := createResp["orchestration"].(map[string]any)
	orchID, _ := orchMap["id"].(string)

	req := makeToolRequest(map[string]any{
		"orchestration_id": orchID,
		"step_id":          "step_1",
		"status":           "failed",
		"error":            "timeout occurred",
	})
	result, err := s.handleOrchestrationCompleteStep(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== handleOrchestrationHandoff =====================

func TestHandleOrchestrationHandoff_Success(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{
		"from_agent": "agent-a",
		"to_agent":   "agent-b",
		"message":    "Please review the migration plan",
	})

	result, err := s.handleOrchestrationHandoff(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	if !strings.Contains(resultText(t, result), "handoff_created") {
		t.Error("expected 'handoff_created' status")
	}
}

func TestHandleOrchestrationHandoff_MissingFields(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})
	tests := []struct {
		name string
		args map[string]any
	}{
		{"missing from_agent", map[string]any{"to_agent": "b", "message": "m"}},
		{"missing to_agent", map[string]any{"from_agent": "a", "message": "m"}},
		{"missing message", map[string]any{"from_agent": "a", "to_agent": "b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := makeToolRequest(tt.args)
			result, err := s.handleOrchestrationHandoff(context.Background(), req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertIsError(t, result)
		})
	}
}

// ===================== handleOrchestrationAgents =====================

func TestHandleOrchestrationAgents_ListEmpty(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{"action": "list"})

	result, err := s.handleOrchestrationAgents(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	var resp map[string]any
	json.Unmarshal([]byte(text), &resp)
	if _, ok := resp["agents"]; !ok {
		t.Error("expected 'agents' in response")
	}
}

func TestHandleOrchestrationAgents_Register(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{
		"action":   "register",
		"agent_id": "monitor-agent",
		"name":     "Monitor Agent",
		"role":     "monitor",
	})

	result, err := s.handleOrchestrationAgents(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	if !strings.Contains(resultText(t, result), "registered") {
		t.Error("expected 'registered' status")
	}
}

func TestHandleOrchestrationAgents_Heartbeat(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	// Register first
	regReq := makeToolRequest(map[string]any{
		"action":   "register",
		"agent_id": "hb-agent",
		"name":     "HB Agent",
	})
	s.handleOrchestrationAgents(context.Background(), regReq)

	// Heartbeat
	hbReq := makeToolRequest(map[string]any{
		"action":   "heartbeat",
		"agent_id": "hb-agent",
	})
	result, err := s.handleOrchestrationAgents(context.Background(), hbReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	if !strings.Contains(resultText(t, result), "ok") {
		t.Error("expected 'ok' status")
	}
}

func TestHandleOrchestrationAgents_HeartbeatUnregistered(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{
		"action":   "heartbeat",
		"agent_id": "unknown-agent",
	})

	result, err := s.handleOrchestrationAgents(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}
