package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/adaptiverate"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/kafkamcp/kafkamcp/internal/taskqueue"
)

func setupTaskQueueServer(t *testing.T) (*Server, context.Context, time.Time) {
	t.Helper()
	s := newTestServerWithMock(t, &kafka.MockKafkaManager{})
	s.taskQueueEngine = taskqueue.NewEngine(s.logger)
	s.adaptiveRateEngine = adaptiverate.NewEngine(s.logger)
	return s, context.Background(), time.Now()
}

// --- handleTaskPublish ---

func TestHandleTaskPublish_Valid(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)
	args := map[string]any{"queue": "test-q", "type": "compute"}
	result, err := s.handleTaskPublish(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("expected success, got error result")
	}
}

func TestHandleTaskPublish_MissingQueue(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)
	args := map[string]any{"type": "compute"}
	result, err := s.handleTaskPublish(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error result for missing queue")
	}
}

func TestHandleTaskPublish_MissingType(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)
	args := map[string]any{"queue": "test-q"}
	result, err := s.handleTaskPublish(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error result for missing type")
	}
}

func TestHandleTaskPublish_WithPriorityAndRetries(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)
	args := map[string]any{
		"queue":       "test-q",
		"type":        "compute",
		"priority":    float64(1),
		"max_retries": float64(5),
	}
	result, err := s.handleTaskPublish(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("expected success, got error result")
	}
}

// --- handleTaskClaim ---

func TestHandleTaskClaim_WithTask(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)

	// Publish a task first
	publishArgs := map[string]any{"queue": "test-q", "type": "compute"}
	_, err := s.handleTaskPublish(ctx, mcp.CallToolRequest{}, "agent-1", publishArgs, start)
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	// Claim it
	claimArgs := map[string]any{"queue": "test-q"}
	result, err := s.handleTaskClaim(ctx, mcp.CallToolRequest{}, "agent-1", claimArgs, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("expected success claiming task")
	}
}

func TestHandleTaskClaim_MissingQueue(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)
	args := map[string]any{}
	result, err := s.handleTaskClaim(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error result for missing queue")
	}
}

func TestHandleTaskClaim_EmptyQueue(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)
	args := map[string]any{"queue": "empty-q"}
	result, err := s.handleTaskClaim(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error result for empty queue")
	}
}

// --- handleTaskComplete ---

func TestHandleTaskComplete_Valid(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)

	// Publish and claim a task
	publishArgs := map[string]any{"queue": "test-q", "type": "compute"}
	_, _ = s.handleTaskPublish(ctx, mcp.CallToolRequest{}, "agent-1", publishArgs, start)
	claimArgs := map[string]any{"queue": "test-q"}
	claimResult, _ := s.handleTaskClaim(ctx, mcp.CallToolRequest{}, "agent-1", claimArgs, start)

	// Extract task_id from claim result
	taskID := extractTaskID(t, claimResult)

	completeArgs := map[string]any{"task_id": taskID, "status": "completed"}
	result, err := s.handleTaskComplete(ctx, mcp.CallToolRequest{}, "agent-1", completeArgs, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("expected success completing task")
	}
}

func TestHandleTaskComplete_MissingTaskID(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)
	args := map[string]any{"status": "completed"}
	result, err := s.handleTaskComplete(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error result for missing task_id")
	}
}

func TestHandleTaskComplete_MissingStatus(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)
	args := map[string]any{"task_id": "some-id"}
	result, err := s.handleTaskComplete(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error result for missing status")
	}
}

// --- handleTaskStatus ---

func TestHandleTaskStatus_Stats(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)
	args := map[string]any{"action": "stats"}
	result, err := s.handleTaskStatus(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("expected success for stats action")
	}
}

func TestHandleTaskStatus_List(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)
	args := map[string]any{"action": "list"}
	result, err := s.handleTaskStatus(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("expected success for list action")
	}
}

func TestHandleTaskStatus_GetValid(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)

	// Publish a task to get an ID
	publishArgs := map[string]any{"queue": "test-q", "type": "compute"}
	pubResult, _ := s.handleTaskPublish(ctx, mcp.CallToolRequest{}, "agent-1", publishArgs, start)
	taskID := extractTaskID(t, pubResult)

	args := map[string]any{"action": "get", "task_id": taskID}
	result, err := s.handleTaskStatus(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("expected success for get action with valid task_id")
	}
}

func TestHandleTaskStatus_GetMissingTaskID(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)
	args := map[string]any{"action": "get"}
	result, err := s.handleTaskStatus(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error for get action without task_id")
	}
}

func TestHandleTaskStatus_HeartbeatMissingTaskID(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)
	args := map[string]any{"action": "heartbeat"}
	result, err := s.handleTaskStatus(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error for heartbeat without task_id")
	}
}

func TestHandleTaskStatus_InvalidAction(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)
	args := map[string]any{"action": "invalid"}
	result, err := s.handleTaskStatus(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error for invalid action")
	}
}

// --- handleQuotaStatus ---

func TestHandleQuotaStatus_NoAgentID(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)
	args := map[string]any{}
	result, err := s.handleQuotaStatus(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("expected success for quota status without agent_id")
	}
}

func TestHandleQuotaStatus_WithAgentID(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)
	args := map[string]any{"agent_id": "agent-1"}
	result, err := s.handleQuotaStatus(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("expected success for quota status with agent_id")
	}
}

// --- handleQuotaRequest ---

func TestHandleQuotaRequest_Increase(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)
	args := map[string]any{"action": "increase", "amount": float64(10)}
	result, err := s.handleQuotaRequest(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("expected success for quota increase")
	}
}

func TestHandleQuotaRequest_Reset(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)
	args := map[string]any{"action": "reset"}
	result, err := s.handleQuotaRequest(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("expected success for quota reset")
	}
}

func TestHandleQuotaRequest_MissingAction(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)
	args := map[string]any{}
	result, err := s.handleQuotaRequest(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error for missing action")
	}
}

// --- handleRateIntelligence ---

func TestHandleRateIntelligence(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)
	args := map[string]any{}
	result, err := s.handleRateIntelligence(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("expected success for rate intelligence")
	}
}

// --- handleClusterHealthUpdate ---

func TestHandleClusterHealthUpdate_Valid(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)
	args := map[string]any{"status": "healthy", "cpu_utilization": float64(0.5)}
	result, err := s.handleClusterHealthUpdate(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("expected success for valid cluster health update")
	}
}

func TestHandleClusterHealthUpdate_MissingStatus(t *testing.T) {
	s, ctx, start := setupTaskQueueServer(t)
	args := map[string]any{}
	result, err := s.handleClusterHealthUpdate(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error for missing status")
	}
}

// extractTaskID parses the task ID from a CallToolResult's JSON content.
func extractTaskID(t *testing.T, result *mcp.CallToolResult) string {
	t.Helper()
	if len(result.Content) == 0 {
		t.Fatal("no content in result")
	}
	textContent, ok := result.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatal("expected TextContent in result")
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(textContent.Text), &data); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}
	id, ok := data["id"].(string)
	if !ok || id == "" {
		t.Fatal("no task id found in result")
	}
	return id
}
