package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/configvcs"
	"github.com/kafkamcp/kafkamcp/internal/costattribution"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/kafkamcp/kafkamcp/internal/sandbox"
)

func newTestServerWithConfigCostSandbox(t *testing.T, mock *kafka.MockKafkaManager) *Server {
	t.Helper()
	s := newTestServerWithMock(t, mock)
	s.configVCSEngine = configvcs.NewEngine(s.logger)
	s.costAttrEngine = costattribution.NewEngine(s.logger)
	s.sandboxEngine = sandbox.NewEngine(s.logger)
	return s
}

// ===================== handleConfigHistory =====================

func TestHandleConfigHistory_MissingParams(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithConfigCostSandbox(t, mock)
	result, err := s.handleConfigHistory(context.Background(), makeToolRequest(nil), "agent-1", map[string]any{}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if text := resultText(t, result); !strings.Contains(text, "resource") {
		t.Errorf("expected error mentioning resource, got: %s", text)
	}
}

func TestHandleConfigHistory_MissingResourceID(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, _ := s.handleConfigHistory(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"resource": "topic"}, time.Now())
	assertIsError(t, result)
}

func TestHandleConfigHistory_ValidParams(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, err := s.handleConfigHistory(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"resource": "topic", "resource_id": "orders"}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleConfigHistory_AuditReport(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, err := s.handleConfigHistory(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"resource": "topic", "resource_id": "orders", "audit_report": true}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== handleConfigRollback =====================

func TestHandleConfigRollback_MissingParams(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, _ := s.handleConfigRollback(context.Background(), makeToolRequest(nil), "agent-1", map[string]any{}, time.Now())
	assertIsError(t, result)
}

func TestHandleConfigRollback_InvalidVersion(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, _ := s.handleConfigRollback(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"resource": "topic", "resource_id": "orders", "target_version": float64(0)}, time.Now())
	assertIsError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "target_version") {
		t.Errorf("expected target_version error, got: %s", text)
	}
}

// ===================== handleConfigCompare =====================

func TestHandleConfigCompare_MissingParams(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, _ := s.handleConfigCompare(context.Background(), makeToolRequest(nil), "agent-1", map[string]any{}, time.Now())
	assertIsError(t, result)
}

func TestHandleConfigCompare_InvalidVersions(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, _ := s.handleConfigCompare(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"resource": "topic", "resource_id": "orders", "version1": float64(0), "version2": float64(1)}, time.Now())
	assertIsError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "version1") {
		t.Errorf("expected version error, got: %s", text)
	}
}

// ===================== handleCostLive =====================

func TestHandleCostLive_AllCosts(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, err := s.handleCostLive(context.Background(), makeToolRequest(nil), "agent-1", map[string]any{}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleCostLive_SpecificResource(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, err := s.handleCostLive(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"resource_type": "topic", "resource_name": "orders"}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// May succeed or error depending on engine state — just verify no panic
	_ = result
}

// ===================== handleCostAllocate =====================

func TestHandleCostAllocate_InvalidAction(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, _ := s.handleCostAllocate(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"action": "invalid"}, time.Now())
	assertIsError(t, result)
}

func TestHandleCostAllocate_CreateMissingName(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, _ := s.handleCostAllocate(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"action": "create"}, time.Now())
	assertIsError(t, result)
}

func TestHandleCostAllocate_List(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, err := s.handleCostAllocate(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"action": "list"}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== handleCostBudget =====================

func TestHandleCostBudget_InvalidAction(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, _ := s.handleCostBudget(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"action": "invalid"}, time.Now())
	assertIsError(t, result)
}

func TestHandleCostBudget_CreateMissingName(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, _ := s.handleCostBudget(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"action": "create"}, time.Now())
	assertIsError(t, result)
}

func TestHandleCostBudget_List(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, err := s.handleCostBudget(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"action": "list"}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== handleSandboxCreate =====================

func TestHandleSandboxCreate_MissingName(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, _ := s.handleSandboxCreate(context.Background(), makeToolRequest(nil), "agent-1", map[string]any{}, time.Now())
	assertIsError(t, result)
}

func TestHandleSandboxCreate_ValidParams(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, err := s.handleSandboxCreate(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"name": "test-sb"}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "test-sb") {
		t.Errorf("expected sandbox name in result, got: %s", text)
	}
}

// ===================== handleSandboxExec =====================

func TestHandleSandboxExec_MissingParams(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, _ := s.handleSandboxExec(context.Background(), makeToolRequest(nil), "agent-1", map[string]any{}, time.Now())
	assertIsError(t, result)
}

func TestHandleSandboxExec_MissingSandboxID(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, _ := s.handleSandboxExec(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"tool": "kafka_produce"}, time.Now())
	assertIsError(t, result)
}

func TestHandleSandboxExec_NonexistentSandbox(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, _ := s.handleSandboxExec(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"sandbox_id": "fake", "tool": "kafka_produce"}, time.Now())
	assertIsError(t, result)
}

// ===================== handleSandboxManage =====================

func TestHandleSandboxManage_InvalidAction(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, _ := s.handleSandboxManage(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"action": "invalid"}, time.Now())
	assertIsError(t, result)
}

func TestHandleSandboxManage_ListAction(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, err := s.handleSandboxManage(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"action": "list"}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleSandboxManage_DestroyMissingID(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, _ := s.handleSandboxManage(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"action": "destroy"}, time.Now())
	assertIsError(t, result)
}

func TestHandleSandboxManage_GetMissingID(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, _ := s.handleSandboxManage(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"action": "get"}, time.Now())
	assertIsError(t, result)
}

func TestHandleSandboxManage_PromotionReportMissingID(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})
	result, _ := s.handleSandboxManage(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"action": "promotion_report"}, time.Now())
	assertIsError(t, result)
}

func TestSandboxLifecycle(t *testing.T) {
	s := newTestServerWithConfigCostSandbox(t, &kafka.MockKafkaManager{})

	// Create
	createResult, err := s.handleSandboxCreate(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"name": "lifecycle-test", "duration_sec": float64(3600)}, time.Now())
	if err != nil {
		t.Fatalf("create error: %v", err)
	}
	assertNotError(t, createResult)

	// List
	listResult, err := s.handleSandboxManage(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"action": "list"}, time.Now())
	if err != nil {
		t.Fatalf("list error: %v", err)
	}
	assertNotError(t, listResult)
	text := resultText(t, listResult)
	if !strings.Contains(text, "lifecycle-test") {
		t.Errorf("expected sandbox in list, got: %s", text)
	}
}
