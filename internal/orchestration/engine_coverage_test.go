package orchestration

import (
	"log/slog"
	"testing"
	"time"
)

// --- ListOrchestrations ---

func TestListOrchestrations_Empty(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	result := e.ListOrchestrations()
	if len(result) != 0 {
		t.Errorf("expected 0, got %d", len(result))
	}
}

func TestListOrchestrations_Multiple(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	e.CreateOrchestration("orch-1", "desc1", "coord", []OrchestrationStep{
		{ID: "s1", Name: "step1", Tool: "tool1"},
	})
	e.CreateOrchestration("orch-2", "desc2", "coord", []OrchestrationStep{
		{ID: "s1", Name: "step1", Tool: "tool1"},
	})

	result := e.ListOrchestrations()
	if len(result) != 2 {
		t.Errorf("expected 2, got %d", len(result))
	}
}

func TestListOrchestrations_FilterByStatus(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	orch1, _ := e.CreateOrchestration("active", "", "c", []OrchestrationStep{
		{ID: "s1", Name: "a", Tool: "t"},
	})
	e.CreateOrchestration("pending", "", "c", []OrchestrationStep{
		{ID: "s1", Name: "a", Tool: "t"},
	})

	// Complete orch1
	e.CompleteStep(orch1.ID, "s1", "a1", "completed", nil, "")

	result := e.ListOrchestrations()
	completed := 0
	for _, o := range result {
		if o.Status == "completed" {
			completed++
		}
	}
	if completed != 1 {
		t.Errorf("expected 1 completed, got %d", completed)
	}
}

// --- AgentHeartbeat ---

func TestAgentHeartbeat_Valid(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterAgent("agent-1", "Monitor", RoleMonitor, nil)

	before := time.Now()
	time.Sleep(1 * time.Millisecond)
	err := e.AgentHeartbeat("agent-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	e.mu.RLock()
	agent := e.agents["agent-1"]
	e.mu.RUnlock()
	if !agent.LastHeartbeat.After(before) {
		t.Error("expected LastHeartbeat to be updated")
	}
}

func TestAgentHeartbeat_Unregistered(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	err := e.AgentHeartbeat("nonexistent")
	if err == nil {
		t.Error("expected error for unregistered agent")
	}
}

func TestAgentHeartbeat_AfterTimeout(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterAgent("agent-1", "Monitor", RoleMonitor, nil)

	// Simulate stale heartbeat
	e.mu.Lock()
	e.agents["agent-1"].LastHeartbeat = time.Now().Add(-10 * time.Minute)
	e.mu.Unlock()

	err := e.AgentHeartbeat("agent-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	e.mu.RLock()
	agent := e.agents["agent-1"]
	e.mu.RUnlock()
	if time.Since(agent.LastHeartbeat) > 1*time.Second {
		t.Error("heartbeat should have refreshed to now")
	}
}

// --- CompleteStep improvements ---

func TestCompleteStep_DownstreamUnlock(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	steps := []OrchestrationStep{
		{ID: "s1", Name: "First", Tool: "t1"},
		{ID: "s2", Name: "Second", Tool: "t2", DependsOn: []string{"s1"}},
		{ID: "s3", Name: "Third", Tool: "t3", DependsOn: []string{"s1"}},
	}
	orch, _ := e.CreateOrchestration("test", "", "c", steps)

	// Before completing s1, s2 and s3 should not be ready
	ready, _ := e.GetReadySteps(orch.ID)
	if len(ready) != 1 {
		t.Fatalf("expected 1 ready step, got %d", len(ready))
	}

	e.CompleteStep(orch.ID, "s1", "a1", "completed", nil, "")

	// After completing s1, both s2 and s3 should be ready
	ready, _ = e.GetReadySteps(orch.ID)
	if len(ready) != 2 {
		t.Errorf("expected 2 ready steps after s1 complete, got %d", len(ready))
	}
}

func TestCompleteStep_AlreadyCompleted(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterAgent("a1", "Agent", RoleOperator, nil)

	steps := []OrchestrationStep{
		{ID: "s1", Name: "First", Tool: "t1"},
	}
	orch, _ := e.CreateOrchestration("test", "", "c", steps)

	// Complete once
	err := e.CompleteStep(orch.ID, "s1", "a1", "completed", "result1", "")
	if err != nil {
		t.Fatalf("first complete: %v", err)
	}

	// Complete again - should overwrite
	err = e.CompleteStep(orch.ID, "s1", "a1", "completed", "result2", "")
	if err != nil {
		t.Fatalf("second complete: %v", err)
	}
}

// --- CreateHandoff improvements ---

func TestCreateHandoff_EmptyFromTo(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	_, err := e.CreateHandoff("", "to", "msg", nil)
	if err == nil {
		t.Error("expected error for empty from_agent")
	}

	_, err = e.CreateHandoff("from", "", "msg", nil)
	if err == nil {
		t.Error("expected error for empty to_agent")
	}
}

func TestCreateHandoff_WithContext(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterAgent("a1", "From", RoleMonitor, nil)
	e.RegisterAgent("a2", "To", RoleOperator, nil)

	ctx := map[string]any{"topic": "orders", "lag": 5000}
	h, err := e.CreateHandoff("a1", "a2", "Please fix lag", ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.Context["topic"] != "orders" {
		t.Error("expected context to be preserved")
	}
}

func TestCreateHandoff_NilContext(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	h, err := e.CreateHandoff("a1", "a2", "msg", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.Context != nil {
		t.Error("expected nil context to be preserved as nil")
	}
}
