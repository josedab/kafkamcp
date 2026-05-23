package runbook

import (
	"log/slog"
	"testing"
)

func TestListRunbooks_Builtins(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	runbooks := e.ListRunbooks("")
	if len(runbooks) < 3 {
		t.Errorf("expected at least 3 built-in runbooks, got %d", len(runbooks))
	}
}

func TestListRunbooks_ByCategory(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	ops := e.ListRunbooks("operations")
	if len(ops) != 1 {
		t.Errorf("expected 1 operations runbook, got %d", len(ops))
	}
}

func TestCreateRunbook(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	rb, err := e.CreateRunbook(`{
		"name": "Custom Runbook",
		"category": "test",
		"steps": [{"name": "Step 1", "tool": "kafka_list_topics"}]
	}`, "test-agent")
	if err != nil {
		t.Fatal(err)
	}
	if rb.Name != "Custom Runbook" {
		t.Errorf("expected name 'Custom Runbook', got %s", rb.Name)
	}
}

func TestCreateRunbook_Invalid(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, err := e.CreateRunbook(`{"name": ""}`, "test")
	if err == nil {
		t.Error("expected error for empty name")
	}
	_, err = e.CreateRunbook(`{"name": "test"}`, "test")
	if err == nil {
		t.Error("expected error for no steps")
	}
}

func TestExecute_Success(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	exec, err := e.Execute("rb_rolling_restart_check", "test-agent", nil)
	if err != nil {
		t.Fatal(err)
	}
	if exec.Status != "running" {
		t.Errorf("expected running, got %s", exec.Status)
	}
	if len(exec.Steps) != 4 {
		t.Errorf("expected 4 steps, got %d", len(exec.Steps))
	}
}

func TestExecute_NotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, err := e.Execute("nonexistent", "test", nil)
	if err == nil {
		t.Error("expected error for nonexistent runbook")
	}
}

func TestCompleteStep(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	exec, _ := e.Execute("rb_rolling_restart_check", "test-agent", nil)

	for i := range exec.Steps {
		err := e.CompleteStep(exec.ID, i, "completed", nil, "")
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}

	got, _ := e.GetExecution(exec.ID)
	if got.Status != "completed" {
		t.Errorf("expected completed, got %s", got.Status)
	}
	if len(got.AuditLog) < 2 {
		t.Error("expected audit log entries")
	}
}

func TestGetRunbook_Exists(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	rb, err := e.GetRunbook("rb_partition_reassign")
	if err != nil {
		t.Fatalf("GetRunbook: %v", err)
	}
	if rb.ID != "rb_partition_reassign" {
		t.Errorf("expected ID rb_partition_reassign, got %s", rb.ID)
	}
}

func TestGetRunbook_NotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, err := e.GetRunbook("nonexistent")
	if err == nil {
		t.Error("expected error for non-existent runbook")
	}
}

func TestListExecutions_FilterByRunbook(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, _ = e.Execute("rb_rolling_restart_check", "agent1", nil)
	_, _ = e.Execute("rb_partition_reassign", "agent1", nil)
	_, _ = e.Execute("rb_rolling_restart_check", "agent1", nil)

	filtered := e.ListExecutions("rb_rolling_restart_check", 0)
	for _, ex := range filtered {
		if ex.RunbookID != "rb_rolling_restart_check" {
			t.Errorf("expected rb_rolling_restart_check, got %s", ex.RunbookID)
		}
	}
	if len(filtered) != 2 {
		t.Errorf("expected 2 executions, got %d", len(filtered))
	}
}

func TestListExecutions_DefaultLimit(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, _ = e.Execute("rb_rolling_restart_check", "agent1", nil)

	// limit <= 0 defaults to 50
	results := e.ListExecutions("", 0)
	if len(results) != 1 {
		t.Errorf("expected 1 execution, got %d", len(results))
	}
}

func TestListExecutions_WithLimit(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	for i := 0; i < 5; i++ {
		_, _ = e.Execute("rb_rolling_restart_check", "agent1", nil)
	}

	results := e.ListExecutions("", 2)
	if len(results) != 2 {
		t.Errorf("expected 2 executions with limit, got %d", len(results))
	}
}

func TestListExecutions_Empty(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	results := e.ListExecutions("", 0)
	if len(results) != 0 {
		t.Errorf("expected 0 executions, got %d", len(results))
	}
}

func TestListExecutions_SortedByTime(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, _ = e.Execute("rb_rolling_restart_check", "agent1", nil)
	_, _ = e.Execute("rb_partition_reassign", "agent1", nil)

	results := e.ListExecutions("", 0)
	if len(results) < 2 {
		t.Fatal("expected at least 2 executions")
	}
	if results[0].StartedAt.Before(results[1].StartedAt) {
		t.Error("expected descending order by started_at")
	}
}

func TestDeleteRunbook_Success(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	err := e.DeleteRunbook("rb_partition_reassign")
	if err != nil {
		t.Fatalf("DeleteRunbook: %v", err)
	}
	_, err = e.GetRunbook("rb_partition_reassign")
	if err == nil {
		t.Error("expected error after deletion")
	}
}

func TestDeleteRunbook_NotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	err := e.DeleteRunbook("nonexistent")
	if err == nil {
		t.Error("expected error for non-existent runbook")
	}
}

func TestDeleteRunbook_Isolation(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_ = e.DeleteRunbook("rb_partition_reassign")

	// Other runbooks should still exist
	_, err := e.GetRunbook("rb_topic_migration")
	if err != nil {
		t.Errorf("other runbook should still exist: %v", err)
	}
}

func TestEnableRunbook_Toggle(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	err := e.EnableRunbook("rb_rolling_restart_check", false)
	if err != nil {
		t.Fatalf("EnableRunbook(false): %v", err)
	}
	rb, _ := e.GetRunbook("rb_rolling_restart_check")
	if rb.Enabled {
		t.Error("expected disabled")
	}

	err = e.EnableRunbook("rb_rolling_restart_check", true)
	if err != nil {
		t.Fatalf("EnableRunbook(true): %v", err)
	}
	rb, _ = e.GetRunbook("rb_rolling_restart_check")
	if !rb.Enabled {
		t.Error("expected enabled")
	}
}

func TestEnableRunbook_NotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	err := e.EnableRunbook("nonexistent", true)
	if err == nil {
		t.Error("expected error for non-existent runbook")
	}
}

func TestEnableRunbook_Idempotent(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_ = e.EnableRunbook("rb_rolling_restart_check", true)
	err := e.EnableRunbook("rb_rolling_restart_check", true)
	if err != nil {
		t.Fatalf("idempotent enable: %v", err)
	}
	rb, _ := e.GetRunbook("rb_rolling_restart_check")
	if !rb.Enabled {
		t.Error("expected enabled after idempotent call")
	}
}

func TestExecute_DisabledRunbook(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_ = e.EnableRunbook("rb_rolling_restart_check", false)

	_, err := e.Execute("rb_rolling_restart_check", "agent1", nil)
	if err == nil {
		t.Error("expected error executing disabled runbook")
	}
}

func TestLifecycle_CreateExecuteListDelete(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	// Create
	rb, err := e.CreateRunbook(`{
		"name": "Lifecycle Test",
		"category": "test",
		"steps": [{"name": "Check", "tool": "kafka_list_topics"}]
	}`, "tester")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Execute
	exec, err := e.Execute(rb.ID, "tester", nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// Complete step
	err = e.CompleteStep(exec.ID, 0, "completed", "ok", "")
	if err != nil {
		t.Fatalf("CompleteStep: %v", err)
	}

	// List executions
	execs := e.ListExecutions(rb.ID, 0)
	if len(execs) != 1 {
		t.Errorf("expected 1 execution, got %d", len(execs))
	}

	// Delete
	err = e.DeleteRunbook(rb.ID)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Verify deleted
	_, err = e.GetRunbook(rb.ID)
	if err == nil {
		t.Error("expected error after deletion")
	}
}
