package runbook

import (
	"log/slog"
	"testing"
)

func TestListRunbooks_Builtins(t *testing.T) {
	e := NewEngine(slog.Default())
	runbooks := e.ListRunbooks("")
	if len(runbooks) < 3 {
		t.Errorf("expected at least 3 built-in runbooks, got %d", len(runbooks))
	}
}

func TestListRunbooks_ByCategory(t *testing.T) {
	e := NewEngine(slog.Default())
	ops := e.ListRunbooks("operations")
	if len(ops) != 1 {
		t.Errorf("expected 1 operations runbook, got %d", len(ops))
	}
}

func TestCreateRunbook(t *testing.T) {
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
	e := NewEngine(slog.Default())
	_, err := e.Execute("nonexistent", "test", nil)
	if err == nil {
		t.Error("expected error for nonexistent runbook")
	}
}

func TestCompleteStep(t *testing.T) {
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
