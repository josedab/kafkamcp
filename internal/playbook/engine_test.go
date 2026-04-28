package playbook

import (
	"log/slog"
	"testing"
)

func newTestEngine() *Engine {
	return NewEngine(slog.Default())
}

func TestBuiltinPlaybooks(t *testing.T) {
	e := newTestEngine()
	playbooks := e.ListPlaybooks()
	if len(playbooks) < 3 {
		t.Errorf("expected at least 3 builtin playbooks, got %d", len(playbooks))
	}
}

func TestCreatePlaybook(t *testing.T) {
	e := newTestEngine()

	steps := []RemediationStep{
		{Name: "Check disk", Tool: "kafka_cluster_info"},
		{Name: "Clean logs", Tool: "kafka_cluster_info", OnFailure: "continue"},
	}
	trigger := TriggerConfig{Type: TriggerDiskUsage, Threshold: 80}

	pb, err := e.CreatePlaybook("Disk Usage Response", "Handle high disk usage", "agent", trigger, steps)
	if err != nil {
		t.Fatal(err)
	}
	if pb.ID == "" {
		t.Error("expected non-empty ID")
	}
	if len(pb.Steps) != 2 {
		t.Errorf("expected 2 steps, got %d", len(pb.Steps))
	}
}

func TestCreatePlaybookValidation(t *testing.T) {
	e := newTestEngine()

	if _, err := e.CreatePlaybook("", "", "", TriggerConfig{}, nil); err == nil {
		t.Error("expected error for empty name")
	}
	if _, err := e.CreatePlaybook("test", "", "", TriggerConfig{}, nil); err == nil {
		t.Error("expected error for no steps")
	}
}

func TestExecutePlaybook(t *testing.T) {
	e := newTestEngine()

	exec, err := e.Execute("pb_high_lag", "agent-1", map[string]any{"lag": 15000, "group": "my-group"})
	if err != nil {
		t.Fatal(err)
	}
	if exec.Status != "running" {
		t.Errorf("expected running, got %s", exec.Status)
	}
	if len(exec.Steps) == 0 {
		t.Error("expected steps in execution")
	}

	// Complete steps
	for i := range exec.Steps {
		e.CompleteStep(exec.ID, i, "completed", nil, "")
	}

	got, _ := e.GetExecution(exec.ID)
	if got.Status != "completed" {
		t.Errorf("expected completed, got %s", got.Status)
	}
}

func TestExecuteDisabledPlaybook(t *testing.T) {
	e := newTestEngine()
	e.EnablePlaybook("pb_high_lag", false)

	_, err := e.Execute("pb_high_lag", "agent", nil)
	if err == nil {
		t.Error("expected error for disabled playbook")
	}
}

func TestMatchTrigger(t *testing.T) {
	e := newTestEngine()

	matches := e.MatchTrigger(TriggerHighLag, 15000)
	if len(matches) == 0 {
		t.Error("expected matches for high lag trigger")
	}

	matches = e.MatchTrigger(TriggerHighLag, 100)
	if len(matches) != 0 {
		t.Error("expected no matches below threshold")
	}

	matches = e.MatchTrigger(TriggerCustom, 0)
	if len(matches) != 0 {
		t.Error("expected no matches for unknown trigger type")
	}
}

func TestPlaybookCooldown(t *testing.T) {
	e := newTestEngine()

	// First execution should succeed
	_, err := e.Execute("pb_high_lag", "agent", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Second execution should be blocked by cooldown
	_, err = e.Execute("pb_high_lag", "agent", nil)
	if err == nil {
		t.Error("expected cooldown error")
	}
}

func TestEnableDisablePlaybook(t *testing.T) {
	e := newTestEngine()

	if err := e.EnablePlaybook("pb_high_lag", false); err != nil {
		t.Fatal(err)
	}
	pb, _ := e.GetPlaybook("pb_high_lag")
	if pb.Enabled {
		t.Error("expected disabled")
	}

	if err := e.EnablePlaybook("nonexistent", true); err == nil {
		t.Error("expected error for nonexistent playbook")
	}
}

func TestDeletePlaybook(t *testing.T) {
	e := newTestEngine()

	if err := e.DeletePlaybook("pb_high_lag"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.GetPlaybook("pb_high_lag"); err == nil {
		t.Error("expected error after delete")
	}
}

func TestListExecutions(t *testing.T) {
	e := newTestEngine()

	e.Execute("pb_under_replicated", "a1", nil)
	e.Execute("pb_throughput_spike", "a2", nil)

	execs := e.ListExecutions("", 10)
	if len(execs) != 2 {
		t.Errorf("expected 2 executions, got %d", len(execs))
	}
}
