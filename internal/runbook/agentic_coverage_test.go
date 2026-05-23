package runbook

import (
	"log/slog"
	"sync"
	"testing"
)

const testRunbookYAML = `
name: "Rollback Test"
category: "operations"
steps:
  - name: "Step 1"
    tool: "kafka_list_topics"
  - name: "Step 2"
    tool: "kafka_describe_topic"
rollback:
  - name: "Revert Step 1"
    tool: "kafka_cluster_info"
    args:
      action: "revert"
`

const testRunbookNoRollbackYAML = `
name: "No Rollback"
steps:
  - name: "Step 1"
    tool: "kafka_list_topics"
`

const testRunbookWithApprovalYAML = `
name: "Approval Test"
steps:
  - name: "Step 1"
    tool: "kafka_list_topics"
    approval_gate:
      required: true
      message: "Approve step 1?"
      timeout: "30m"
  - name: "Step 2"
    tool: "kafka_describe_topic"
`

// --- RollbackExecution tests ---

func TestRollbackExecution_FromFailedState(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	rb, err := e.CreateAgenticRunbook(testRunbookYAML, "tester")
	if err != nil {
		t.Fatalf("CreateAgenticRunbook: %v", err)
	}

	exec, err := e.ExecuteAgentic(rb.ID, "tester", nil, false)
	if err != nil {
		t.Fatalf("ExecuteAgentic: %v", err)
	}

	// Simulate failure
	e.mu.Lock()
	exec.Status = "failed"
	e.mu.Unlock()

	result, err := e.RollbackExecution(exec.ID)
	if err != nil {
		t.Fatalf("RollbackExecution: %v", err)
	}
	if result.Status != "rolling_back" {
		t.Errorf("Status = %q, want rolling_back", result.Status)
	}

	// Check audit log
	found := false
	for _, entry := range result.AuditLog {
		if entry.Event == "rollback_started" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected rollback_started audit entry")
	}
}

func TestRollbackExecution_FromCompletedShouldReject(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	rb, err := e.CreateAgenticRunbook(testRunbookYAML, "tester")
	if err != nil {
		t.Fatalf("CreateAgenticRunbook: %v", err)
	}

	exec, err := e.ExecuteAgentic(rb.ID, "tester", nil, true) // dry run = completed
	if err != nil {
		t.Fatalf("ExecuteAgentic: %v", err)
	}

	_, err = e.RollbackExecution(exec.ID)
	if err == nil {
		t.Fatal("expected error when rolling back completed execution")
	}
}

func TestRollbackExecution_NoRollbackSteps(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	rb, err := e.CreateAgenticRunbook(testRunbookNoRollbackYAML, "tester")
	if err != nil {
		t.Fatalf("CreateAgenticRunbook: %v", err)
	}

	exec, err := e.ExecuteAgentic(rb.ID, "tester", nil, false)
	if err != nil {
		t.Fatalf("ExecuteAgentic: %v", err)
	}

	// Simulate failure
	e.mu.Lock()
	exec.Status = "failed"
	e.mu.Unlock()

	_, err = e.RollbackExecution(exec.ID)
	if err == nil {
		t.Fatal("expected error when no rollback steps defined")
	}
}

func TestRollbackExecution_NonExistentExecution(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, err := e.RollbackExecution("nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent execution")
	}
}

// --- ResumeExecution tests ---

func TestResumeExecution_FromPausedState(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	rb, err := e.CreateAgenticRunbook(testRunbookWithApprovalYAML, "tester")
	if err != nil {
		t.Fatalf("CreateAgenticRunbook: %v", err)
	}

	exec, err := e.ExecuteAgentic(rb.ID, "tester", nil, false)
	if err != nil {
		t.Fatalf("ExecuteAgentic: %v", err)
	}
	if exec.Status != "paused" {
		t.Fatalf("expected paused status, got %q", exec.Status)
	}

	// Approve to clear pending, then set back to paused for resume
	_ = e.ApproveStep(exec.ID, "admin")

	// Set to paused again to test resume
	e.mu.Lock()
	exec.Status = "paused"
	exec.Checkpoint = 1
	e.mu.Unlock()

	err = e.ResumeExecution(exec.ID)
	if err != nil {
		t.Fatalf("ResumeExecution: %v", err)
	}

	updated, _ := e.GetAgenticExecution(exec.ID)
	if updated.Status != "running" {
		t.Errorf("Status = %q, want running", updated.Status)
	}

	// Check audit log for resume entry
	found := false
	for _, entry := range updated.AuditLog {
		if entry.Event == "execution_resumed" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected execution_resumed audit entry")
	}
}

func TestResumeExecution_FromNonPausedState(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	rb, err := e.CreateAgenticRunbook(testRunbookNoRollbackYAML, "tester")
	if err != nil {
		t.Fatalf("CreateAgenticRunbook: %v", err)
	}

	exec, err := e.ExecuteAgentic(rb.ID, "tester", nil, false)
	if err != nil {
		t.Fatalf("ExecuteAgentic: %v", err)
	}

	// exec is "running", not "paused"
	err = e.ResumeExecution(exec.ID)
	if err == nil {
		t.Fatal("expected error when resuming non-paused execution")
	}
}

func TestResumeExecution_NonExistent(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	err := e.ResumeExecution("nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent execution")
	}
}

func TestResumeExecution_CheckpointPreservation(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	rb, err := e.CreateAgenticRunbook(testRunbookWithApprovalYAML, "tester")
	if err != nil {
		t.Fatalf("CreateAgenticRunbook: %v", err)
	}

	exec, err := e.ExecuteAgentic(rb.ID, "tester", nil, false)
	if err != nil {
		t.Fatalf("ExecuteAgentic: %v", err)
	}

	_ = e.ApproveStep(exec.ID, "admin")
	e.mu.Lock()
	exec.Status = "paused"
	exec.Checkpoint = 3
	e.mu.Unlock()

	_ = e.ResumeExecution(exec.ID)

	updated, _ := e.GetAgenticExecution(exec.ID)
	if updated.Checkpoint != 3 {
		t.Errorf("Checkpoint = %d, want 3 (should be preserved)", updated.Checkpoint)
	}
}

// --- GetAgenticRunbook tests ---

func TestGetAgenticRunbook_Existing(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	rb, _ := e.CreateAgenticRunbook(testRunbookYAML, "tester")

	found, err := e.GetAgenticRunbook(rb.ID)
	if err != nil {
		t.Fatalf("GetAgenticRunbook: %v", err)
	}
	if found.Name != "Rollback Test" {
		t.Errorf("Name = %q, want Rollback Test", found.Name)
	}
}

func TestGetAgenticRunbook_NonExisting(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, err := e.GetAgenticRunbook("nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existing runbook")
	}
}

// --- ListAgenticRunbooks tests ---

func TestListAgenticRunbooks_Unfiltered(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, _ = e.CreateAgenticRunbook(testRunbookYAML, "a")
	_, _ = e.CreateAgenticRunbook(testRunbookNoRollbackYAML, "b")

	all := e.ListAgenticRunbooks("")
	if len(all) < 2 {
		t.Errorf("expected at least 2 runbooks, got %d", len(all))
	}
}

func TestListAgenticRunbooks_FilteredByCategory(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, _ = e.CreateAgenticRunbook(testRunbookYAML, "a") // category: operations

	ops := e.ListAgenticRunbooks("operations")
	for _, rb := range ops {
		if rb.Category != "operations" {
			t.Errorf("expected category=operations, got %q", rb.Category)
		}
	}
}

func TestListAgenticRunbooks_EmptyResult(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	result := e.ListAgenticRunbooks("nonexistent-category")
	if len(result) != 0 {
		t.Errorf("expected 0 runbooks, got %d", len(result))
	}
}

// --- ListAgenticExecutions tests ---

func TestListAgenticExecutions_DefaultLimit(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	rb, _ := e.CreateAgenticRunbook(testRunbookNoRollbackYAML, "a")
	for i := 0; i < 3; i++ {
		_, _ = e.ExecuteAgentic(rb.ID, "a", nil, true)
	}

	result := e.ListAgenticExecutions(0) // 0 should default to 50
	if len(result) < 3 {
		t.Errorf("expected at least 3 executions, got %d", len(result))
	}
}

func TestListAgenticExecutions_CustomLimit(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	rb, _ := e.CreateAgenticRunbook(testRunbookNoRollbackYAML, "a")
	for i := 0; i < 5; i++ {
		_, _ = e.ExecuteAgentic(rb.ID, "a", nil, true)
	}

	result := e.ListAgenticExecutions(2)
	if len(result) > 2 {
		t.Errorf("expected max 2 executions, got %d", len(result))
	}
}

func TestListAgenticExecutions_LimitGreaterThanTotal(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	rb, _ := e.CreateAgenticRunbook(testRunbookNoRollbackYAML, "a")
	_, _ = e.ExecuteAgentic(rb.ID, "a", nil, true)

	result := e.ListAgenticExecutions(100)
	if len(result) < 1 {
		t.Error("expected at least 1 execution")
	}
}

// --- Audit log tests ---

func TestAuditLog_ContainsTimestampAndAction(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	rb, _ := e.CreateAgenticRunbook(testRunbookYAML, "tester")
	exec, _ := e.ExecuteAgentic(rb.ID, "tester", nil, true)

	if len(exec.AuditLog) == 0 {
		t.Fatal("expected audit log entries")
	}

	for _, entry := range exec.AuditLog {
		if entry.Timestamp.IsZero() {
			t.Error("audit entry has zero timestamp")
		}
		if entry.Event == "" {
			t.Error("audit entry has empty event")
		}
		if entry.Details == "" {
			t.Error("audit entry has empty details")
		}
	}
}

func TestAuditLog_RollbackAddsEntry(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	rb, _ := e.CreateAgenticRunbook(testRunbookYAML, "tester")
	exec, _ := e.ExecuteAgentic(rb.ID, "tester", nil, false)

	e.mu.Lock()
	exec.Status = "failed"
	e.mu.Unlock()

	result, _ := e.RollbackExecution(exec.ID)
	found := false
	for _, entry := range result.AuditLog {
		if entry.Event == "rollback_started" {
			found = true
		}
	}
	if !found {
		t.Error("expected rollback_started in audit log")
	}
}

// --- Concurrent rollback + resume ---

func TestConcurrentRollbackResume(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	rb, _ := e.CreateAgenticRunbook(testRunbookYAML, "tester")
	exec, _ := e.ExecuteAgentic(rb.ID, "tester", nil, false)

	var wg sync.WaitGroup
	errs := make(chan error, 20)

	// Try concurrent rollback and resume
	for i := 0; i < 5; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			e.mu.Lock()
			exec.Status = "failed"
			e.mu.Unlock()
			_, err := e.RollbackExecution(exec.ID)
			if err != nil {
				errs <- err
			}
		}()
		go func() {
			defer wg.Done()
			e.mu.Lock()
			exec.Status = "paused"
			e.mu.Unlock()
			err := e.ResumeExecution(exec.ID)
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)

	// We just verify no panics or data races (run with -race)
	for err := range errs {
		// Errors are expected due to state conflicts - just log them
		_ = err
	}
}

// --- RollbackExecution from running state ---

func TestRollbackExecution_FromRunningState(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	rb, _ := e.CreateAgenticRunbook(testRunbookYAML, "tester")
	exec, _ := e.ExecuteAgentic(rb.ID, "tester", nil, false)

	if exec.Status != "running" {
		t.Fatalf("expected running status, got %q", exec.Status)
	}

	result, err := e.RollbackExecution(exec.ID)
	if err != nil {
		t.Fatalf("RollbackExecution from running: %v", err)
	}
	if result.Status != "rolling_back" {
		t.Errorf("Status = %q, want rolling_back", result.Status)
	}
}
