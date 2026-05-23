package dagworkflow

import (
	"log/slog"
	"os"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestCreateWorkflow(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	steps := []DAGStep{
		{ID: "step1", Name: "Consume", Tool: "kafka_consume", Args: map[string]string{"topic": "${topic}"}},
		{ID: "step2", Name: "Transform", Tool: "kafka_produce", DependsOn: []string{"step1"}},
	}

	wf, err := e.CreateWorkflow("test-wf", "Test workflow", "agent-1", nil, steps, nil)
	if err != nil {
		t.Fatalf("CreateWorkflow failed: %v", err)
	}
	if wf.Name != "test-wf" {
		t.Errorf("expected name test-wf, got %s", wf.Name)
	}
	if wf.Version != 1 {
		t.Errorf("expected version 1, got %d", wf.Version)
	}
}

func TestCreateWorkflowVersioning(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	steps := []DAGStep{{ID: "s1", Name: "S1", Tool: "tool1"}}

	wf1, _ := e.CreateWorkflow("my-wf", "", "agent-1", nil, steps, nil)
	wf2, _ := e.CreateWorkflow("my-wf", "", "agent-1", nil, steps, nil)

	if wf1.Version != 1 || wf2.Version != 2 {
		t.Errorf("expected versions 1,2 got %d,%d", wf1.Version, wf2.Version)
	}
}

func TestDAGCycleDetection(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	steps := []DAGStep{
		{ID: "a", Name: "A", Tool: "t1", DependsOn: []string{"b"}},
		{ID: "b", Name: "B", Tool: "t2", DependsOn: []string{"a"}},
	}

	_, err := e.CreateWorkflow("cyclic", "", "agent-1", nil, steps, nil)
	if err == nil {
		t.Error("expected cycle detection error")
	}
}

func TestRunAndReadySteps(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	steps := []DAGStep{
		{ID: "s1", Name: "S1", Tool: "t1"},
		{ID: "s2", Name: "S2", Tool: "t2", DependsOn: []string{"s1"}},
		{ID: "s3", Name: "S3", Tool: "t3"},
	}

	wf, _ := e.CreateWorkflow("parallel-wf", "", "agent-1", nil, steps, nil)
	run, err := e.StartRun(wf.ID, "agent-1", nil)
	if err != nil {
		t.Fatalf("StartRun failed: %v", err)
	}

	ready, _ := e.GetReadySteps(run.ID)
	// s1 and s3 should be ready (no deps)
	if len(ready) != 2 {
		t.Errorf("expected 2 ready steps, got %d", len(ready))
	}

	// Complete s1
	_ = e.CompleteStep(run.ID, "s1", "completed", nil, "")
	ready, _ = e.GetReadySteps(run.ID)
	// s2 should now be ready, s3 still pending
	readyIDs := make(map[string]bool)
	for _, s := range ready {
		readyIDs[s.ID] = true
	}
	if !readyIDs["s2"] || !readyIDs["s3"] {
		t.Errorf("expected s2 and s3 ready, got %v", readyIDs)
	}
}

func TestCancelRun(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	steps := []DAGStep{{ID: "s1", Name: "S1", Tool: "t1"}}
	wf, _ := e.CreateWorkflow("cancel-test", "", "agent-1", nil, steps, nil)
	run, _ := e.StartRun(wf.ID, "agent-1", nil)

	err := e.CancelRun(run.ID)
	if err != nil {
		t.Fatalf("CancelRun failed: %v", err)
	}

	r, _ := e.GetRun(run.ID)
	if r.Status != "canceled" {
		t.Errorf("expected canceled, got %s", r.Status)
	}
}
