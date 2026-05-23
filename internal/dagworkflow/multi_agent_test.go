package dagworkflow

import (
	"log/slog"
	"testing"
)

func TestMultiAgent_AssignSteps(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	tmpl, _ := e.CreateWorkflow("test", "test workflow", "admin", nil, []DAGStep{
		{ID: "s1", Name: "Step 1", Tool: "tool_a"},
		{ID: "s2", Name: "Step 2", Tool: "tool_b", DependsOn: []string{"s1"}},
	}, nil)
	run, _ := e.StartRun(tmpl.ID, "admin", nil)

	mar, err := e.AssignSteps(run.ID, []AgentAssignment{
		{StepID: "s1", AgentID: "agent-a", AgentRole: "producer"},
		{StepID: "s2", AgentID: "agent-b", AgentRole: "consumer"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mar.Assignments) != 2 {
		t.Errorf("expected 2 assignments, got %d", len(mar.Assignments))
	}
}

func TestMultiAgent_AssignSteps_InvalidStep(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	tmpl, _ := e.CreateWorkflow("test", "", "admin", nil, []DAGStep{
		{ID: "s1", Name: "Step 1", Tool: "tool_a"},
	}, nil)
	run, _ := e.StartRun(tmpl.ID, "admin", nil)

	_, err := e.AssignSteps(run.ID, []AgentAssignment{
		{StepID: "nonexistent", AgentID: "agent-a"},
	})
	if err == nil {
		t.Fatal("expected error for invalid step ID")
	}
}

func TestMultiAgent_RecordHandoff(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	tmpl, _ := e.CreateWorkflow("test", "", "admin", nil, []DAGStep{
		{ID: "s1", Name: "Step 1", Tool: "tool_a"},
		{ID: "s2", Name: "Step 2", Tool: "tool_b", DependsOn: []string{"s1"}},
	}, nil)
	run, _ := e.StartRun(tmpl.ID, "admin", nil)

	handoff, err := e.RecordHandoff(run.ID, "agent-a", "agent-b", "s1", map[string]any{"result": "ok"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if handoff.FromAgent != "agent-a" {
		t.Errorf("expected agent-a, got %s", handoff.FromAgent)
	}
}

func TestMultiAgent_GetAgentSteps(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	tmpl, _ := e.CreateWorkflow("test", "", "admin", nil, []DAGStep{
		{ID: "s1", Name: "Step 1", Tool: "tool_a"},
		{ID: "s2", Name: "Step 2", Tool: "tool_b", DependsOn: []string{"s1"}},
		{ID: "s3", Name: "Step 3", Tool: "tool_c", DependsOn: []string{"s1"}},
	}, nil)
	run, _ := e.StartRun(tmpl.ID, "admin", nil)

	_, _ = e.AssignSteps(run.ID, []AgentAssignment{
		{StepID: "s1", AgentID: "agent-a"},
		{StepID: "s2", AgentID: "agent-a"},
		{StepID: "s3", AgentID: "agent-b"},
	})

	agentASteps, err := e.GetAgentSteps(run.ID, "agent-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(agentASteps) != 2 {
		t.Errorf("expected 2 steps for agent-a, got %d", len(agentASteps))
	}

	agentBSteps, _ := e.GetAgentSteps(run.ID, "agent-b")
	if len(agentBSteps) != 1 {
		t.Errorf("expected 1 step for agent-b, got %d", len(agentBSteps))
	}
}

func TestMultiAgent_GetReadyAgentSteps(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	tmpl, _ := e.CreateWorkflow("test", "", "admin", nil, []DAGStep{
		{ID: "s1", Name: "Step 1", Tool: "tool_a"},
		{ID: "s2", Name: "Step 2", Tool: "tool_b", DependsOn: []string{"s1"}},
	}, nil)
	run, _ := e.StartRun(tmpl.ID, "admin", nil)

	_, _ = e.AssignSteps(run.ID, []AgentAssignment{
		{StepID: "s1", AgentID: "agent-a"},
		{StepID: "s2", AgentID: "agent-b"},
	})

	// Initially only s1 is ready (no deps)
	ready, _ := e.GetReadyAgentSteps(run.ID, "agent-a")
	if len(ready) != 1 {
		t.Errorf("expected 1 ready step for agent-a, got %d", len(ready))
	}

	readyB, _ := e.GetReadyAgentSteps(run.ID, "agent-b")
	if len(readyB) != 0 {
		t.Errorf("expected 0 ready steps for agent-b, got %d", len(readyB))
	}

	// Complete s1, now s2 should be ready for agent-b
	_ = e.CompleteStep(run.ID, "s1", "completed", nil, "")
	readyB2, _ := e.GetReadyAgentSteps(run.ID, "agent-b")
	if len(readyB2) != 1 {
		t.Errorf("expected 1 ready step for agent-b after s1 completion, got %d", len(readyB2))
	}
}

func TestMultiAgent_GetStatus(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	tmpl, _ := e.CreateWorkflow("test", "", "admin", nil, []DAGStep{
		{ID: "s1", Name: "Step 1", Tool: "tool_a"},
	}, nil)
	run, _ := e.StartRun(tmpl.ID, "admin", nil)

	_, _ = e.AssignSteps(run.ID, []AgentAssignment{{StepID: "s1", AgentID: "agent-a"}})

	mar, err := e.GetMultiAgentStatus(run.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mar.AgentStatus["agent-a"] != "idle" {
		t.Errorf("expected idle, got %s", mar.AgentStatus["agent-a"])
	}
}
