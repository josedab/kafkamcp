package runbook

import (
	"log/slog"
	"testing"
)

func TestParseAgenticRunbookYAML(t *testing.T) {
	yamlDef := `
name: "Test Rebalance"
description: "Rebalance partitions across brokers"
category: "operations"
version: "1.0"
params:
  - name: topic
    required: true
    description: "Topic to rebalance"
pre_conditions:
  - name: "cluster_healthy"
    tool: "kafka_health_score"
    check: "score > 80"
    required: true
blast_radius:
  max_topics: 5
  max_partitions: 100
  allowed_clusters:
    - "production"
steps:
  - name: "Check health"
    tool: "kafka_health_score"
    description: "Verify cluster is healthy"
  - name: "Plan reassignment"
    tool: "kafka_cluster_info"
    args:
      action: "reassignment_plan"
    approval_gate:
      required: true
      message: "Approve reassignment plan?"
      timeout: "1h"
    on_failure: rollback
  - name: "Execute reassignment"
    tool: "kafka_cluster_info"
    args:
      action: "execute"
      topic: "${topic}"
    checkpoint: true
rollback:
  - name: "Revert assignments"
    tool: "kafka_cluster_info"
    args:
      action: "revert"
`
	rb, err := ParseAgenticRunbookYAML(yamlDef)
	if err != nil {
		t.Fatalf("ParseAgenticRunbookYAML: %v", err)
	}
	if rb.Name != "Test Rebalance" {
		t.Errorf("Name = %q, want %q", rb.Name, "Test Rebalance")
	}
	if len(rb.Steps) != 3 {
		t.Errorf("Steps = %d, want 3", len(rb.Steps))
	}
	if rb.Steps[1].ApprovalGate == nil {
		t.Error("Step 2 should have approval gate")
	}
	if rb.BlastRadius.MaxTopics != 5 {
		t.Errorf("MaxTopics = %d, want 5", rb.BlastRadius.MaxTopics)
	}
	if len(rb.Rollback) != 1 {
		t.Errorf("Rollback steps = %d, want 1", len(rb.Rollback))
	}
}

func TestParseAgenticRunbookYAML_Invalid(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{"empty name", `steps: [{name: "x", tool: "y"}]`},
		{"no steps", `name: "test"`},
		{"invalid on_failure", `name: "test"
steps:
  - name: "x"
    tool: "y"
    on_failure: "invalid"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseAgenticRunbookYAML(tt.yaml)
			if err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestAgenticExecution_DryRun(t *testing.T) {
	e := NewEngine(slog.Default())

	yamlDef := `
name: "DryRun Test"
category: "test"
steps:
  - name: "Step 1"
    tool: "kafka_list_topics"
  - name: "Step 2"
    tool: "kafka_describe_topic"
`
	rb, err := e.CreateAgenticRunbook(yamlDef, "tester")
	if err != nil {
		t.Fatalf("CreateAgenticRunbook: %v", err)
	}

	exec, err := e.ExecuteAgentic(rb.ID, "tester", nil, true)
	if err != nil {
		t.Fatalf("ExecuteAgentic: %v", err)
	}
	if exec.Status != "dry_run_complete" {
		t.Errorf("Status = %q, want dry_run_complete", exec.Status)
	}
	if !exec.DryRun {
		t.Error("DryRun should be true")
	}
	if len(exec.Steps) != 2 {
		t.Errorf("Steps = %d, want 2", len(exec.Steps))
	}
	for _, s := range exec.Steps {
		if s.Status != "dry_run" {
			t.Errorf("Step status = %q, want dry_run", s.Status)
		}
	}
}

func TestAgenticExecution_ApprovalGate(t *testing.T) {
	e := NewEngine(slog.Default())

	yamlDef := `
name: "Approval Test"
steps:
  - name: "Step 1"
    tool: "kafka_list_topics"
    approval_gate:
      required: true
      message: "Approve step 1?"
      timeout: "30m"
`
	rb, err := e.CreateAgenticRunbook(yamlDef, "tester")
	if err != nil {
		t.Fatalf("CreateAgenticRunbook: %v", err)
	}

	exec, err := e.ExecuteAgentic(rb.ID, "tester", nil, false)
	if err != nil {
		t.Fatalf("ExecuteAgentic: %v", err)
	}
	if exec.Status != "paused" {
		t.Errorf("Status = %q, want paused", exec.Status)
	}
	if exec.PendingApproval == nil {
		t.Fatal("expected pending approval")
	}

	// Approve
	if err := e.ApproveStep(exec.ID, "admin"); err != nil {
		t.Fatalf("ApproveStep: %v", err)
	}

	exec2, _ := e.GetAgenticExecution(exec.ID)
	if exec2.Status != "running" {
		t.Errorf("Status after approval = %q, want running", exec2.Status)
	}
	if exec2.PendingApproval != nil {
		t.Error("pending approval should be nil after approval")
	}
}

func TestAgenticExecution_Reject(t *testing.T) {
	e := NewEngine(slog.Default())

	yamlDef := `
name: "Reject Test"
steps:
  - name: "Step 1"
    tool: "kafka_list_topics"
    approval_gate:
      required: true
      message: "Approve?"
`
	rb, err := e.CreateAgenticRunbook(yamlDef, "tester")
	if err != nil {
		t.Fatalf("CreateAgenticRunbook: %v", err)
	}

	exec, err := e.ExecuteAgentic(rb.ID, "tester", nil, false)
	if err != nil {
		t.Fatalf("ExecuteAgentic: %v", err)
	}

	if err := e.RejectStep(exec.ID, "admin", "too risky"); err != nil {
		t.Fatalf("RejectStep: %v", err)
	}

	exec2, _ := e.GetAgenticExecution(exec.ID)
	if exec2.Status != "failed" {
		t.Errorf("Status after reject = %q, want failed", exec2.Status)
	}
}

func TestCheckBlastRadius(t *testing.T) {
	e := NewEngine(slog.Default())

	yamlDef := `
name: "Blast Test"
blast_radius:
  max_topics: 3
  max_partitions: 10
  allowed_clusters:
    - "dev"
steps:
  - name: "Step 1"
    tool: "kafka_list_topics"
`
	rb, err := e.CreateAgenticRunbook(yamlDef, "tester")
	if err != nil {
		t.Fatalf("CreateAgenticRunbook: %v", err)
	}

	if err := e.CheckBlastRadius(rb.ID, 2, 5, 1, "dev"); err != nil {
		t.Errorf("expected no error for within-limits: %v", err)
	}
	if err := e.CheckBlastRadius(rb.ID, 5, 5, 1, "dev"); err == nil {
		t.Error("expected error for exceeding max_topics")
	}
	if err := e.CheckBlastRadius(rb.ID, 1, 1, 1, "production"); err == nil {
		t.Error("expected error for non-allowed cluster")
	}
}

func TestInterpolateArgs(t *testing.T) {
	args := map[string]string{
		"topic": "${topic_name}",
		"group": "${group_id}",
	}
	params := map[string]string{
		"topic_name": "my-topic",
		"group_id":   "my-group",
	}
	result := interpolateArgs(args, params)
	if result["topic"] != "my-topic" {
		t.Errorf("topic = %q, want my-topic", result["topic"])
	}
	if result["group"] != "my-group" {
		t.Errorf("group = %q, want my-group", result["group"])
	}
}
