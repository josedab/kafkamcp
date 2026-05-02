package smartroute

import (
	"log/slog"
	"testing"
)

func TestClassifyAndPlan(t *testing.T) {
	t.Parallel()
	r := NewRouter(slog.Default())

	tests := []struct {
		input    string
		intentID string
	}{
		{"why are orders delayed?", "investigate_delay"},
		{"investigate the outage in production", "incident_investigation"},
		{"what's our capacity forecast?", "capacity_analysis"},
		{"check schema compatibility", "schema_investigation"},
		{"explore the cluster topics", "topic_exploration"},
		{"optimize our Kafka costs", "cost_investigation"},
	}

	for _, tt := range tests {
		plan, err := r.ClassifyAndPlan(tt.input)
		if err != nil {
			t.Fatalf("input=%q: unexpected error: %v", tt.input, err)
		}
		if plan.Intent != tt.intentID {
			t.Errorf("input=%q: expected intent %s, got %s", tt.input, tt.intentID, plan.Intent)
		}
		if len(plan.Steps) == 0 {
			t.Errorf("input=%q: expected steps, got none", tt.input)
		}
	}
}

func TestGetNextSteps(t *testing.T) {
	t.Parallel()
	r := NewRouter(slog.Default())

	plan, err := r.ClassifyAndPlan("why are orders delayed?")
	if err != nil {
		t.Fatalf("ClassifyAndPlan failed: %v", err)
	}

	// First, only step s1 should be ready (no dependencies)
	steps, err := r.GetNextSteps(plan.ID)
	if err != nil {
		t.Fatalf("GetNextSteps failed: %v", err)
	}
	if len(steps) != 1 || steps[0].ID != "s1" {
		t.Fatalf("expected only s1 ready, got %v", steps)
	}

	// Complete s1
	err = r.RecordStepResult(plan.ID, StepResult{StepID: "s1", Tool: "kafka_consumer_groups", Status: "completed"})
	if err != nil {
		t.Fatalf("RecordStepResult failed: %v", err)
	}

	// Now s2 and s3 should be ready
	steps, err = r.GetNextSteps(plan.ID)
	if err != nil {
		t.Fatalf("GetNextSteps failed: %v", err)
	}
	if len(steps) != 2 {
		t.Fatalf("expected 2 ready steps, got %d", len(steps))
	}
}

func TestNoMatch(t *testing.T) {
	t.Parallel()
	r := NewRouter(slog.Default())
	_, err := r.ClassifyAndPlan("")
	if err == nil {
		t.Fatal("expected error for empty request")
	}
}

func TestRecordStepResultCompletion(t *testing.T) {
	t.Parallel()
	r := NewRouter(slog.Default())

	plan, _ := r.ClassifyAndPlan("explore cluster topics")
	for _, step := range plan.Steps {
		r.RecordStepResult(plan.ID, StepResult{StepID: step.ID, Tool: step.Tool, Status: "completed"})
	}

	result, err := r.GetResult(plan.ID)
	if err != nil {
		t.Fatalf("GetResult failed: %v", err)
	}
	if result.Status != "completed" {
		t.Errorf("expected completed, got %s", result.Status)
	}

	p, _ := r.GetPlan(plan.ID)
	if p.Status != "completed" {
		t.Errorf("plan status expected completed, got %s", p.Status)
	}
}
