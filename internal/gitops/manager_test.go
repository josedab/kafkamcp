package gitops

import (
	"testing"
)

func TestValidateDesiredState(t *testing.T) {
	m := NewManager(nil)

	// Valid state
	errs := m.ValidateDesiredState(DesiredState{
		Version: "1",
		Topics: []TopicSpec{
			{Name: "orders", Partitions: 6, ReplicationFactor: 3},
		},
	})
	if len(errs) != 0 {
		t.Errorf("expected no errors, got %v", errs)
	}

	// Invalid state
	errs = m.ValidateDesiredState(DesiredState{
		Topics: []TopicSpec{
			{Name: "", Partitions: 0, ReplicationFactor: 0},
			{Name: "dup", Partitions: 1, ReplicationFactor: 1},
			{Name: "dup", Partitions: 1, ReplicationFactor: 1},
		},
	})
	if len(errs) < 3 {
		t.Errorf("expected at least 3 errors, got %d: %v", len(errs), errs)
	}
}

func TestDiff(t *testing.T) {
	m := NewManager(nil)

	desired := DesiredState{
		Version: "1",
		Topics: []TopicSpec{
			{Name: "orders", Partitions: 12, ReplicationFactor: 3, Configs: map[string]string{"retention.ms": "604800000"}},
			{Name: "new-topic", Partitions: 6, ReplicationFactor: 3},
		},
	}

	live := LiveState{
		Topics: map[string]TopicSpec{
			"orders":      {Name: "orders", Partitions: 6, ReplicationFactor: 3, Configs: map[string]string{"retention.ms": "86400000"}},
			"old-topic":   {Name: "old-topic", Partitions: 3, ReplicationFactor: 3},
		},
	}

	diff, err := m.Diff(desired, live)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(diff.TopicsToCreate) != 1 {
		t.Errorf("expected 1 topic to create, got %d", len(diff.TopicsToCreate))
	}
	if len(diff.TopicsToUpdate) != 1 {
		t.Errorf("expected 1 topic to update, got %d", len(diff.TopicsToUpdate))
	}
	if len(diff.TopicsToDelete) != 1 {
		t.Errorf("expected 1 topic to delete, got %d", len(diff.TopicsToDelete))
	}
}

func TestGeneratePlan(t *testing.T) {
	m := NewManager(nil)

	diff := &DiffResult{
		TopicsToCreate: []TopicSpec{{Name: "new-topic", Partitions: 6, ReplicationFactor: 3}},
		TopicsToUpdate: []TopicDiff{{Name: "orders", ConfigChanges: map[string]Change{"retention.ms": {From: "86400000", To: "604800000"}}}},
	}

	plan := m.GeneratePlan(diff, true)
	if len(plan.Steps) != 2 {
		t.Errorf("expected 2 steps, got %d", len(plan.Steps))
	}
	if !plan.DryRun {
		t.Error("expected dry run")
	}
	if plan.Steps[0].Action != "create_topic" {
		t.Errorf("expected create_topic first, got %s", plan.Steps[0].Action)
	}
}
