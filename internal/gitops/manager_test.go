package gitops

import (
	"testing"
)

func TestValidateDesiredState(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
			"orders":    {Name: "orders", Partitions: 6, ReplicationFactor: 3, Configs: map[string]string{"retention.ms": "86400000"}},
			"old-topic": {Name: "old-topic", Partitions: 3, ReplicationFactor: 3},
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
	t.Parallel()
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

func TestValidateDesiredState_MissingTopicName(t *testing.T) {
	t.Parallel()
	m := NewManager(nil)
	errs := m.ValidateDesiredState(DesiredState{
		Version: "1",
		Topics:  []TopicSpec{{Name: "", Partitions: 3, ReplicationFactor: 1}},
	})
	found := false
	for _, e := range errs {
		if e == "topics[0]: name is required" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'name is required' error, got %v", errs)
	}
}

func TestValidateDesiredState_MissingPartitions(t *testing.T) {
	t.Parallel()
	m := NewManager(nil)
	errs := m.ValidateDesiredState(DesiredState{
		Version: "1",
		Topics:  []TopicSpec{{Name: "test", Partitions: 0, ReplicationFactor: 1}},
	})
	found := false
	for _, e := range errs {
		if contains(e, "partitions must be >= 1") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected partitions error, got %v", errs)
	}
}

func TestValidateDesiredState_NegativePartitions(t *testing.T) {
	t.Parallel()
	m := NewManager(nil)
	errs := m.ValidateDesiredState(DesiredState{
		Version: "1",
		Topics:  []TopicSpec{{Name: "test", Partitions: -5, ReplicationFactor: 1}},
	})
	found := false
	for _, e := range errs {
		if contains(e, "partitions must be >= 1") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected partitions error for negative value, got %v", errs)
	}
}

func TestValidateDesiredState_ZeroReplication(t *testing.T) {
	t.Parallel()
	m := NewManager(nil)
	errs := m.ValidateDesiredState(DesiredState{
		Version: "1",
		Topics:  []TopicSpec{{Name: "test", Partitions: 1, ReplicationFactor: 0}},
	})
	found := false
	for _, e := range errs {
		if contains(e, "replication_factor must be >= 1") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected replication_factor error, got %v", errs)
	}
}

func TestValidateDesiredState_DuplicateTopicNames(t *testing.T) {
	t.Parallel()
	m := NewManager(nil)
	errs := m.ValidateDesiredState(DesiredState{
		Version: "1",
		Topics: []TopicSpec{
			{Name: "orders", Partitions: 3, ReplicationFactor: 1},
			{Name: "orders", Partitions: 6, ReplicationFactor: 1},
		},
	})
	found := false
	for _, e := range errs {
		if contains(e, "duplicate topic name") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected duplicate topic name error, got %v", errs)
	}
}

func TestValidateDesiredState_EmptyState(t *testing.T) {
	t.Parallel()
	m := NewManager(nil)
	errs := m.ValidateDesiredState(DesiredState{Version: "1"})
	if len(errs) != 0 {
		t.Errorf("expected no errors for empty state with version, got %v", errs)
	}
}

func TestValidateDesiredState_MissingVersion(t *testing.T) {
	t.Parallel()
	m := NewManager(nil)
	errs := m.ValidateDesiredState(DesiredState{
		Topics: []TopicSpec{{Name: "test", Partitions: 1, ReplicationFactor: 1}},
	})
	found := false
	for _, e := range errs {
		if e == "version is required" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'version is required' error, got %v", errs)
	}
}

func TestValidateDesiredState_UnicodeTopicNames(t *testing.T) {
	t.Parallel()
	m := NewManager(nil)
	errs := m.ValidateDesiredState(DesiredState{
		Version: "1",
		Topics: []TopicSpec{
			{Name: "日本語トピック", Partitions: 1, ReplicationFactor: 1},
		},
	})
	// Should not error on name validation (Kafka supports unicode)
	for _, e := range errs {
		if contains(e, "name") {
			t.Errorf("unicode topic name should be accepted, got error: %s", e)
		}
	}
}

func TestValidateDesiredState_InvalidSchemaType(t *testing.T) {
	t.Parallel()
	m := NewManager(nil)
	errs := m.ValidateDesiredState(DesiredState{
		Version: "1",
		Schemas: []SchemaSpec{
			{Subject: "test-value", Schema: "{}", SchemaType: "INVALID"},
		},
	})
	found := false
	for _, e := range errs {
		if contains(e, "invalid schema_type") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected invalid schema_type error, got %v", errs)
	}
}

func TestValidateDesiredState_MissingSchemaFields(t *testing.T) {
	t.Parallel()
	m := NewManager(nil)
	errs := m.ValidateDesiredState(DesiredState{
		Version: "1",
		Schemas: []SchemaSpec{
			{Subject: "", Schema: ""},
		},
	})
	if len(errs) < 2 {
		t.Errorf("expected at least 2 errors for missing subject and schema, got %d: %v", len(errs), errs)
	}
}

func TestValidateDesiredState_ValidSchemaTypes(t *testing.T) {
	t.Parallel()
	m := NewManager(nil)
	for _, st := range []string{"AVRO", "PROTOBUF", "JSON"} {
		errs := m.ValidateDesiredState(DesiredState{
			Version: "1",
			Schemas: []SchemaSpec{
				{Subject: "test-value", Schema: "{}", SchemaType: st},
			},
		})
		for _, e := range errs {
			if contains(e, "invalid schema_type") {
				t.Errorf("schema type %q should be valid, got error: %s", st, e)
			}
		}
	}
}

func TestValidateDesiredState_MultipleErrors(t *testing.T) {
	t.Parallel()
	m := NewManager(nil)
	errs := m.ValidateDesiredState(DesiredState{
		// Missing version
		Topics: []TopicSpec{
			{Name: "", Partitions: -1, ReplicationFactor: 0},  // 3 errors
			{Name: "ok", Partitions: 0, ReplicationFactor: 0}, // 2 errors
		},
		Schemas: []SchemaSpec{
			{Subject: "", Schema: ""}, // 2 errors
		},
	})
	// version(1) + name(1) + partitions(2) + replication(2) + subject(1) + schema(1) = 8
	if len(errs) < 7 {
		t.Errorf("expected at least 7 errors, got %d: %v", len(errs), errs)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && containsHelper(s, substr)
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
