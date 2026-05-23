package migration

import (
	"testing"
)

// --- ExecutePlan ---

func TestExecutePlanDryRun(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	plan := p.CreatePlan("src", "tgt",
		[]TopicInfo{{Name: "t1", Partitions: 3, ReplicationFactor: 1}},
		[]TopicInfo{},
	)

	result, err := p.ExecutePlan(plan.ID, true)
	if err != nil {
		t.Fatalf("ExecutePlan dry-run failed: %v", err)
	}
	if !result.DryRun {
		t.Error("expected DryRun=true")
	}
	if result.Succeeded != 1 {
		t.Errorf("expected 1 succeeded, got %d", result.Succeeded)
	}
	// Plan should remain pending after dry-run
	got, _ := p.GetPlan(plan.ID)
	if got.Status != "pending" {
		t.Errorf("expected pending after dry-run, got %s", got.Status)
	}
}

func TestExecutePlanActual(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	plan := p.CreatePlan("src", "tgt",
		[]TopicInfo{{Name: "t1", Partitions: 3, ReplicationFactor: 1}},
		[]TopicInfo{},
	)

	result, err := p.ExecutePlan(plan.ID, false)
	if err != nil {
		t.Fatalf("ExecutePlan failed: %v", err)
	}
	if result.DryRun {
		t.Error("expected DryRun=false")
	}
	got, _ := p.GetPlan(plan.ID)
	if got.Status != "completed" {
		t.Errorf("expected completed, got %s", got.Status)
	}
}

func TestExecutePlanAlreadyCompleted(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	plan := p.CreatePlan("src", "tgt",
		[]TopicInfo{{Name: "t1", Partitions: 3, ReplicationFactor: 1}},
		[]TopicInfo{},
	)
	_, _ = p.ExecutePlan(plan.ID, false)

	_, err := p.ExecutePlan(plan.ID, false)
	if err == nil {
		t.Error("expected error executing already completed plan")
	}
}

func TestExecutePlanNotFound(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	_, err := p.ExecutePlan("nonexistent", false)
	if err == nil {
		t.Error("expected error for nonexistent plan")
	}
}

func TestExecutePlanWithConsumerGroups(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	plan := p.CreatePlan("src", "tgt", []TopicInfo{}, []TopicInfo{})
	plan.ConsumerGroups = []GroupMigration{
		{GroupID: "g1", Topics: []string{"t1"}, Action: "recreate", OffsetStrategy: "earliest"},
		{GroupID: "g2", Topics: []string{"t2"}, Action: "skip"},
	}

	result, err := p.ExecutePlan(plan.ID, false)
	if err != nil {
		t.Fatalf("ExecutePlan failed: %v", err)
	}
	if result.Executed != 1 {
		t.Errorf("expected 1 executed (skip excluded), got %d", result.Executed)
	}
}

func TestExecutePlanWithSchemas(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	plan := p.CreatePlan("src", "tgt", []TopicInfo{}, []TopicInfo{})
	plan.SchemaSubjects = []SchemaMigration{
		{Subject: "orders-value", Versions: 3, Compatible: true, Action: "migrate"},
		{Subject: "users-value", Versions: 1, Compatible: true, Action: "skip"},
	}

	result, err := p.ExecutePlan(plan.ID, false)
	if err != nil {
		t.Fatalf("ExecutePlan failed: %v", err)
	}
	if result.Executed != 1 {
		t.Errorf("expected 1 executed, got %d", result.Executed)
	}
}

// --- Rollback ---

func TestRollbackCompleted(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	plan := p.CreatePlan("src", "tgt",
		[]TopicInfo{{Name: "t1", Partitions: 3, ReplicationFactor: 1}},
		[]TopicInfo{},
	)
	_, _ = p.ExecutePlan(plan.ID, false)

	record, err := p.Rollback(plan.ID)
	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}
	if len(record.TopicsCreated) != 1 {
		t.Errorf("expected 1 topic created in rollback record, got %d", len(record.TopicsCreated))
	}
	got, _ := p.GetPlan(plan.ID)
	if got.Status != "rolled_back" {
		t.Errorf("expected rolled_back, got %s", got.Status)
	}
}

func TestRollbackPendingError(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	plan := p.CreatePlan("src", "tgt",
		[]TopicInfo{{Name: "t1", Partitions: 3, ReplicationFactor: 1}},
		[]TopicInfo{},
	)
	// Execute dry-run only, plan stays pending
	_, _ = p.ExecutePlan(plan.ID, true)

	_, err := p.Rollback(plan.ID)
	if err == nil {
		t.Error("expected error rolling back pending plan")
	}
}

func TestRollbackAlreadyRolledBack(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	plan := p.CreatePlan("src", "tgt",
		[]TopicInfo{{Name: "t1", Partitions: 3, ReplicationFactor: 1}},
		[]TopicInfo{},
	)
	_, _ = p.ExecutePlan(plan.ID, false)
	_, _ = p.Rollback(plan.ID)

	_, err := p.Rollback(plan.ID)
	if err == nil {
		t.Error("expected error rolling back already rolled-back plan")
	}
}

func TestRollbackNoRecord(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	_, err := p.Rollback("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent rollback")
	}
}

// --- MapOffsets ---

func TestMapOffsetsEarliest(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	plan := p.CreatePlan("src", "tgt", []TopicInfo{{Name: "t1", Partitions: 1, ReplicationFactor: 1}}, nil)

	offsets := map[int32]int64{0: 100, 1: 200}
	mapping, err := p.MapOffsets(plan.ID, "t1", offsets, "earliest")
	if err != nil {
		t.Fatalf("MapOffsets failed: %v", err)
	}
	for _, v := range mapping.PartitionMap {
		if v != 0 {
			t.Errorf("earliest strategy should map to 0, got %d", v)
		}
	}
}

func TestMapOffsetsLatest(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	plan := p.CreatePlan("src", "tgt", []TopicInfo{{Name: "t1", Partitions: 1, ReplicationFactor: 1}}, nil)

	offsets := map[int32]int64{0: 100}
	mapping, err := p.MapOffsets(plan.ID, "t1", offsets, "latest")
	if err != nil {
		t.Fatalf("MapOffsets failed: %v", err)
	}
	if mapping.PartitionMap[0] != -1 {
		t.Errorf("latest strategy should map to -1, got %d", mapping.PartitionMap[0])
	}
}

func TestMapOffsetsTranslate(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	plan := p.CreatePlan("src", "tgt", []TopicInfo{{Name: "t1", Partitions: 1, ReplicationFactor: 1}}, nil)

	offsets := map[int32]int64{0: 42, 1: 99}
	mapping, err := p.MapOffsets(plan.ID, "t1", offsets, "translate")
	if err != nil {
		t.Fatalf("MapOffsets failed: %v", err)
	}
	if mapping.PartitionMap[0] != 42 || mapping.PartitionMap[1] != 99 {
		t.Errorf("translate should preserve offsets, got %v", mapping.PartitionMap)
	}
}

func TestMapOffsetsUnknownStrategy(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	plan := p.CreatePlan("src", "tgt", []TopicInfo{{Name: "t1", Partitions: 1, ReplicationFactor: 1}}, nil)

	_, err := p.MapOffsets(plan.ID, "t1", map[int32]int64{0: 10}, "bogus")
	if err == nil {
		t.Error("expected error for unknown strategy")
	}
}

func TestMapOffsetsNonexistentPlan(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	_, err := p.MapOffsets("nonexistent", "t1", map[int32]int64{0: 1}, "earliest")
	if err == nil {
		t.Error("expected error for nonexistent plan")
	}
}

func TestMapOffsetsEmptyOffsets(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	plan := p.CreatePlan("src", "tgt", []TopicInfo{{Name: "t1", Partitions: 1, ReplicationFactor: 1}}, nil)

	mapping, err := p.MapOffsets(plan.ID, "t1", map[int32]int64{}, "earliest")
	if err != nil {
		t.Fatalf("MapOffsets failed: %v", err)
	}
	if len(mapping.PartitionMap) != 0 {
		t.Errorf("expected empty partition map, got %v", mapping.PartitionMap)
	}
}

// --- GetProgress ---

func TestGetProgressValid(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	plan := p.CreatePlan("src", "tgt",
		[]TopicInfo{{Name: "t1", Partitions: 3, ReplicationFactor: 1}},
		[]TopicInfo{},
	)
	_, _ = p.ExecutePlan(plan.ID, false)

	progress, err := p.GetProgress(plan.ID)
	if err != nil {
		t.Fatalf("GetProgress failed: %v", err)
	}
	if progress.Status != "completed" {
		t.Errorf("expected completed, got %s", progress.Status)
	}
}

func TestGetProgressNotFound(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	_, err := p.GetProgress("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent progress")
	}
}

// --- GetReport ---

func TestGetReportValid(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	plan := p.CreatePlan("src", "tgt",
		[]TopicInfo{{Name: "t1", Partitions: 3, ReplicationFactor: 1}},
		[]TopicInfo{},
	)
	_, _ = p.ExecutePlan(plan.ID, false)

	report, err := p.GetReport(plan.ID)
	if err != nil {
		t.Fatalf("GetReport failed: %v", err)
	}
	if report.TopicsMigrated != 1 {
		t.Errorf("expected 1 topic migrated, got %d", report.TopicsMigrated)
	}
}

func TestGetReportNotFound(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	_, err := p.GetReport("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent report")
	}
}

// --- countNonSkipped helpers ---

func TestCountNonSkippedEmpty(t *testing.T) {
	t.Parallel()
	if countNonSkipped(nil) != 0 {
		t.Error("expected 0 for nil topics")
	}
}

func TestCountNonSkippedAllSkipped(t *testing.T) {
	t.Parallel()
	topics := []TopicMigration{
		{Action: "skip"}, {Action: "skip"},
	}
	if countNonSkipped(topics) != 0 {
		t.Error("expected 0 when all skipped")
	}
}

func TestCountNonSkippedMixed(t *testing.T) {
	t.Parallel()
	topics := []TopicMigration{
		{Action: "create"}, {Action: "skip"}, {Action: "update"},
	}
	if countNonSkipped(topics) != 2 {
		t.Errorf("expected 2, got %d", countNonSkipped(topics))
	}
}

func TestCountNonSkippedGroupsEmpty(t *testing.T) {
	t.Parallel()
	if countNonSkippedGroups(nil) != 0 {
		t.Error("expected 0")
	}
}

func TestCountNonSkippedGroupsMixed(t *testing.T) {
	t.Parallel()
	groups := []GroupMigration{
		{Action: "recreate"}, {Action: "skip"},
	}
	if countNonSkippedGroups(groups) != 1 {
		t.Errorf("expected 1, got %d", countNonSkippedGroups(groups))
	}
}

func TestCountNonSkippedSchemasEmpty(t *testing.T) {
	t.Parallel()
	if countNonSkippedSchemas(nil) != 0 {
		t.Error("expected 0")
	}
}

func TestCountNonSkippedSchemasMixed(t *testing.T) {
	t.Parallel()
	schemas := []SchemaMigration{
		{Action: "migrate"}, {Action: "skip"}, {Action: "migrate"},
	}
	if countNonSkippedSchemas(schemas) != 2 {
		t.Errorf("expected 2, got %d", countNonSkippedSchemas(schemas))
	}
}

// --- Edge cases ---

func TestExecutePlanSkippedTopics(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	// Create plan where source and target match → skip
	plan := p.CreatePlan("src", "tgt",
		[]TopicInfo{{Name: "t1", Partitions: 3, ReplicationFactor: 1}},
		[]TopicInfo{{Name: "t1", Partitions: 3, ReplicationFactor: 1}},
	)

	result, err := p.ExecutePlan(plan.ID, false)
	if err != nil {
		t.Fatalf("ExecutePlan failed: %v", err)
	}
	if result.Executed != 0 {
		t.Errorf("expected 0 executed (all skipped), got %d", result.Executed)
	}
}

func TestNegativePartitionWarning(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	plan := p.CreatePlan("src", "tgt",
		[]TopicInfo{{Name: "bad", Partitions: -1, ReplicationFactor: 0}},
		[]TopicInfo{},
	)
	if len(plan.Warnings) == 0 {
		t.Error("expected warnings for invalid partition/replication")
	}
}

func TestNilPlan(t *testing.T) {
	t.Parallel()
	warnings := ValidatePlan(&MigrationPlan{})
	if len(warnings) < 2 {
		t.Error("expected warnings for empty source and target clusters")
	}
}
