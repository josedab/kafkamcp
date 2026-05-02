package migration

import (
	"testing"
)

func TestNewPlanner(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	if p == nil {
		t.Fatal("expected non-nil planner")
	}
	if len(p.ListPlans()) != 0 {
		t.Fatal("expected empty plan list")
	}
}

func TestCreatePlan_NewTopics(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	source := []TopicInfo{
		{Name: "orders", Partitions: 6, ReplicationFactor: 3, Configs: map[string]string{"retention.ms": "86400000"}},
	}

	plan := p.CreatePlan("cluster-a", "cluster-b", source, nil)

	if plan.SourceCluster != "cluster-a" {
		t.Errorf("expected source cluster-a, got %s", plan.SourceCluster)
	}
	if plan.TargetCluster != "cluster-b" {
		t.Errorf("expected target cluster-b, got %s", plan.TargetCluster)
	}
	if plan.Status != "pending" {
		t.Errorf("expected status pending, got %s", plan.Status)
	}
	if len(plan.Topics) != 1 {
		t.Fatalf("expected 1 topic migration, got %d", len(plan.Topics))
	}
	if plan.Topics[0].Action != "create" {
		t.Errorf("expected action create, got %s", plan.Topics[0].Action)
	}
}

func TestCreatePlan_SkipMatchingTopics(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	topics := []TopicInfo{
		{Name: "events", Partitions: 3, ReplicationFactor: 2, Configs: map[string]string{"cleanup.policy": "compact"}},
	}

	plan := p.CreatePlan("src", "tgt", topics, topics)

	if len(plan.Topics) != 1 {
		t.Fatalf("expected 1 topic migration, got %d", len(plan.Topics))
	}
	if plan.Topics[0].Action != "skip" {
		t.Errorf("expected action skip, got %s", plan.Topics[0].Action)
	}
}

func TestCreatePlan_UpdateDifferingTopics(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	source := []TopicInfo{
		{Name: "logs", Partitions: 12, ReplicationFactor: 3, Configs: map[string]string{"retention.ms": "604800000"}},
	}
	target := []TopicInfo{
		{Name: "logs", Partitions: 6, ReplicationFactor: 3, Configs: map[string]string{"retention.ms": "604800000"}},
	}

	plan := p.CreatePlan("src", "tgt", source, target)

	if len(plan.Topics) != 1 {
		t.Fatalf("expected 1 topic migration, got %d", len(plan.Topics))
	}
	if plan.Topics[0].Action != "update" {
		t.Errorf("expected action update, got %s", plan.Topics[0].Action)
	}
}

func TestCreatePlan_ConfigDiff(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	source := []TopicInfo{
		{Name: "metrics", Partitions: 3, ReplicationFactor: 1, Configs: map[string]string{"retention.ms": "3600000"}},
	}
	target := []TopicInfo{
		{Name: "metrics", Partitions: 3, ReplicationFactor: 1, Configs: map[string]string{"retention.ms": "7200000"}},
	}

	plan := p.CreatePlan("src", "tgt", source, target)

	if plan.Topics[0].Action != "update" {
		t.Errorf("expected action update for config diff, got %s", plan.Topics[0].Action)
	}
}

func TestCreatePlan_MultipleTopics(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	source := []TopicInfo{
		{Name: "new-topic", Partitions: 4, ReplicationFactor: 2},
		{Name: "existing", Partitions: 3, ReplicationFactor: 1},
		{Name: "changed", Partitions: 8, ReplicationFactor: 3},
	}
	target := []TopicInfo{
		{Name: "existing", Partitions: 3, ReplicationFactor: 1},
		{Name: "changed", Partitions: 4, ReplicationFactor: 3},
	}

	plan := p.CreatePlan("src", "tgt", source, target)

	if len(plan.Topics) != 3 {
		t.Fatalf("expected 3 topic migrations, got %d", len(plan.Topics))
	}

	actions := make(map[string]string)
	for _, tm := range plan.Topics {
		actions[tm.Name] = tm.Action
	}
	if actions["new-topic"] != "create" {
		t.Errorf("expected new-topic action create, got %s", actions["new-topic"])
	}
	if actions["existing"] != "skip" {
		t.Errorf("expected existing action skip, got %s", actions["existing"])
	}
	if actions["changed"] != "update" {
		t.Errorf("expected changed action update, got %s", actions["changed"])
	}
}

func TestGetPlan(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	plan := p.CreatePlan("a", "b", nil, nil)

	got, err := p.GetPlan(plan.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.ID != plan.ID {
		t.Errorf("expected plan ID %s, got %s", plan.ID, got.ID)
	}
}

func TestGetPlan_NotFound(t *testing.T) {
	t.Parallel()
	p := NewPlanner()

	_, err := p.GetPlan("nonexistent")
	if err == nil {
		t.Fatal("expected error for missing plan")
	}
}

func TestListPlans(t *testing.T) {
	t.Parallel()
	p := NewPlanner()
	p.CreatePlan("a", "b", nil, nil)
	p.CreatePlan("c", "d", nil, nil)

	plans := p.ListPlans()
	if len(plans) != 2 {
		t.Errorf("expected 2 plans, got %d", len(plans))
	}
}

func TestValidatePlan_EmptySource(t *testing.T) {
	t.Parallel()
	warnings := ValidatePlan(&MigrationPlan{TargetCluster: "tgt"})

	found := false
	for _, w := range warnings {
		if w == "source cluster is empty" {
			found = true
		}
	}
	if !found {
		t.Error("expected warning about empty source cluster")
	}
}

func TestValidatePlan_SameCluster(t *testing.T) {
	t.Parallel()
	warnings := ValidatePlan(&MigrationPlan{SourceCluster: "same", TargetCluster: "same"})

	found := false
	for _, w := range warnings {
		if w == "source and target clusters are the same" {
			found = true
		}
	}
	if !found {
		t.Error("expected warning about same clusters")
	}
}

func TestValidatePlan_InvalidPartitions(t *testing.T) {
	t.Parallel()
	plan := &MigrationPlan{
		SourceCluster: "src",
		TargetCluster: "tgt",
		Topics: []TopicMigration{
			{Name: "bad", Partitions: 0, ReplicationFactor: 1, Action: "create"},
		},
	}

	warnings := ValidatePlan(plan)
	if len(warnings) == 0 {
		t.Error("expected warning about invalid partitions")
	}
}

func TestValidatePlan_InvalidReplicationFactor(t *testing.T) {
	t.Parallel()
	plan := &MigrationPlan{
		SourceCluster: "src",
		TargetCluster: "tgt",
		Topics: []TopicMigration{
			{Name: "bad", Partitions: 3, ReplicationFactor: -1, Action: "create"},
		},
	}

	warnings := ValidatePlan(plan)
	if len(warnings) == 0 {
		t.Error("expected warning about invalid replication factor")
	}
}

func TestValidatePlan_NoWarnings(t *testing.T) {
	t.Parallel()
	plan := &MigrationPlan{
		SourceCluster: "src",
		TargetCluster: "tgt",
		Topics: []TopicMigration{
			{Name: "ok", Partitions: 3, ReplicationFactor: 2, Action: "create"},
		},
	}

	warnings := ValidatePlan(plan)
	if len(warnings) != 0 {
		t.Errorf("expected no warnings, got %v", warnings)
	}
}

func TestMapsEqual(t *testing.T) {
	t.Parallel()
	if !mapsEqual(nil, nil) {
		t.Error("nil maps should be equal")
	}
	if !mapsEqual(map[string]string{"a": "1"}, map[string]string{"a": "1"}) {
		t.Error("identical maps should be equal")
	}
	if mapsEqual(map[string]string{"a": "1"}, map[string]string{"a": "2"}) {
		t.Error("different values should not be equal")
	}
	if mapsEqual(map[string]string{"a": "1"}, map[string]string{"b": "1"}) {
		t.Error("different keys should not be equal")
	}
	if mapsEqual(map[string]string{"a": "1"}, map[string]string{"a": "1", "b": "2"}) {
		t.Error("different lengths should not be equal")
	}
}
