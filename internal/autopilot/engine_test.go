package autopilot

import (
	"log/slog"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/digitaltwin"
	"github.com/kafkamcp/kafkamcp/internal/migration"
	"github.com/kafkamcp/kafkamcp/internal/sandbox"
)

func newTestEngine() *Engine {
	logger := slog.Default()
	mp := migration.NewPlanner()
	dt := digitaltwin.NewEngine(logger)
	sb := sandbox.NewEngine(logger)
	return NewEngine(mp, dt, sb, logger)
}

func TestAssess(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	sourceTopics := []migration.TopicInfo{
		{Name: "orders", Partitions: 12, ReplicationFactor: 3},
		{Name: "users", Partitions: 6, ReplicationFactor: 3},
		{Name: "events", Partitions: 24, ReplicationFactor: 3},
	}
	targetTopics := []migration.TopicInfo{
		{Name: "users", Partitions: 6, ReplicationFactor: 3},
	}
	groups := []string{"order-processor", "event-consumer"}

	session, err := e.Assess("cluster-a", "cluster-b", sourceTopics, targetTopics, groups)
	if err != nil {
		t.Fatalf("Assess failed: %v", err)
	}
	if session.Assessment == nil {
		t.Fatal("expected assessment to be set")
	}
	if session.Assessment.TopicsToMigrate != 2 {
		t.Errorf("expected 2 topics to migrate, got %d", session.Assessment.TopicsToMigrate)
	}
	if session.Assessment.TopicsToSkip != 1 {
		t.Errorf("expected 1 topic to skip, got %d", session.Assessment.TopicsToSkip)
	}
	if session.Phase != PhaseAssessment {
		t.Errorf("expected assessment phase, got %s", session.Phase)
	}
	if len(session.Assessment.Recommendations) == 0 {
		t.Error("expected recommendations for non-trivial migration")
	}
}

func TestAssessValidation(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	if _, err := e.Assess("", "cluster-b", nil, nil, nil); err == nil {
		t.Fatal("expected error for empty source cluster")
	}
	if _, err := e.Assess("cluster-a", "", nil, nil, nil); err == nil {
		t.Fatal("expected error for empty target cluster")
	}
	if _, err := e.Assess("cluster-a", "cluster-a", nil, nil, nil); err == nil {
		t.Fatal("expected error for same source and target")
	}
}

func TestAssessRiskDetection(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	sourceTopics := []migration.TopicInfo{
		{Name: "orders", Partitions: 12, ReplicationFactor: 3},
	}
	targetTopics := []migration.TopicInfo{
		{Name: "orders", Partitions: 6, ReplicationFactor: 1},
	}

	session, err := e.Assess("src", "tgt", sourceTopics, targetTopics, nil)
	if err != nil {
		t.Fatalf("Assess: %v", err)
	}
	if len(session.Assessment.Risks) < 2 {
		t.Errorf("expected at least 2 risks (partition + replication), got %d", len(session.Assessment.Risks))
	}
	hasDataLoss := false
	for _, r := range session.Assessment.Risks {
		if r.Category == "data_loss" {
			hasDataLoss = true
		}
	}
	if !hasDataLoss {
		t.Error("expected data_loss risk for reduced replication factor")
	}
}

func TestAssessEmptyTopics(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	session, err := e.Assess("src", "tgt", nil, nil, nil)
	if err != nil {
		t.Fatalf("Assess with empty topics: %v", err)
	}
	if session.Assessment.TopicsToMigrate != 0 {
		t.Errorf("expected 0 topics to migrate, got %d", session.Assessment.TopicsToMigrate)
	}
	if session.Assessment.RiskLevel != "low" {
		t.Errorf("expected low risk for empty migration, got %s", session.Assessment.RiskLevel)
	}
}

func TestGeneratePlanValidation(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	if _, err := e.GeneratePlan("nonexistent", nil, nil); err == nil {
		t.Fatal("expected error for nonexistent session")
	}

	session, _ := e.Assess("src", "tgt", nil, nil, nil)
	// GeneratePlan should work even with empty topics
	session, err := e.GeneratePlan(session.ID, nil, nil)
	if err != nil {
		t.Fatalf("GeneratePlan: %v", err)
	}
	if session.Phase != PhasePlanning {
		t.Errorf("expected planning phase, got %s", session.Phase)
	}
}

func TestDryRunValidation(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	if _, err := e.DryRun("nonexistent", nil, nil, nil); err == nil {
		t.Fatal("expected error for nonexistent session")
	}

	session, _ := e.Assess("src", "tgt", nil, nil, nil)
	if _, err := e.DryRun(session.ID, nil, nil, nil); err == nil {
		t.Fatal("expected error when no plan exists")
	}
}

func TestValidateStep(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	sourceTopics := []migration.TopicInfo{
		{Name: "orders", Partitions: 12, ReplicationFactor: 3},
	}
	session, _ := e.Assess("src", "tgt", sourceTopics, nil, nil)
	session, _ = e.GeneratePlan(session.ID, sourceTopics, nil)

	topics := map[string]digitaltwin.TopicState{
		"orders": {Name: "orders", Partitions: 12, ReplicationFactor: 3},
	}
	session, _ = e.DryRun(session.ID, topics, nil, nil)

	session, err := e.Validate(session.ID, "test-agent")
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if session.Phase != PhaseValidation {
		t.Errorf("expected validation phase, got %s", session.Phase)
	}
	if session.SandboxID == "" {
		t.Error("expected sandbox ID to be set")
	}
}

func TestValidateRequiresDryRun(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	session, _ := e.Assess("src", "tgt", nil, nil, nil)
	session, _ = e.GeneratePlan(session.ID, nil, nil)

	if _, err := e.Validate(session.ID, "agent"); err == nil {
		t.Fatal("expected error when dry-run not completed")
	}
}

func TestFullPipeline(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	sourceTopics := []migration.TopicInfo{
		{Name: "orders", Partitions: 12, ReplicationFactor: 3},
	}

	session, err := e.Assess("src", "tgt", sourceTopics, nil, nil)
	if err != nil {
		t.Fatalf("Assess: %v", err)
	}

	session, err = e.GeneratePlan(session.ID, sourceTopics, nil)
	if err != nil {
		t.Fatalf("GeneratePlan: %v", err)
	}
	if session.PlanID == "" {
		t.Fatal("expected plan ID to be set")
	}

	topics := map[string]digitaltwin.TopicState{
		"orders": {Name: "orders", Partitions: 12, ReplicationFactor: 3},
	}
	session, err = e.DryRun(session.ID, topics, nil, nil)
	if err != nil {
		t.Fatalf("DryRun: %v", err)
	}
	if session.DryRun == nil {
		t.Fatal("expected dry run result")
	}

	session, err = e.Execute(session.ID, false)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if session.Phase != PhaseCompleted {
		t.Errorf("expected completed phase, got %s", session.Phase)
	}

	sessions := e.ListSessions()
	if len(sessions) != 1 {
		t.Errorf("expected 1 session, got %d", len(sessions))
	}
}

func TestRollback(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	sourceTopics := []migration.TopicInfo{
		{Name: "data", Partitions: 6, ReplicationFactor: 2},
	}

	session, _ := e.Assess("src", "tgt", sourceTopics, nil, nil)
	session, _ = e.GeneratePlan(session.ID, sourceTopics, nil)
	session, _ = e.Execute(session.ID, false)

	if !session.RollbackAvail {
		t.Fatal("expected rollback to be available")
	}

	session, err := e.Rollback(session.ID)
	if err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if session.Phase != PhaseRolledBack {
		t.Errorf("expected rolled_back phase, got %s", session.Phase)
	}

	// Double rollback should fail
	if _, err := e.Rollback(session.ID); err == nil {
		t.Fatal("expected error on double rollback")
	}
}

func TestRollbackRequiresExecution(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	session, _ := e.Assess("src", "tgt", nil, nil, nil)
	if _, err := e.Rollback(session.ID); err == nil {
		t.Fatal("expected error when no execution has happened")
	}
}

func TestGetSession(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	if _, err := e.GetSession("nonexistent"); err == nil {
		t.Fatal("expected error for nonexistent session")
	}

	session, _ := e.Assess("src", "tgt", nil, nil, nil)
	got, err := e.GetSession(session.ID)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if got.ID != session.ID {
		t.Errorf("expected session ID %s, got %s", session.ID, got.ID)
	}
}

func TestExecuteDryRunMode(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	sourceTopics := []migration.TopicInfo{
		{Name: "data", Partitions: 3, ReplicationFactor: 1},
	}
	session, _ := e.Assess("src", "tgt", sourceTopics, nil, nil)
	session, _ = e.GeneratePlan(session.ID, sourceTopics, nil)

	session, err := e.Execute(session.ID, true)
	if err != nil {
		t.Fatalf("Execute dry-run: %v", err)
	}
	if session.Phase != PhaseDryRun {
		t.Errorf("expected dry_run phase after dry-run execution, got %s", session.Phase)
	}
	if session.RollbackAvail {
		t.Error("rollback should not be available after dry-run")
	}
}
