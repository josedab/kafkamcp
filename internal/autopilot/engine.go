package autopilot

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/digitaltwin"
	"github.com/kafkamcp/kafkamcp/internal/migration"
	"github.com/kafkamcp/kafkamcp/internal/randid"
	"github.com/kafkamcp/kafkamcp/internal/sandbox"
)

// Phase describes a stage in the autopilot migration pipeline.
type Phase string

const (
	PhaseAssessment Phase = "assessment"
	PhasePlanning   Phase = "planning"
	PhaseDryRun     Phase = "dry_run"
	PhaseValidation Phase = "validation"
	PhaseExecution  Phase = "execution"
	PhaseCompleted  Phase = "completed"
	PhaseFailed     Phase = "failed"
	PhaseRolledBack Phase = "rolled_back"
)

// ClusterAssessment captures the health and readiness of source and target clusters.
type ClusterAssessment struct {
	SourceCluster       string              `json:"source_cluster"`
	TargetCluster       string              `json:"target_cluster"`
	SourceTopicCount    int                 `json:"source_topic_count"`
	TargetTopicCount    int                 `json:"target_topic_count"`
	SourceGroupCount    int                 `json:"source_group_count"`
	TopicsToMigrate     int                 `json:"topics_to_migrate"`
	TopicsToUpdate      int                 `json:"topics_to_update"`
	TopicsToSkip        int                 `json:"topics_to_skip"`
	EstimatedDataSizeGB float64             `json:"estimated_data_size_gb"`
	RiskLevel           string              `json:"risk_level"` // low, medium, high, critical
	Risks               []Risk              `json:"risks,omitempty"`
	Recommendations     []string            `json:"recommendations,omitempty"`
	Ready               bool                `json:"ready"`
	AssessedAt          string              `json:"assessed_at"`
}

// Risk describes a migration risk factor.
type Risk struct {
	Category    string `json:"category"` // data_loss, downtime, performance, compatibility
	Severity    string `json:"severity"` // low, medium, high, critical
	Description string `json:"description"`
	Mitigation  string `json:"mitigation"`
}

// DryRunResult captures the outcome of simulating the migration on a digital twin.
type DryRunResult struct {
	SessionID      string                        `json:"session_id"`
	PlanID         string                        `json:"plan_id"`
	TwinID         string                        `json:"twin_id"`
	SnapshotID     string                        `json:"snapshot_id"`
	SimulationOps  int                           `json:"simulation_ops"`
	AllSucceeded   bool                          `json:"all_succeeded"`
	Results        []digitaltwin.SimulationResult `json:"results"`
	Diff           *digitaltwin.DiffResult       `json:"diff,omitempty"`
	Warnings       []string                      `json:"warnings,omitempty"`
	DurationMs     int64                         `json:"duration_ms"`
	CompletedAt    string                        `json:"completed_at"`
}

// AutopilotSession tracks the full lifecycle of an automated migration.
type AutopilotSession struct {
	ID              string             `json:"id"`
	SourceCluster   string             `json:"source_cluster"`
	TargetCluster   string             `json:"target_cluster"`
	Phase           Phase              `json:"phase"`
	Assessment      *ClusterAssessment `json:"assessment,omitempty"`
	PlanID          string             `json:"plan_id,omitempty"`
	DryRun          *DryRunResult      `json:"dry_run,omitempty"`
	SandboxID       string             `json:"sandbox_id,omitempty"`
	ExecutionResult *migration.MigrationResult `json:"execution_result,omitempty"`
	RollbackAvail   bool               `json:"rollback_available"`
	Errors          []string           `json:"errors,omitempty"`
	CreatedAt       string             `json:"created_at"`
	UpdatedAt       string             `json:"updated_at"`
}

// Engine orchestrates the full autopilot migration pipeline.
type Engine struct {
	mu              sync.RWMutex
	sessions        map[string]*AutopilotSession
	migrationPlanner *migration.Planner
	digitalTwin     *digitaltwin.Engine
	sandboxEngine   *sandbox.Engine
	logger          *slog.Logger
}

// NewEngine creates a new autopilot engine.
func NewEngine(mp *migration.Planner, dt *digitaltwin.Engine, sb *sandbox.Engine, logger *slog.Logger) *Engine {
	return &Engine{
		sessions:         make(map[string]*AutopilotSession),
		migrationPlanner: mp,
		digitalTwin:      dt,
		sandboxEngine:    sb,
		logger:           logger,
	}
}

// Assess evaluates source and target clusters for migration readiness.
func (e *Engine) Assess(sourceCluster, targetCluster string, sourceTopics, targetTopics []migration.TopicInfo, sourceGroups []string) (*AutopilotSession, error) {
	if sourceCluster == "" {
		return nil, fmt.Errorf("source cluster is required")
	}
	if targetCluster == "" {
		return nil, fmt.Errorf("target cluster is required")
	}
	if sourceCluster == targetCluster {
		return nil, fmt.Errorf("source and target clusters must be different")
	}

	now := time.Now().UTC().Format(time.RFC3339)
	session := &AutopilotSession{
		ID:            randid.New("autopilot"),
		SourceCluster: sourceCluster,
		TargetCluster: targetCluster,
		Phase:         PhaseAssessment,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	// Build assessment
	targetMap := make(map[string]migration.TopicInfo, len(targetTopics))
	for _, t := range targetTopics {
		targetMap[t.Name] = t
	}

	var toMigrate, toUpdate, toSkip int
	var risks []Risk
	var dataSize float64

	for _, src := range sourceTopics {
		tgt, exists := targetMap[src.Name]
		if !exists {
			toMigrate++
			dataSize += float64(src.Partitions) * 0.5 // estimate 0.5 GB per partition
			continue
		}
		if src.Partitions != tgt.Partitions || src.ReplicationFactor != tgt.ReplicationFactor {
			toUpdate++
			if src.Partitions > tgt.Partitions {
				risks = append(risks, Risk{
					Category:    "compatibility",
					Severity:    "medium",
					Description: fmt.Sprintf("Topic %q has more partitions in source (%d) than target (%d)", src.Name, src.Partitions, tgt.Partitions),
					Mitigation:  "Increase partition count on target before migration",
				})
			}
			if src.ReplicationFactor > tgt.ReplicationFactor {
				risks = append(risks, Risk{
					Category:    "data_loss",
					Severity:    "high",
					Description: fmt.Sprintf("Topic %q replication factor lower on target (%d vs %d); reduced durability", src.Name, tgt.ReplicationFactor, src.ReplicationFactor),
					Mitigation:  "Increase replication factor on target before migration",
				})
			}
			continue
		}
		toSkip++
	}

	// Assess risk level
	riskLevel := "low"
	if toMigrate > 50 || len(sourceGroups) > 20 {
		riskLevel = "medium"
	}
	if toMigrate > 200 || dataSize > 100 {
		riskLevel = "high"
	}
	for _, r := range risks {
		if r.Severity == "critical" {
			riskLevel = "critical"
			break
		}
	}

	var recommendations []string
	if toMigrate > 0 {
		recommendations = append(recommendations, fmt.Sprintf("Run dry-run simulation before migrating %d new topics", toMigrate))
	}
	if len(sourceGroups) > 0 {
		recommendations = append(recommendations, "Coordinate consumer group offset migration with application teams")
	}
	if riskLevel == "high" || riskLevel == "critical" {
		recommendations = append(recommendations, "Consider phased migration: migrate topics in batches")
	}

	session.Assessment = &ClusterAssessment{
		SourceCluster:       sourceCluster,
		TargetCluster:       targetCluster,
		SourceTopicCount:    len(sourceTopics),
		TargetTopicCount:    len(targetTopics),
		SourceGroupCount:    len(sourceGroups),
		TopicsToMigrate:     toMigrate,
		TopicsToUpdate:      toUpdate,
		TopicsToSkip:        toSkip,
		EstimatedDataSizeGB: dataSize,
		RiskLevel:           riskLevel,
		Risks:               risks,
		Recommendations:     recommendations,
		Ready:               riskLevel != "critical",
		AssessedAt:          now,
	}

	e.mu.Lock()
	e.sessions[session.ID] = session
	e.mu.Unlock()

	e.logger.Info("autopilot assessment complete",
		"session", session.ID,
		"risk_level", riskLevel,
		"topics_to_migrate", toMigrate,
	)

	return session, nil
}

// GeneratePlan creates a migration plan for an assessed session.
func (e *Engine) GeneratePlan(sessionID string, sourceTopics, targetTopics []migration.TopicInfo) (*AutopilotSession, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	session, ok := e.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %q not found", sessionID)
	}
	if session.Assessment == nil {
		return nil, fmt.Errorf("session %q has not been assessed yet", sessionID)
	}
	if !session.Assessment.Ready {
		return nil, fmt.Errorf("session %q assessment indicates critical risks; resolve before planning", sessionID)
	}

	plan := e.migrationPlanner.CreatePlan(session.SourceCluster, session.TargetCluster, sourceTopics, targetTopics)
	session.PlanID = plan.ID
	session.Phase = PhasePlanning
	session.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	return session, nil
}

// DryRun simulates the migration on a digital twin.
func (e *Engine) DryRun(sessionID string, topics map[string]digitaltwin.TopicState, groups map[string]digitaltwin.ConsumerGroupState, brokers []digitaltwin.BrokerState) (*AutopilotSession, error) {
	e.mu.Lock()
	session, ok := e.sessions[sessionID]
	if !ok {
		e.mu.Unlock()
		return nil, fmt.Errorf("session %q not found", sessionID)
	}
	if session.PlanID == "" {
		e.mu.Unlock()
		return nil, fmt.Errorf("session %q has no migration plan; generate one first", sessionID)
	}
	e.mu.Unlock()

	start := time.Now()

	// Capture snapshot and create twin
	snapshot := e.digitalTwin.CaptureSnapshot(session.SourceCluster, topics, groups, brokers)
	twin, err := e.digitalTwin.CreateTwin(snapshot.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to create digital twin: %w", err)
	}

	// Get migration plan to derive simulation ops
	plan, err := e.migrationPlanner.GetPlan(session.PlanID)
	if err != nil {
		return nil, fmt.Errorf("failed to get migration plan: %w", err)
	}

	dryRun := &DryRunResult{
		SessionID:  sessionID,
		PlanID:     session.PlanID,
		TwinID:     twin.ID,
		SnapshotID: snapshot.ID,
	}

	allSucceeded := true
	for _, t := range plan.Topics {
		if t.Action == "skip" {
			continue
		}

		var op digitaltwin.SimulationOp
		switch t.Action {
		case "create":
			op = digitaltwin.SimulationOp{
				Type: "create_topic",
				Params: map[string]any{
					"name":               t.Name,
					"partitions":         t.Partitions,
					"replication_factor": t.ReplicationFactor,
				},
			}
		case "update":
			op = digitaltwin.SimulationOp{
				Type: "alter_partitions",
				Params: map[string]any{
					"name":       t.Name,
					"partitions": t.Partitions,
				},
			}
		default:
			continue
		}

		result, err := e.digitalTwin.SimulateOp(twin.ID, op)
		if err != nil {
			allSucceeded = false
			dryRun.Warnings = append(dryRun.Warnings, fmt.Sprintf("simulation error for %s: %v", t.Name, err))
			continue
		}
		dryRun.Results = append(dryRun.Results, *result)
		dryRun.SimulationOps++
		if !result.Success {
			allSucceeded = false
			dryRun.Warnings = append(dryRun.Warnings, fmt.Sprintf("simulation failed for %s: %s", t.Name, result.Error))
		}
	}

	// Generate diff
	diff, err := e.digitalTwin.Diff(twin.ID)
	if err == nil {
		dryRun.Diff = diff
	}

	dryRun.AllSucceeded = allSucceeded
	dryRun.DurationMs = time.Since(start).Milliseconds()
	dryRun.CompletedAt = time.Now().UTC().Format(time.RFC3339)

	e.mu.Lock()
	session.DryRun = dryRun
	session.Phase = PhaseDryRun
	session.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	e.mu.Unlock()

	return session, nil
}

// Validate runs sandbox validation of the migration plan.
func (e *Engine) Validate(sessionID, agentID string) (*AutopilotSession, error) {
	e.mu.Lock()
	session, ok := e.sessions[sessionID]
	if !ok {
		e.mu.Unlock()
		return nil, fmt.Errorf("session %q not found", sessionID)
	}
	if session.DryRun == nil {
		e.mu.Unlock()
		return nil, fmt.Errorf("session %q has not completed dry-run; run dry-run first", sessionID)
	}
	e.mu.Unlock()

	// Create a sandbox for validation
	sb, err := e.sandboxEngine.Create(
		fmt.Sprintf("autopilot-validate-%s", session.ID),
		fmt.Sprintf("Sandbox validation for migration %s", session.ID),
		agentID,
		3600, // 1 hour
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create validation sandbox: %w", err)
	}

	e.mu.Lock()
	session.SandboxID = sb.ID
	session.Phase = PhaseValidation
	session.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	e.mu.Unlock()

	return session, nil
}

// Execute runs the migration plan (with dry-run support).
func (e *Engine) Execute(sessionID string, dryRun bool) (*AutopilotSession, error) {
	e.mu.Lock()
	session, ok := e.sessions[sessionID]
	if !ok {
		e.mu.Unlock()
		return nil, fmt.Errorf("session %q not found", sessionID)
	}
	if session.PlanID == "" {
		e.mu.Unlock()
		return nil, fmt.Errorf("session %q has no migration plan", sessionID)
	}
	e.mu.Unlock()

	result, err := e.migrationPlanner.ExecutePlan(session.PlanID, dryRun)
	if err != nil {
		e.mu.Lock()
		session.Phase = PhaseFailed
		session.Errors = append(session.Errors, err.Error())
		session.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
		e.mu.Unlock()
		return session, err
	}

	e.mu.Lock()
	session.ExecutionResult = result
	session.RollbackAvail = !dryRun && result.Succeeded > 0
	if dryRun {
		session.Phase = PhaseDryRun
	} else if result.Failed > 0 {
		session.Phase = PhaseFailed
	} else {
		session.Phase = PhaseCompleted
	}
	session.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	e.mu.Unlock()

	return session, nil
}

// Rollback reverts a completed migration.
func (e *Engine) Rollback(sessionID string) (*AutopilotSession, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	session, ok := e.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %q not found", sessionID)
	}
	if !session.RollbackAvail {
		return nil, fmt.Errorf("session %q has no rollback available", sessionID)
	}

	_, err := e.migrationPlanner.Rollback(session.PlanID)
	if err != nil {
		return nil, fmt.Errorf("rollback failed: %w", err)
	}

	session.Phase = PhaseRolledBack
	session.RollbackAvail = false
	session.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	return session, nil
}

// GetSession returns a session by ID.
func (e *Engine) GetSession(sessionID string) (*AutopilotSession, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	session, ok := e.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %q not found", sessionID)
	}
	return session, nil
}

// ListSessions returns all autopilot sessions.
func (e *Engine) ListSessions() []AutopilotSession {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]AutopilotSession, 0, len(e.sessions))
	for _, s := range e.sessions {
		result = append(result, *s)
	}
	return result
}
