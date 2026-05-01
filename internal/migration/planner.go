package migration

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"github.com/kafkamcp/kafkamcp/internal/randid"
)

var planCounter atomic.Uint64

// TopicInfo describes a topic in a Kafka cluster.
type TopicInfo struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	Configs           map[string]string
}

// TopicMigration describes the migration action for a single topic.
type TopicMigration struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	Configs           map[string]string
	Action            string // create, skip, update
	Reason            string
}

// GroupMigration describes the migration action for a consumer group.
type GroupMigration struct {
	GroupID        string
	Topics         []string
	Action         string // recreate, skip
	OffsetStrategy string // earliest, latest, translate
}

// SchemaMigration describes the migration action for a schema subject.
type SchemaMigration struct {
	Subject    string
	Versions   int
	Compatible bool
	Action     string // migrate, skip
}

// MigrationPlan holds the full migration plan between two clusters.
type MigrationPlan struct {
	ID             string
	SourceCluster  string
	TargetCluster  string
	Status         string
	Topics         []TopicMigration
	ConsumerGroups []GroupMigration
	SchemaSubjects []SchemaMigration
	Warnings       []string
	CreatedAt      string
}

// MigrationStepResult captures the outcome of a single migration step.
type MigrationStepResult struct {
	Step     string
	Resource string
	Status   string
	Error    string
}

// MigrationResult captures the outcome of executing a migration plan.
type MigrationResult struct {
	PlanID    string
	Executed  int
	Succeeded int
	Failed    int
	DryRun    bool
	Details   []MigrationStepResult
}

// MigrationProgress tracks ongoing migration execution.
type MigrationProgress struct {
	PlanID         string                `json:"plan_id"`
	Status         string                `json:"status"`
	TotalSteps     int                   `json:"total_steps"`
	CompletedSteps int                   `json:"completed_steps"`
	FailedSteps    int                   `json:"failed_steps"`
	CurrentStep    string                `json:"current_step"`
	StartedAt      string                `json:"started_at"`
	UpdatedAt      string                `json:"updated_at"`
	DryRun         bool                  `json:"dry_run"`
	Resumable      bool                  `json:"resumable"`
	Details        []MigrationStepResult `json:"details"`
	Errors         []string              `json:"errors,omitempty"`
}

// OffsetMapping maps source offsets to target offsets for consumer group migration.
type OffsetMapping struct {
	Topic          string            `json:"topic"`
	PartitionMap   map[int32]int64   `json:"partition_map"`
	Strategy       string            `json:"strategy"`
	MappedAt       string            `json:"mapped_at"`
}

// RollbackRecord stores information needed to rollback a migration.
type RollbackRecord struct {
	PlanID          string              `json:"plan_id"`
	TopicsCreated   []string            `json:"topics_created"`
	TopicsModified  []TopicMigration    `json:"topics_modified"`
	GroupsRecreated []string            `json:"groups_recreated"`
	SchemasMigrated []string            `json:"schemas_migrated"`
	CreatedAt       string              `json:"created_at"`
}

// MigrationReport summarizes a completed migration.
type MigrationReport struct {
	PlanID          string                `json:"plan_id"`
	SourceCluster   string                `json:"source_cluster"`
	TargetCluster   string                `json:"target_cluster"`
	Status          string                `json:"status"`
	TopicsMigrated  int                   `json:"topics_migrated"`
	GroupsMigrated  int                   `json:"groups_migrated"`
	SchemasMigrated int                   `json:"schemas_migrated"`
	DurationMs      int64                 `json:"duration_ms"`
	Warnings        []string              `json:"warnings"`
	ValidationResult *ValidationResult    `json:"validation_result,omitempty"`
	GeneratedAt     string                `json:"generated_at"`
}

// ValidationResult captures pre/post migration validation.
type ValidationResult struct {
	TopicsValid     bool     `json:"topics_valid"`
	SchemasValid    bool     `json:"schemas_valid"`
	GroupsValid     bool     `json:"groups_valid"`
	Errors          []string `json:"errors,omitempty"`
	Warnings        []string `json:"warnings,omitempty"`
}

// Planner creates and manages migration plans.
type Planner struct {
	mu        sync.RWMutex
	plans     map[string]*MigrationPlan
	progress  map[string]*MigrationProgress
	rollbacks map[string]*RollbackRecord
	reports   map[string]*MigrationReport
}

// NewPlanner returns a new Planner.
func NewPlanner() *Planner {
	return &Planner{
		plans:     make(map[string]*MigrationPlan),
		progress:  make(map[string]*MigrationProgress),
		rollbacks: make(map[string]*RollbackRecord),
		reports:   make(map[string]*MigrationReport),
	}
}

// CreatePlan diffs topics between source and target clusters and returns a migration plan.
func (p *Planner) CreatePlan(sourceCluster, targetCluster string, sourceTopics, targetTopics []TopicInfo) *MigrationPlan {
	targetMap := make(map[string]TopicInfo, len(targetTopics))
	for _, t := range targetTopics {
		targetMap[t.Name] = t
	}

	var topics []TopicMigration
	for _, src := range sourceTopics {
		tgt, exists := targetMap[src.Name]
		if !exists {
			topics = append(topics, TopicMigration{
				Name:              src.Name,
				Partitions:        src.Partitions,
				ReplicationFactor: src.ReplicationFactor,
				Configs:           src.Configs,
				Action:            "create",
				Reason:            "topic does not exist in target cluster",
			})
			continue
		}

		if src.Partitions != tgt.Partitions || src.ReplicationFactor != tgt.ReplicationFactor || !mapsEqual(src.Configs, tgt.Configs) {
			topics = append(topics, TopicMigration{
				Name:              src.Name,
				Partitions:        src.Partitions,
				ReplicationFactor: src.ReplicationFactor,
				Configs:           src.Configs,
				Action:            "update",
				Reason:            "topic configuration differs between clusters",
			})
			continue
		}

		topics = append(topics, TopicMigration{
			Name:              src.Name,
			Partitions:        src.Partitions,
			ReplicationFactor: src.ReplicationFactor,
			Configs:           src.Configs,
			Action:            "skip",
			Reason:            "topic already exists with matching configuration",
		})
	}

	plan := &MigrationPlan{
		ID:            randid.New("plan"),
		SourceCluster: sourceCluster,
		TargetCluster: targetCluster,
		Status:        "pending",
		Topics:        topics,
		CreatedAt:     time.Now().UTC().Format(time.RFC3339),
	}

	plan.Warnings = ValidatePlan(plan)

	p.mu.Lock()
	p.plans[plan.ID] = plan
	p.mu.Unlock()

	return plan
}

// GetPlan returns a plan by ID.
func (p *Planner) GetPlan(planID string) (*MigrationPlan, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	plan, ok := p.plans[planID]
	if !ok {
		return nil, fmt.Errorf("plan %q not found", planID)
	}
	return plan, nil
}

// ListPlans returns all stored migration plans.
func (p *Planner) ListPlans() []MigrationPlan {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make([]MigrationPlan, 0, len(p.plans))
	for _, plan := range p.plans {
		result = append(result, *plan)
	}
	return result
}

// ValidatePlan checks a migration plan and returns validation warnings.
func ValidatePlan(plan *MigrationPlan) []string {
	var warnings []string

	if plan.SourceCluster == "" {
		warnings = append(warnings, "source cluster is empty")
	}
	if plan.TargetCluster == "" {
		warnings = append(warnings, "target cluster is empty")
	}
	if plan.SourceCluster == plan.TargetCluster && plan.SourceCluster != "" {
		warnings = append(warnings, "source and target clusters are the same")
	}

	for _, t := range plan.Topics {
		if t.Partitions <= 0 {
			warnings = append(warnings, fmt.Sprintf("topic %q has invalid partition count: %d", t.Name, t.Partitions))
		}
		if t.ReplicationFactor <= 0 {
			warnings = append(warnings, fmt.Sprintf("topic %q has invalid replication factor: %d", t.Name, t.ReplicationFactor))
		}
	}

	return warnings
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// ExecutePlan simulates executing a migration plan.
// In dry-run mode, it validates what would happen without making changes.
func (p *Planner) ExecutePlan(planID string, dryRun bool) (*MigrationResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	plan, ok := p.plans[planID]
	if !ok {
		return nil, fmt.Errorf("plan %q not found", planID)
	}
	if plan.Status != "pending" && plan.Status != "failed" {
		return nil, fmt.Errorf("plan %q is in status %q (must be pending or failed)", planID, plan.Status)
	}

	progress := &MigrationProgress{
		PlanID:     planID,
		Status:     "running",
		TotalSteps: len(plan.Topics) + len(plan.ConsumerGroups) + len(plan.SchemaSubjects),
		StartedAt:  time.Now().UTC().Format(time.RFC3339),
		DryRun:     dryRun,
		Resumable:  true,
	}
	p.progress[planID] = progress

	rollback := &RollbackRecord{
		PlanID:    planID,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	result := &MigrationResult{
		PlanID: planID,
		DryRun: dryRun,
	}

	// Execute topic migrations
	for _, t := range plan.Topics {
		progress.CurrentStep = fmt.Sprintf("topic:%s:%s", t.Action, t.Name)
		progress.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

		step := MigrationStepResult{
			Step:     "topic_" + t.Action,
			Resource: t.Name,
		}

		if t.Action == "skip" {
			step.Status = "skipped"
			result.Details = append(result.Details, step)
			progress.CompletedSteps++
			continue
		}

		if dryRun {
			step.Status = "dry_run"
			result.Details = append(result.Details, step)
			result.Executed++
			result.Succeeded++
			progress.CompletedSteps++
			continue
		}

		// Simulate execution success
		step.Status = "success"
		result.Details = append(result.Details, step)
		result.Executed++
		result.Succeeded++
		progress.CompletedSteps++

		if t.Action == "create" {
			rollback.TopicsCreated = append(rollback.TopicsCreated, t.Name)
		} else if t.Action == "update" {
			rollback.TopicsModified = append(rollback.TopicsModified, t)
		}
	}

	// Execute consumer group migrations
	for _, g := range plan.ConsumerGroups {
		progress.CurrentStep = fmt.Sprintf("group:%s:%s", g.Action, g.GroupID)
		progress.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

		step := MigrationStepResult{
			Step:     "group_" + g.Action,
			Resource: g.GroupID,
		}

		if g.Action == "skip" {
			step.Status = "skipped"
			result.Details = append(result.Details, step)
			progress.CompletedSteps++
			continue
		}

		if dryRun {
			step.Status = "dry_run"
		} else {
			step.Status = "success"
			rollback.GroupsRecreated = append(rollback.GroupsRecreated, g.GroupID)
		}

		result.Details = append(result.Details, step)
		result.Executed++
		result.Succeeded++
		progress.CompletedSteps++
	}

	// Execute schema migrations
	for _, sc := range plan.SchemaSubjects {
		progress.CurrentStep = fmt.Sprintf("schema:%s:%s", sc.Action, sc.Subject)
		progress.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

		step := MigrationStepResult{
			Step:     "schema_" + sc.Action,
			Resource: sc.Subject,
		}

		if sc.Action == "skip" {
			step.Status = "skipped"
			result.Details = append(result.Details, step)
			progress.CompletedSteps++
			continue
		}

		if dryRun {
			step.Status = "dry_run"
		} else {
			step.Status = "success"
			rollback.SchemasMigrated = append(rollback.SchemasMigrated, sc.Subject)
		}

		result.Details = append(result.Details, step)
		result.Executed++
		result.Succeeded++
		progress.CompletedSteps++
	}

	// Update plan status
	if dryRun {
		plan.Status = "pending"
		progress.Status = "dry_run_complete"
	} else if result.Failed > 0 {
		plan.Status = "failed"
		progress.Status = "failed"
	} else {
		plan.Status = "completed"
		progress.Status = "completed"
	}

	progress.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	p.rollbacks[planID] = rollback

	// Generate report
	report := &MigrationReport{
		PlanID:          planID,
		SourceCluster:   plan.SourceCluster,
		TargetCluster:   plan.TargetCluster,
		Status:          string(plan.Status),
		TopicsMigrated:  countNonSkipped(plan.Topics),
		GroupsMigrated:  countNonSkippedGroups(plan.ConsumerGroups),
		SchemasMigrated: countNonSkippedSchemas(plan.SchemaSubjects),
		Warnings:        plan.Warnings,
		GeneratedAt:     time.Now().UTC().Format(time.RFC3339),
	}
	if progress.StartedAt != "" {
		if startTime, err := time.Parse(time.RFC3339, progress.StartedAt); err == nil {
			report.DurationMs = time.Since(startTime).Milliseconds()
		}
	}
	p.reports[planID] = report

	return result, nil
}

// GetProgress returns the current progress of a migration execution.
func (p *Planner) GetProgress(planID string) (*MigrationProgress, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	progress, ok := p.progress[planID]
	if !ok {
		return nil, fmt.Errorf("no execution found for plan %q", planID)
	}
	return progress, nil
}

// Rollback reverts the changes made by a migration.
func (p *Planner) Rollback(planID string) (*RollbackRecord, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	record, ok := p.rollbacks[planID]
	if !ok {
		return nil, fmt.Errorf("no rollback record for plan %q", planID)
	}

	plan, ok := p.plans[planID]
	if !ok {
		return nil, fmt.Errorf("plan %q not found", planID)
	}

	if plan.Status != "completed" && plan.Status != "failed" {
		return nil, fmt.Errorf("plan %q cannot be rolled back (status: %s)", planID, plan.Status)
	}

	plan.Status = "rolled_back"

	if progress, ok := p.progress[planID]; ok {
		progress.Status = "rolled_back"
		progress.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	}

	return record, nil
}

// GetReport returns the migration report for a plan.
func (p *Planner) GetReport(planID string) (*MigrationReport, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	report, ok := p.reports[planID]
	if !ok {
		return nil, fmt.Errorf("no report for plan %q", planID)
	}
	return report, nil
}

// MapOffsets creates offset mappings for consumer group transfer.
func (p *Planner) MapOffsets(planID string, topic string, sourceOffsets map[int32]int64, strategy string) (*OffsetMapping, error) {
	p.mu.RLock()
	_, ok := p.plans[planID]
	p.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("plan %q not found", planID)
	}

	mapping := &OffsetMapping{
		Topic:        topic,
		PartitionMap: make(map[int32]int64),
		Strategy:     strategy,
		MappedAt:     time.Now().UTC().Format(time.RFC3339),
	}

	switch strategy {
	case "earliest":
		for partition := range sourceOffsets {
			mapping.PartitionMap[partition] = 0
		}
	case "latest":
		for partition := range sourceOffsets {
			mapping.PartitionMap[partition] = -1 // -1 = latest
		}
	case "translate":
		// Use source offsets directly (timestamp-based would need broker access)
		for partition, offset := range sourceOffsets {
			mapping.PartitionMap[partition] = offset
		}
	default:
		return nil, fmt.Errorf("unknown offset strategy %q; must be: earliest, latest, translate", strategy)
	}

	return mapping, nil
}

func countNonSkipped(topics []TopicMigration) int {
	count := 0
	for _, t := range topics {
		if t.Action != "skip" {
			count++
		}
	}
	return count
}

func countNonSkippedGroups(groups []GroupMigration) int {
	count := 0
	for _, g := range groups {
		if g.Action != "skip" {
			count++
		}
	}
	return count
}

func countNonSkippedSchemas(schemas []SchemaMigration) int {
	count := 0
	for _, s := range schemas {
		if s.Action != "skip" {
			count++
		}
	}
	return count
}
