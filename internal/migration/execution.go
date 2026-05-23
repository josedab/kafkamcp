package migration

import (
	"fmt"
	"time"
)

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
		Status:          plan.Status,
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
