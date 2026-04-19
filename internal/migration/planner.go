package migration

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
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

// Planner creates and manages migration plans.
type Planner struct {
	mu    sync.RWMutex
	plans map[string]*MigrationPlan
}

// NewPlanner returns a new Planner.
func NewPlanner() *Planner {
	return &Planner{
		plans: make(map[string]*MigrationPlan),
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
		ID:            fmt.Sprintf("plan-%d-%d", time.Now().UnixNano(), planCounter.Add(1)),
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
