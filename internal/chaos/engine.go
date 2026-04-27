package chaos

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// ExperimentType defines the type of chaos experiment.
type ExperimentType string

const (
	TypeBrokerOutage     ExperimentType = "broker_outage"
	TypeNetworkPartition ExperimentType = "network_partition"
	TypeSlowConsumer     ExperimentType = "slow_consumer"
	TypeSchemaCorruption ExperimentType = "schema_corruption"
	TypeRebalanceStorm   ExperimentType = "rebalance_storm"
	TypeProducerFlood    ExperimentType = "producer_flood"
	TypeTopicDeletion    ExperimentType = "topic_deletion"
)

// ExperimentStatus tracks the lifecycle of a chaos experiment.
type ExperimentStatus string

const (
	StatusPending    ExperimentStatus = "pending"
	StatusRunning    ExperimentStatus = "running"
	StatusCompleted  ExperimentStatus = "completed"
	StatusRolledBack ExperimentStatus = "rolled_back"
	StatusFailed     ExperimentStatus = "failed"
	StatusStopped    ExperimentStatus = "stopped"
)

// SafetyGuard defines blast radius limits for chaos experiments.
type SafetyGuard struct {
	MaxAffectedTopics     int           `json:"max_affected_topics"`
	MaxAffectedPartitions int           `json:"max_affected_partitions"`
	MaxDuration           time.Duration `json:"max_duration"`
	AutoRollbackEnabled   bool          `json:"auto_rollback_enabled"`
	RequireApproval       bool          `json:"require_approval"`
	ExcludeTopics         []string      `json:"exclude_topics,omitempty"`
}

// DefaultSafetyGuard returns conservative safety defaults.
func DefaultSafetyGuard() SafetyGuard {
	return SafetyGuard{
		MaxAffectedTopics:     5,
		MaxAffectedPartitions: 50,
		MaxDuration:           10 * time.Minute,
		AutoRollbackEnabled:   true,
		RequireApproval:       false,
	}
}

// Experiment represents a chaos experiment.
type Experiment struct {
	ID             string           `json:"id"`
	Type           ExperimentType   `json:"type"`
	Status         ExperimentStatus `json:"status"`
	Description    string           `json:"description"`
	TargetCluster  string           `json:"target_cluster"`
	TargetTopics   []string         `json:"target_topics,omitempty"`
	TargetGroups   []string         `json:"target_groups,omitempty"`
	SafetyGuard    SafetyGuard      `json:"safety_guard"`
	Parameters     map[string]any   `json:"parameters,omitempty"`
	StartedAt      string           `json:"started_at,omitempty"`
	CompletedAt    string           `json:"completed_at,omitempty"`
	Duration       string           `json:"duration,omitempty"`
	AffectedItems  []AffectedItem   `json:"affected_items,omitempty"`
	HealthReport   *HealthReport    `json:"health_report,omitempty"`
	Error          string           `json:"error,omitempty"`
	CreatedAt      string           `json:"created_at"`
}

// AffectedItem records a resource affected by the experiment.
type AffectedItem struct {
	Type       string `json:"type"`
	Name       string `json:"name"`
	Impact     string `json:"impact"`
	Recovered  bool   `json:"recovered"`
}

// HealthReport is generated after chaos experiment completion.
type HealthReport struct {
	OverallStatus    string            `json:"overall_status"`
	PreChaosMetrics  map[string]any    `json:"pre_chaos_metrics"`
	PostChaosMetrics map[string]any    `json:"post_chaos_metrics"`
	RecoveryTimeMs   int64             `json:"recovery_time_ms"`
	Findings         []Finding         `json:"findings"`
	Recommendations  []string          `json:"recommendations"`
	GeneratedAt      string            `json:"generated_at"`
}

// Finding is a single observation from a chaos experiment.
type Finding struct {
	Severity    string `json:"severity"`
	Category    string `json:"category"`
	Description string `json:"description"`
	Metric      string `json:"metric,omitempty"`
	Expected    string `json:"expected,omitempty"`
	Actual      string `json:"actual,omitempty"`
}

// Scenario is a pre-built chaos scenario.
type Scenario struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Type        ExperimentType `json:"type"`
	Category    string         `json:"category"`
	Parameters  map[string]any `json:"default_parameters"`
	SafetyGuard SafetyGuard    `json:"safety_guard"`
}

// Engine manages chaos experiments.
type Engine struct {
	experiments       map[string]*Experiment
	experimentCancels map[string]context.CancelFunc
	scenarios         []Scenario
	logger            *slog.Logger
	mu                sync.RWMutex
	experimentCounter atomic.Uint64
}

// NewEngine creates a new chaos engineering engine.
func NewEngine(logger *slog.Logger) *Engine {
	e := &Engine{
		experiments:       make(map[string]*Experiment),
		experimentCancels: make(map[string]context.CancelFunc),
		logger:            logger,
	}
	e.initScenarios()
	return e
}

func (e *Engine) initScenarios() {
	e.scenarios = []Scenario{
		{
			Name:        "split-brain",
			Description: "Simulate a split-brain scenario by partitioning consumer groups from a subset of partitions",
			Type:        TypeNetworkPartition,
			Category:    "network",
			Parameters:  map[string]any{"partition_percentage": 50, "duration_seconds": 120},
			SafetyGuard: SafetyGuard{MaxAffectedTopics: 3, MaxDuration: 5 * time.Minute, AutoRollbackEnabled: true},
		},
		{
			Name:        "rolling-restart",
			Description: "Simulate rolling broker restarts with configurable interval between restarts",
			Type:        TypeBrokerOutage,
			Category:    "infrastructure",
			Parameters:  map[string]any{"restart_interval_seconds": 30, "brokers_affected": 1},
			SafetyGuard: SafetyGuard{MaxAffectedTopics: 10, MaxDuration: 10 * time.Minute, AutoRollbackEnabled: true},
		},
		{
			Name:        "schema-break",
			Description: "Introduce an incompatible schema change to test consumer resilience",
			Type:        TypeSchemaCorruption,
			Category:    "data",
			Parameters:  map[string]any{"corruption_type": "incompatible_field_removal"},
			SafetyGuard: SafetyGuard{MaxAffectedTopics: 1, MaxDuration: 5 * time.Minute, AutoRollbackEnabled: true},
		},
		{
			Name:        "consumer-storm",
			Description: "Trigger multiple simultaneous consumer group rebalances",
			Type:        TypeRebalanceStorm,
			Category:    "consumer",
			Parameters:  map[string]any{"concurrent_rebalances": 3, "groups_affected": 5},
			SafetyGuard: SafetyGuard{MaxAffectedTopics: 5, MaxDuration: 5 * time.Minute, AutoRollbackEnabled: true},
		},
		{
			Name:        "slow-consumer",
			Description: "Introduce artificial processing delay in consumers to test backpressure handling",
			Type:        TypeSlowConsumer,
			Category:    "consumer",
			Parameters:  map[string]any{"delay_ms": 500, "affected_percentage": 50},
			SafetyGuard: SafetyGuard{MaxAffectedTopics: 3, MaxDuration: 5 * time.Minute, AutoRollbackEnabled: true},
		},
		{
			Name:        "producer-flood",
			Description: "Generate a high volume of messages to test throughput limits and backpressure",
			Type:        TypeProducerFlood,
			Category:    "producer",
			Parameters:  map[string]any{"messages_per_second": 1000, "duration_seconds": 60},
			SafetyGuard: SafetyGuard{MaxAffectedTopics: 1, MaxDuration: 3 * time.Minute, AutoRollbackEnabled: true},
		},
	}
}

// Inject creates and starts a new chaos experiment.
func (e *Engine) Inject(expType ExperimentType, cluster string, topics, groups []string, params map[string]any, guard *SafetyGuard) (*Experiment, error) {
	if cluster == "" {
		return nil, fmt.Errorf("target_cluster is required")
	}

	sg := DefaultSafetyGuard()
	if guard != nil {
		sg = *guard
	}

	// Validate blast radius
	if len(topics) > sg.MaxAffectedTopics {
		return nil, fmt.Errorf("blast radius exceeded: %d topics > max %d", len(topics), sg.MaxAffectedTopics)
	}

	// Check excluded topics
	for _, t := range topics {
		for _, ex := range sg.ExcludeTopics {
			if t == ex {
				return nil, fmt.Errorf("topic %q is in the exclusion list", t)
			}
		}
	}

	exp := &Experiment{
		ID:            fmt.Sprintf("chaos-%d-%d", time.Now().UnixNano(), e.experimentCounter.Add(1)),
		Type:          expType,
		Status:        StatusRunning,
		Description:   fmt.Sprintf("%s experiment on cluster %s", expType, cluster),
		TargetCluster: cluster,
		TargetTopics:  topics,
		TargetGroups:  groups,
		SafetyGuard:   sg,
		Parameters:    params,
		StartedAt:     time.Now().UTC().Format(time.RFC3339),
		CreatedAt:     time.Now().UTC().Format(time.RFC3339),
	}

	// Record affected items
	for _, t := range topics {
		exp.AffectedItems = append(exp.AffectedItems, AffectedItem{
			Type: "topic", Name: t, Impact: string(expType), Recovered: false,
		})
	}
	for _, g := range groups {
		exp.AffectedItems = append(exp.AffectedItems, AffectedItem{
			Type: "consumer_group", Name: g, Impact: string(expType), Recovered: false,
		})
	}

	e.mu.Lock()
	e.experiments[exp.ID] = exp
	e.mu.Unlock()

	e.logger.Info("chaos experiment started", "id", exp.ID, "type", expType, "cluster", cluster)

	// Auto-rollback timer
	if sg.AutoRollbackEnabled {
		ctx, cancel := context.WithCancel(context.Background())
		e.mu.Lock()
		e.experimentCancels[exp.ID] = cancel
		e.mu.Unlock()
		go func() {
			timer := time.NewTimer(sg.MaxDuration)
			defer timer.Stop()
			select {
			case <-timer.C:
				e.mu.Lock()
				if exp.Status == StatusRunning {
					exp.Status = StatusRolledBack
					exp.CompletedAt = time.Now().UTC().Format(time.RFC3339)
					for i := range exp.AffectedItems {
						exp.AffectedItems[i].Recovered = true
					}
					exp.HealthReport = e.generateHealthReport(exp)
					e.logger.Info("chaos experiment auto-rolled-back", "id", exp.ID)
				}
				delete(e.experimentCancels, exp.ID)
				e.mu.Unlock()
			case <-ctx.Done():
				return
			}
		}()
	}

	return exp, nil
}

// Status returns the current status of an experiment.
func (e *Engine) Status(experimentID string) (*Experiment, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	exp, ok := e.experiments[experimentID]
	if !ok {
		return nil, fmt.Errorf("experiment %q not found", experimentID)
	}
	return exp, nil
}

// Stop stops a running experiment and triggers rollback.
func (e *Engine) Stop(experimentID string) (*Experiment, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	exp, ok := e.experiments[experimentID]
	if !ok {
		return nil, fmt.Errorf("experiment %q not found", experimentID)
	}
	if exp.Status != StatusRunning {
		return nil, fmt.Errorf("experiment %q is not running (status: %s)", experimentID, exp.Status)
	}

	exp.Status = StatusStopped
	exp.CompletedAt = time.Now().UTC().Format(time.RFC3339)
	if cancel, ok := e.experimentCancels[exp.ID]; ok {
		cancel()
		delete(e.experimentCancels, exp.ID)
	}
	if exp.StartedAt != "" {
		if startTime, err := time.Parse(time.RFC3339, exp.StartedAt); err == nil {
			exp.Duration = time.Since(startTime).String()
		}
	}

	for i := range exp.AffectedItems {
		exp.AffectedItems[i].Recovered = true
	}

	exp.HealthReport = e.generateHealthReport(exp)
	e.logger.Info("chaos experiment stopped", "id", experimentID)
	return exp, nil
}

// ListExperiments returns all experiments with optional status filter.
func (e *Engine) ListExperiments(statusFilter string) []Experiment {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []Experiment
	for _, exp := range e.experiments {
		if statusFilter == "" || string(exp.Status) == statusFilter {
			result = append(result, *exp)
		}
	}
	return result
}

// ListScenarios returns available pre-built scenarios.
func (e *Engine) ListScenarios(category string) []Scenario {
	if category == "" {
		return e.scenarios
	}
	var result []Scenario
	for _, s := range e.scenarios {
		if s.Category == category {
			result = append(result, s)
		}
	}
	return result
}

// GetScenario returns a specific scenario by name.
func (e *Engine) GetScenario(name string) (*Scenario, error) {
	for _, s := range e.scenarios {
		if s.Name == name {
			return &s, nil
		}
	}
	return nil, fmt.Errorf("scenario %q not found", name)
}

func (e *Engine) generateHealthReport(exp *Experiment) *HealthReport {
	report := &HealthReport{
		OverallStatus:    "recovered",
		PreChaosMetrics:  map[string]any{"status": "healthy"},
		PostChaosMetrics: map[string]any{"status": "recovering"},
		GeneratedAt:      time.Now().UTC().Format(time.RFC3339),
	}

	if exp.StartedAt != "" && exp.CompletedAt != "" {
		if start, err := time.Parse(time.RFC3339, exp.StartedAt); err == nil {
			if end, err := time.Parse(time.RFC3339, exp.CompletedAt); err == nil {
				report.RecoveryTimeMs = end.Sub(start).Milliseconds()
			}
		}
	}

	// Generate findings based on experiment type
	switch exp.Type {
	case TypeBrokerOutage:
		report.Findings = append(report.Findings, Finding{
			Severity: "info", Category: "availability",
			Description: "Broker outage simulation completed",
		})
		report.Recommendations = append(report.Recommendations,
			"Ensure replication factor >= 3 for critical topics",
			"Configure min.insync.replicas >= 2",
		)
	case TypeSlowConsumer:
		report.Findings = append(report.Findings, Finding{
			Severity: "warning", Category: "performance",
			Description: "Consumer lag increased during slow consumer simulation",
		})
		report.Recommendations = append(report.Recommendations,
			"Consider increasing consumer parallelism",
			"Implement backpressure handling in consumer applications",
		)
	case TypeRebalanceStorm:
		report.Findings = append(report.Findings, Finding{
			Severity: "warning", Category: "stability",
			Description: "Multiple rebalances detected during storm simulation",
		})
		report.Recommendations = append(report.Recommendations,
			"Increase session.timeout.ms for consumer groups",
			"Use static group membership where possible",
		)
	case TypeSchemaCorruption:
		report.Findings = append(report.Findings, Finding{
			Severity: "critical", Category: "data_integrity",
			Description: "Schema incompatibility detected during corruption test",
		})
		report.Recommendations = append(report.Recommendations,
			"Enable schema compatibility checks at registry level",
			"Implement dead letter queue for deserialization failures",
		)
	default:
		report.Findings = append(report.Findings, Finding{
			Severity: "info", Category: "general",
			Description: fmt.Sprintf("Chaos experiment %s completed", exp.Type),
		})
	}

	for _, item := range exp.AffectedItems {
		if !item.Recovered {
			report.OverallStatus = "degraded"
			report.Findings = append(report.Findings, Finding{
				Severity:    "critical",
				Category:    "recovery",
				Description: fmt.Sprintf("%s %q has not recovered", item.Type, item.Name),
			})
		}
	}

	return report
}
