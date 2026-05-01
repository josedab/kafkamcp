// Package digitaltwin provides an in-memory simulation of a Kafka cluster
// that allows agents to test operations before executing on production.
package digitaltwin

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
	"github.com/kafkamcp/kafkamcp/internal/randid"
)

// TopicState represents a topic in the simulated cluster.
type TopicState struct {
	Name              string            `json:"name"`
	Partitions        int               `json:"partitions"`
	ReplicationFactor int               `json:"replication_factor"`
	Configs           map[string]string `json:"configs,omitempty"`
	MessageCount      int64             `json:"message_count"`
	SizeBytes         int64             `json:"size_bytes"`
}

// ConsumerGroupState represents a consumer group in the simulation.
type ConsumerGroupState struct {
	GroupID     string                 `json:"group_id"`
	State      string                 `json:"state"`
	Members    int                    `json:"members"`
	Offsets    map[string]int64       `json:"offsets"`
	TotalLag   int64                  `json:"total_lag"`
}

// BrokerState represents a broker node.
type BrokerState struct {
	ID        int    `json:"id"`
	Host      string `json:"host"`
	Port      int    `json:"port"`
	Rack      string `json:"rack,omitempty"`
	Partitions int   `json:"partitions"`
}

// ClusterSnapshot captures the full state of a cluster.
type ClusterSnapshot struct {
	ID             string               `json:"id"`
	SourceCluster  string               `json:"source_cluster"`
	Topics         map[string]TopicState `json:"topics"`
	ConsumerGroups map[string]ConsumerGroupState `json:"consumer_groups"`
	Brokers        []BrokerState        `json:"brokers"`
	CapturedAt     time.Time            `json:"captured_at"`
}

// SimulationOp represents an operation to simulate.
type SimulationOp struct {
	Type   string         `json:"type"`
	Params map[string]any `json:"params"`
}

// SimulationResult is the outcome of simulating an operation.
type SimulationResult struct {
	Operation   SimulationOp    `json:"operation"`
	Success     bool            `json:"success"`
	Changes     []Change        `json:"changes,omitempty"`
	Warnings    []string        `json:"warnings,omitempty"`
	Error       string          `json:"error,omitempty"`
	SimulatedAt time.Time       `json:"simulated_at"`
}

// Change describes a state modification from a simulated operation.
type Change struct {
	Resource string `json:"resource"`
	Field    string `json:"field"`
	OldValue string `json:"old_value"`
	NewValue string `json:"new_value"`
}

// DiffResult compares two cluster states.
type DiffResult struct {
	AddedTopics    []string `json:"added_topics,omitempty"`
	RemovedTopics  []string `json:"removed_topics,omitempty"`
	ModifiedTopics []TopicDiff `json:"modified_topics,omitempty"`
	AddedGroups    []string `json:"added_groups,omitempty"`
	RemovedGroups  []string `json:"removed_groups,omitempty"`
	TotalChanges   int      `json:"total_changes"`
}

// TopicDiff describes changes to a topic between two states.
type TopicDiff struct {
	Name    string   `json:"name"`
	Changes []Change `json:"changes"`
}

// MigrationPlan describes steps to apply twin changes to production.
type MigrationPlan struct {
	Steps    []MigrationStep `json:"steps"`
	Warnings []string        `json:"warnings,omitempty"`
	Reversible bool          `json:"reversible"`
}

// MigrationStep is a single step in a migration plan.
type MigrationStep struct {
	Order       int            `json:"order"`
	Operation   string         `json:"operation"`
	Resource    string         `json:"resource"`
	Params      map[string]any `json:"params"`
	Reversible  bool           `json:"reversible"`
	Description string         `json:"description"`
}

// Engine manages cluster digital twins.
type Engine struct {
	mu        sync.RWMutex
	snapshots map[string]*ClusterSnapshot
	twins     map[string]*ClusterSnapshot // active twins (mutable copies)
	results   map[string][]SimulationResult
	logger    *slog.Logger
}

// NewEngine creates a new digital twin engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		snapshots: make(map[string]*ClusterSnapshot),
		twins:     make(map[string]*ClusterSnapshot),
		results:   make(map[string][]SimulationResult),
		logger:    logger,
	}
}

// CaptureSnapshot creates a snapshot from provided cluster state.
func (e *Engine) CaptureSnapshot(clusterName string, topics map[string]TopicState, groups map[string]ConsumerGroupState, brokers []BrokerState) *ClusterSnapshot {
	e.mu.Lock()
	defer e.mu.Unlock()

	snapshot := &ClusterSnapshot{
		ID:             randid.New("snap"),
		SourceCluster:  clusterName,
		Topics:         topics,
		ConsumerGroups: groups,
		Brokers:        brokers,
		CapturedAt:     time.Now(),
	}
	e.snapshots[snapshot.ID] = snapshot
	return snapshot
}

// CreateTwin creates a mutable copy of a snapshot for simulation.
func (e *Engine) CreateTwin(snapshotID string) (*ClusterSnapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	snap, ok := e.snapshots[snapshotID]
	if !ok {
		return nil, fmt.Errorf("snapshot %q not found", snapshotID)
	}

	twin := &ClusterSnapshot{
		ID:            randid.New("twin"),
		SourceCluster: snap.SourceCluster,
		Topics:        make(map[string]TopicState, len(snap.Topics)),
		ConsumerGroups: make(map[string]ConsumerGroupState, len(snap.ConsumerGroups)),
		Brokers:       make([]BrokerState, len(snap.Brokers)),
		CapturedAt:    snap.CapturedAt,
	}

	for k, v := range snap.Topics {
		configs := make(map[string]string)
		for ck, cv := range v.Configs {
			configs[ck] = cv
		}
		v.Configs = configs
		twin.Topics[k] = v
	}
	for k, v := range snap.ConsumerGroups {
		offsets := make(map[string]int64)
		for ok, ov := range v.Offsets {
			offsets[ok] = ov
		}
		v.Offsets = offsets
		twin.ConsumerGroups[k] = v
	}
	copy(twin.Brokers, snap.Brokers)

	e.twins[twin.ID] = twin
	return twin, nil
}

// SimulateOp simulates an operation against a twin.
func (e *Engine) SimulateOp(twinID string, op SimulationOp) (*SimulationResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	twin, ok := e.twins[twinID]
	if !ok {
		return nil, fmt.Errorf("twin %q not found", twinID)
	}

	result := &SimulationResult{
		Operation:   op,
		SimulatedAt: time.Now(),
	}

	switch op.Type {
	case "create_topic":
		result = e.simCreateTopic(twin, op.Params)
	case "delete_topic":
		result = e.simDeleteTopic(twin, op.Params)
	case "alter_partitions":
		result = e.simAlterPartitions(twin, op.Params)
	case "alter_config":
		result = e.simAlterConfig(twin, op.Params)
	case "rebalance":
		result = e.simRebalance(twin, op.Params)
	default:
		result.Success = false
		result.Error = fmt.Sprintf("unknown operation type: %s", op.Type)
	}

	result.Operation = op
	result.SimulatedAt = time.Now()
	e.results[twinID] = append(e.results[twinID], *result)
	return result, nil
}

// Diff compares a twin against its source snapshot.
func (e *Engine) Diff(twinID string) (*DiffResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	twin, ok := e.twins[twinID]
	if !ok {
		return nil, fmt.Errorf("twin %q not found", twinID)
	}

	// Find original snapshot
	var source *ClusterSnapshot
	for _, snap := range e.snapshots {
		if snap.SourceCluster == twin.SourceCluster {
			source = snap
			break
		}
	}
	if source == nil {
		return nil, fmt.Errorf("source snapshot not found for twin")
	}

	diff := &DiffResult{}

	// Compare topics
	for name := range twin.Topics {
		if _, exists := source.Topics[name]; !exists {
			diff.AddedTopics = append(diff.AddedTopics, name)
		}
	}
	for name := range source.Topics {
		if _, exists := twin.Topics[name]; !exists {
			diff.RemovedTopics = append(diff.RemovedTopics, name)
		}
	}
	for name, twinTopic := range twin.Topics {
		if srcTopic, exists := source.Topics[name]; exists {
			var changes []Change
			if twinTopic.Partitions != srcTopic.Partitions {
				changes = append(changes, Change{
					Resource: name, Field: "partitions",
					OldValue: fmt.Sprintf("%d", srcTopic.Partitions),
					NewValue: fmt.Sprintf("%d", twinTopic.Partitions),
				})
			}
			if twinTopic.ReplicationFactor != srcTopic.ReplicationFactor {
				changes = append(changes, Change{
					Resource: name, Field: "replication_factor",
					OldValue: fmt.Sprintf("%d", srcTopic.ReplicationFactor),
					NewValue: fmt.Sprintf("%d", twinTopic.ReplicationFactor),
				})
			}
			if len(changes) > 0 {
				diff.ModifiedTopics = append(diff.ModifiedTopics, TopicDiff{Name: name, Changes: changes})
			}
		}
	}

	// Compare groups
	for name := range twin.ConsumerGroups {
		if _, exists := source.ConsumerGroups[name]; !exists {
			diff.AddedGroups = append(diff.AddedGroups, name)
		}
	}
	for name := range source.ConsumerGroups {
		if _, exists := twin.ConsumerGroups[name]; !exists {
			diff.RemovedGroups = append(diff.RemovedGroups, name)
		}
	}

	diff.TotalChanges = len(diff.AddedTopics) + len(diff.RemovedTopics) +
		len(diff.ModifiedTopics) + len(diff.AddedGroups) + len(diff.RemovedGroups)

	return diff, nil
}

// GenerateMigrationPlan creates steps to apply twin changes to production.
func (e *Engine) GenerateMigrationPlan(twinID string) (*MigrationPlan, error) {
	diff, err := e.Diff(twinID)
	if err != nil {
		return nil, err
	}

	e.mu.RLock()
	twin := e.twins[twinID]
	e.mu.RUnlock()

	plan := &MigrationPlan{Reversible: true}
	order := 1

	// Create topics first
	for _, name := range diff.AddedTopics {
		topic := twin.Topics[name]
		plan.Steps = append(plan.Steps, MigrationStep{
			Order:       order,
			Operation:   "create_topic",
			Resource:    name,
			Params:      map[string]any{"partitions": topic.Partitions, "replication_factor": topic.ReplicationFactor},
			Reversible:  true,
			Description: fmt.Sprintf("Create topic %s with %d partitions", name, topic.Partitions),
		})
		order++
	}

	// Modify topics
	for _, mod := range diff.ModifiedTopics {
		for _, change := range mod.Changes {
			plan.Steps = append(plan.Steps, MigrationStep{
				Order:       order,
				Operation:   "alter_" + change.Field,
				Resource:    mod.Name,
				Params:      map[string]any{change.Field: change.NewValue},
				Reversible:  change.Field != "partitions",
				Description: fmt.Sprintf("Change %s.%s from %s to %s", mod.Name, change.Field, change.OldValue, change.NewValue),
			})
			if change.Field == "partitions" {
				plan.Warnings = append(plan.Warnings, fmt.Sprintf("Partition increase for %s is irreversible", mod.Name))
				plan.Reversible = false
			}
			order++
		}
	}

	// Delete topics last
	for _, name := range diff.RemovedTopics {
		plan.Steps = append(plan.Steps, MigrationStep{
			Order:       order,
			Operation:   "delete_topic",
			Resource:    name,
			Params:      map[string]any{},
			Reversible:  false,
			Description: fmt.Sprintf("Delete topic %s", name),
		})
		plan.Warnings = append(plan.Warnings, fmt.Sprintf("Deleting topic %s is destructive and irreversible", name))
		plan.Reversible = false
		order++
	}

	return plan, nil
}

// GetTwin retrieves a twin.
func (e *Engine) GetTwin(twinID string) (*ClusterSnapshot, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	twin, ok := e.twins[twinID]
	if !ok {
		return nil, fmt.Errorf("twin %q not found", twinID)
	}
	return twin, nil
}

// ListTwins returns all active twins.
func (e *Engine) ListTwins() []*ClusterSnapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]*ClusterSnapshot, 0, len(e.twins))
	for _, t := range e.twins {
		result = append(result, t)
	}
	return result
}

// GetSimulationHistory returns all simulation results for a twin.
func (e *Engine) GetSimulationHistory(twinID string) []SimulationResult {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.results[twinID]
}

// --- Simulation helpers ---

func (e *Engine) simCreateTopic(twin *ClusterSnapshot, params map[string]any) *SimulationResult {
	name, _ := params["name"].(string)
	if name == "" {
		return &SimulationResult{Success: false, Error: "topic name is required"}
	}
	if _, exists := twin.Topics[name]; exists {
		return &SimulationResult{Success: false, Error: fmt.Sprintf("topic %q already exists", name)}
	}

	partitions := intParam(params, "partitions", 3)
	rf := intParam(params, "replication_factor", 1)

	if rf > len(twin.Brokers) && len(twin.Brokers) > 0 {
		return &SimulationResult{
			Success:  false,
			Error:    fmt.Sprintf("replication factor %d exceeds broker count %d", rf, len(twin.Brokers)),
			Warnings: []string{"Reduce replication_factor or add more brokers"},
		}
	}

	twin.Topics[name] = TopicState{
		Name: name, Partitions: partitions, ReplicationFactor: rf,
		Configs: make(map[string]string),
	}

	return &SimulationResult{
		Success: true,
		Changes: []Change{{Resource: name, Field: "created", NewValue: fmt.Sprintf("%d partitions, RF=%d", partitions, rf)}},
	}
}

func (e *Engine) simDeleteTopic(twin *ClusterSnapshot, params map[string]any) *SimulationResult {
	name, _ := params["name"].(string)
	if _, exists := twin.Topics[name]; !exists {
		return &SimulationResult{Success: false, Error: fmt.Sprintf("topic %q not found", name)}
	}

	delete(twin.Topics, name)
	return &SimulationResult{
		Success:  true,
		Changes:  []Change{{Resource: name, Field: "deleted", OldValue: "existed"}},
		Warnings: []string{"Data will be permanently lost"},
	}
}

func (e *Engine) simAlterPartitions(twin *ClusterSnapshot, params map[string]any) *SimulationResult {
	name, _ := params["name"].(string)
	topic, exists := twin.Topics[name]
	if !exists {
		return &SimulationResult{Success: false, Error: fmt.Sprintf("topic %q not found", name)}
	}

	newCount := intParam(params, "partitions", topic.Partitions)
	if newCount < topic.Partitions {
		return &SimulationResult{
			Success: false,
			Error:   "cannot decrease partition count",
		}
	}

	old := topic.Partitions
	topic.Partitions = newCount
	twin.Topics[name] = topic

	return &SimulationResult{
		Success: true,
		Changes: []Change{{Resource: name, Field: "partitions", OldValue: fmt.Sprintf("%d", old), NewValue: fmt.Sprintf("%d", newCount)}},
		Warnings: []string{"Partition increase may cause message reordering for existing keys"},
	}
}

func (e *Engine) simAlterConfig(twin *ClusterSnapshot, params map[string]any) *SimulationResult {
	name, _ := params["name"].(string)
	topic, exists := twin.Topics[name]
	if !exists {
		return &SimulationResult{Success: false, Error: fmt.Sprintf("topic %q not found", name)}
	}

	configKey, _ := params["config_key"].(string)
	configVal, _ := params["config_value"].(string)
	if configKey == "" {
		return &SimulationResult{Success: false, Error: "config_key is required"}
	}

	oldVal := topic.Configs[configKey]
	if topic.Configs == nil {
		topic.Configs = make(map[string]string)
	}
	topic.Configs[configKey] = configVal
	twin.Topics[name] = topic

	return &SimulationResult{
		Success: true,
		Changes: []Change{{Resource: name, Field: configKey, OldValue: oldVal, NewValue: configVal}},
	}
}

func (e *Engine) simRebalance(twin *ClusterSnapshot, params map[string]any) *SimulationResult {
	if len(twin.Brokers) == 0 {
		return &SimulationResult{Success: false, Error: "no brokers in twin"}
	}

	totalPartitions := 0
	for _, t := range twin.Topics {
		totalPartitions += t.Partitions
	}

	perBroker := totalPartitions / len(twin.Brokers)
	for i := range twin.Brokers {
		twin.Brokers[i].Partitions = perBroker
	}
	// Remainder goes to first broker
	if rem := totalPartitions % len(twin.Brokers); rem > 0 {
		twin.Brokers[0].Partitions += rem
	}

	return &SimulationResult{
		Success:  true,
		Changes:  []Change{{Resource: "cluster", Field: "partition_distribution", NewValue: fmt.Sprintf("%d partitions across %d brokers", totalPartitions, len(twin.Brokers))}},
		Warnings: []string{"Rebalancing may cause temporary consumer group disruption"},
	}
}

func intParam(params map[string]any, key string, def int) int {
	if v, ok := params[key]; ok {
		switch val := v.(type) {
		case float64:
			return int(val)
		case int:
			return val
		}
	}
	return def
}
