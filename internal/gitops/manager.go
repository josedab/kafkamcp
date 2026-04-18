package gitops

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
)

// DesiredState represents declarative configuration for Kafka resources.
type DesiredState struct {
	Version  string             `json:"version" yaml:"version"`
	Topics   []TopicSpec        `json:"topics" yaml:"topics"`
	Schemas  []SchemaSpec       `json:"schemas,omitempty" yaml:"schemas,omitempty"`
	ACLs     []ACLSpec          `json:"acls,omitempty" yaml:"acls,omitempty"`
}

// TopicSpec is a declarative topic definition.
type TopicSpec struct {
	Name              string            `json:"name" yaml:"name"`
	Partitions        int               `json:"partitions" yaml:"partitions"`
	ReplicationFactor int               `json:"replication_factor" yaml:"replication_factor"`
	Configs           map[string]string `json:"configs,omitempty" yaml:"configs,omitempty"`
}

// SchemaSpec is a declarative schema definition.
type SchemaSpec struct {
	Subject       string `json:"subject" yaml:"subject"`
	SchemaType    string `json:"schema_type" yaml:"schema_type"` // AVRO, PROTOBUF, JSON
	Schema        string `json:"schema" yaml:"schema"`
	Compatibility string `json:"compatibility,omitempty" yaml:"compatibility,omitempty"`
}

// ACLSpec is a declarative ACL definition.
type ACLSpec struct {
	Principal    string `json:"principal" yaml:"principal"`
	ResourceType string `json:"resource_type" yaml:"resource_type"`
	ResourceName string `json:"resource_name" yaml:"resource_name"`
	PatternType  string `json:"pattern_type" yaml:"pattern_type"` // LITERAL, PREFIXED
	Operation    string `json:"operation" yaml:"operation"`
	Permission   string `json:"permission" yaml:"permission"` // ALLOW, DENY
}

// DiffResult contains the differences between desired and live state.
type DiffResult struct {
	TopicsToCreate  []TopicSpec       `json:"topics_to_create"`
	TopicsToUpdate  []TopicDiff       `json:"topics_to_update"`
	TopicsToDelete  []string          `json:"topics_to_delete,omitempty"`
	SchemasToCreate []SchemaSpec      `json:"schemas_to_create,omitempty"`
	SchemasToUpdate []SchemaSpec      `json:"schemas_to_update,omitempty"`
	ACLsToCreate    []ACLSpec         `json:"acls_to_create,omitempty"`
	ACLsToDelete    []ACLSpec         `json:"acls_to_delete,omitempty"`
	TotalChanges    int               `json:"total_changes"`
	Summary         string            `json:"summary"`
}

// TopicDiff shows differences for a single topic.
type TopicDiff struct {
	Name           string            `json:"name"`
	ConfigChanges  map[string]Change `json:"config_changes,omitempty"`
	PartitionChange *Change          `json:"partition_change,omitempty"`
}

// Change represents a single config change.
type Change struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// MigrationPlan is an ordered list of operations to reach desired state.
type MigrationPlan struct {
	Steps     []MigrationStep `json:"steps"`
	DryRun    bool            `json:"dry_run"`
	Validated bool            `json:"validated"`
}

// MigrationStep is a single migration operation.
type MigrationStep struct {
	Order       int               `json:"order"`
	Action      string            `json:"action"` // create_topic, update_topic, delete_topic, register_schema, create_acl
	Resource    string            `json:"resource"`
	Description string            `json:"description"`
	Params      map[string]any    `json:"params"`
	Reversible  bool              `json:"reversible"`
}

// LiveState represents the current state from a Kafka cluster.
type LiveState struct {
	Topics map[string]TopicSpec
}

// Manager manages GitOps configuration operations.
type Manager struct {
	logger *slog.Logger
	mu     sync.RWMutex
}

// NewManager creates a new GitOps manager.
func NewManager(logger *slog.Logger) *Manager {
	return &Manager{logger: logger}
}

// ValidateDesiredState checks a desired state definition for errors.
func (m *Manager) ValidateDesiredState(desired DesiredState) []string {
	var errors []string

	if desired.Version == "" {
		errors = append(errors, "version is required")
	}

	seen := make(map[string]bool)
	for i, t := range desired.Topics {
		if t.Name == "" {
			errors = append(errors, fmt.Sprintf("topics[%d]: name is required", i))
		}
		if seen[t.Name] {
			errors = append(errors, fmt.Sprintf("topics[%d]: duplicate topic name %q", i, t.Name))
		}
		seen[t.Name] = true
		if t.Partitions < 1 {
			errors = append(errors, fmt.Sprintf("topics[%d] %q: partitions must be >= 1", i, t.Name))
		}
		if t.ReplicationFactor < 1 {
			errors = append(errors, fmt.Sprintf("topics[%d] %q: replication_factor must be >= 1", i, t.Name))
		}
	}

	for i, s := range desired.Schemas {
		if s.Subject == "" {
			errors = append(errors, fmt.Sprintf("schemas[%d]: subject is required", i))
		}
		if s.Schema == "" {
			errors = append(errors, fmt.Sprintf("schemas[%d]: schema definition is required", i))
		}
		validTypes := map[string]bool{"AVRO": true, "PROTOBUF": true, "JSON": true}
		if s.SchemaType != "" && !validTypes[s.SchemaType] {
			errors = append(errors, fmt.Sprintf("schemas[%d]: invalid schema_type %q", i, s.SchemaType))
		}
	}

	return errors
}

// Diff computes differences between desired and live state.
func (m *Manager) Diff(desired DesiredState, live LiveState) (*DiffResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := &DiffResult{}

	desiredMap := make(map[string]TopicSpec)
	for _, t := range desired.Topics {
		desiredMap[t.Name] = t
	}

	// Find topics to create or update
	for name, desiredTopic := range desiredMap {
		liveTopic, exists := live.Topics[name]
		if !exists {
			result.TopicsToCreate = append(result.TopicsToCreate, desiredTopic)
			continue
		}

		diff := TopicDiff{Name: name, ConfigChanges: make(map[string]Change)}

		// Check partition count (can only increase)
		if desiredTopic.Partitions > liveTopic.Partitions {
			diff.PartitionChange = &Change{
				From: fmt.Sprintf("%d", liveTopic.Partitions),
				To:   fmt.Sprintf("%d", desiredTopic.Partitions),
			}
		}

		// Check config changes
		for k, v := range desiredTopic.Configs {
			liveVal := liveTopic.Configs[k]
			if liveVal != v {
				diff.ConfigChanges[k] = Change{From: liveVal, To: v}
			}
		}

		if diff.PartitionChange != nil || len(diff.ConfigChanges) > 0 {
			result.TopicsToUpdate = append(result.TopicsToUpdate, diff)
		}
	}

	// Find topics to delete (in live but not in desired) - optional, only if explicitly managed
	for name := range live.Topics {
		if _, ok := desiredMap[name]; !ok {
			// Only flag for deletion if not an internal topic
			if !strings.HasPrefix(name, "__") && !strings.HasPrefix(name, "_") {
				result.TopicsToDelete = append(result.TopicsToDelete, name)
			}
		}
	}

	result.SchemasToCreate = desired.Schemas
	result.ACLsToCreate = desired.ACLs
	result.TotalChanges = len(result.TopicsToCreate) + len(result.TopicsToUpdate) +
		len(result.TopicsToDelete) + len(result.SchemasToCreate) + len(result.ACLsToCreate)
	result.Summary = m.generateSummary(result)

	return result, nil
}

// GeneratePlan creates an ordered migration plan from a diff result.
func (m *Manager) GeneratePlan(diff *DiffResult, dryRun bool) *MigrationPlan {
	plan := &MigrationPlan{DryRun: dryRun, Validated: true}
	order := 1

	// Create topics first
	for _, t := range diff.TopicsToCreate {
		plan.Steps = append(plan.Steps, MigrationStep{
			Order:       order,
			Action:      "create_topic",
			Resource:    t.Name,
			Description: fmt.Sprintf("Create topic %q with %d partitions, RF=%d", t.Name, t.Partitions, t.ReplicationFactor),
			Params:      map[string]any{"name": t.Name, "partitions": t.Partitions, "replication_factor": t.ReplicationFactor, "configs": t.Configs},
			Reversible:  true,
		})
		order++
	}

	// Update topics
	for _, td := range diff.TopicsToUpdate {
		desc := fmt.Sprintf("Update topic %q:", td.Name)
		params := map[string]any{"name": td.Name}

		if td.PartitionChange != nil {
			desc += fmt.Sprintf(" partitions %s→%s", td.PartitionChange.From, td.PartitionChange.To)
		}
		configs := make(map[string]string)
		for k, v := range td.ConfigChanges {
			configs[k] = v.To
			desc += fmt.Sprintf(" %s=%s", k, v.To)
		}
		if len(configs) > 0 {
			params["configs"] = configs
		}

		plan.Steps = append(plan.Steps, MigrationStep{
			Order:       order,
			Action:      "update_topic",
			Resource:    td.Name,
			Description: desc,
			Params:      params,
			Reversible:  td.PartitionChange == nil, // partition increase is not reversible
		})
		order++
	}

	// Schemas
	for _, s := range diff.SchemasToCreate {
		plan.Steps = append(plan.Steps, MigrationStep{
			Order:       order,
			Action:      "register_schema",
			Resource:    s.Subject,
			Description: fmt.Sprintf("Register schema for subject %q (%s)", s.Subject, s.SchemaType),
			Params:      map[string]any{"subject": s.Subject, "schema_type": s.SchemaType, "schema": s.Schema},
			Reversible:  false,
		})
		order++
	}

	// Sort by order
	sort.Slice(plan.Steps, func(i, j int) bool { return plan.Steps[i].Order < plan.Steps[j].Order })

	return plan
}

func (m *Manager) generateSummary(diff *DiffResult) string {
	parts := []string{}
	if n := len(diff.TopicsToCreate); n > 0 {
		parts = append(parts, fmt.Sprintf("%d topics to create", n))
	}
	if n := len(diff.TopicsToUpdate); n > 0 {
		parts = append(parts, fmt.Sprintf("%d topics to update", n))
	}
	if n := len(diff.TopicsToDelete); n > 0 {
		parts = append(parts, fmt.Sprintf("%d topics to delete", n))
	}
	if n := len(diff.SchemasToCreate); n > 0 {
		parts = append(parts, fmt.Sprintf("%d schemas to register", n))
	}
	if n := len(diff.ACLsToCreate); n > 0 {
		parts = append(parts, fmt.Sprintf("%d ACLs to create", n))
	}
	if len(parts) == 0 {
		return "No changes detected - cluster is in sync with desired state"
	}
	return strings.Join(parts, ", ")
}
