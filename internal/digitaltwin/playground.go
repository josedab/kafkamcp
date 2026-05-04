package digitaltwin

import (
	"fmt"
	"time"
)

// Scenario defines a pre-built simulation scenario.
type Scenario struct {
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Category    string         `json:"category"` // scaling, resilience, migration, optimization
	Operations  []SimulationOp `json:"operations"`
	Validations []Validation   `json:"validations"`
}

// Validation defines an expected outcome after running a scenario.
type Validation struct {
	Description string `json:"description"`
	Check       string `json:"check"` // topic_exists, topic_not_exists, partition_count, config_value
	Resource    string `json:"resource"`
	Field       string `json:"field,omitempty"`
	Expected    string `json:"expected"`
}

// ScenarioResult is the outcome of running a scenario.
type ScenarioResult struct {
	ScenarioID    string             `json:"scenario_id"`
	ScenarioName  string             `json:"scenario_name"`
	TwinID        string             `json:"twin_id"`
	Operations    []SimulationResult `json:"operations"`
	Validations   []ValidationResult `json:"validations"`
	AllPassed     bool               `json:"all_passed"`
	ExecutedAt    time.Time          `json:"executed_at"`
}

// ValidationResult is the result of checking one validation.
type ValidationResult struct {
	Description string `json:"description"`
	Passed      bool   `json:"passed"`
	Expected    string `json:"expected"`
	Actual      string `json:"actual"`
}

// ImpactAnalysis predicts the impact of operations on the cluster.
type ImpactAnalysis struct {
	TwinID              string           `json:"twin_id"`
	Operations          []SimulationOp   `json:"operations"`
	RiskLevel           string           `json:"risk_level"` // low, medium, high, critical
	AffectedTopics      []string         `json:"affected_topics"`
	AffectedGroups      []string         `json:"affected_groups"`
	PartitionChanges    int              `json:"partition_changes"`
	DataLossRisk        bool             `json:"data_loss_risk"`
	DowntimeRisk        bool             `json:"downtime_risk"`
	Warnings            []string         `json:"warnings"`
	Recommendations     []string         `json:"recommendations"`
	AnalyzedAt          time.Time        `json:"analyzed_at"`
}

// initScenarios creates built-in simulation scenarios.
func (e *Engine) initScenarios() []Scenario {
	return []Scenario{
		{
			ID: "scenario-scale-partitions", Name: "Scale Partitions", Category: "scaling",
			Description: "Test the impact of doubling partition count on a topic",
			Operations: []SimulationOp{
				{Type: "alter_partitions", Params: map[string]any{"name": "${topic}", "partitions": "${target_partitions}"}},
				{Type: "rebalance", Params: map[string]any{}},
			},
			Validations: []Validation{
				{Description: "Topic partition count updated", Check: "partition_count", Resource: "${topic}", Expected: "${target_partitions}"},
			},
		},
		{
			ID: "scenario-add-topic", Name: "Add New Topic", Category: "migration",
			Description: "Simulate adding a new topic with specific configuration",
			Operations: []SimulationOp{
				{Type: "create_topic", Params: map[string]any{"name": "${topic}", "partitions": 6, "replication_factor": 3}},
			},
			Validations: []Validation{
				{Description: "Topic created successfully", Check: "topic_exists", Resource: "${topic}"},
			},
		},
		{
			ID: "scenario-retention-change", Name: "Retention Policy Change", Category: "optimization",
			Description: "Test impact of reducing retention on storage",
			Operations: []SimulationOp{
				{Type: "alter_config", Params: map[string]any{"name": "${topic}", "config_key": "retention.ms", "config_value": "${retention_ms}"}},
			},
			Validations: []Validation{
				{Description: "Retention config updated", Check: "config_value", Resource: "${topic}", Field: "retention.ms", Expected: "${retention_ms}"},
			},
		},
		{
			ID: "scenario-broker-loss", Name: "Broker Loss Simulation", Category: "resilience",
			Description: "Remove a broker and verify topic availability",
			Operations: []SimulationOp{
				{Type: "rebalance", Params: map[string]any{}},
			},
		},
	}
}

// GetScenarios returns all built-in scenarios.
func (e *Engine) GetScenarios(category string) []Scenario {
	scenarios := e.initScenarios()
	if category == "" {
		return scenarios
	}
	var filtered []Scenario
	for _, s := range scenarios {
		if s.Category == category {
			filtered = append(filtered, s)
		}
	}
	return filtered
}

// RunScenario executes a scenario against a twin.
func (e *Engine) RunScenario(twinID, scenarioID string, params map[string]any) (*ScenarioResult, error) {
	scenarios := e.initScenarios()
	var scenario *Scenario
	for i := range scenarios {
		if scenarios[i].ID == scenarioID {
			scenario = &scenarios[i]
			break
		}
	}
	if scenario == nil {
		return nil, fmt.Errorf("scenario %q not found", scenarioID)
	}

	result := &ScenarioResult{
		ScenarioID:   scenario.ID,
		ScenarioName: scenario.Name,
		TwinID:       twinID,
		AllPassed:    true,
		ExecutedAt:   time.Now(),
	}

	// Execute operations with parameter interpolation
	for _, op := range scenario.Operations {
		resolvedParams := make(map[string]any)
		for k, v := range op.Params {
			if strVal, ok := v.(string); ok {
				resolved := interpolateParam(strVal, params)
				// Try to preserve numeric types from params
				if resolved != strVal {
					if numVal, found := findParamValue(strVal, params); found {
						resolvedParams[k] = numVal
						continue
					}
				}
				resolvedParams[k] = resolved
			} else {
				resolvedParams[k] = v
			}
		}

		simResult, err := e.SimulateOp(twinID, SimulationOp{Type: op.Type, Params: resolvedParams})
		if err != nil {
			return nil, fmt.Errorf("operation %s failed: %w", op.Type, err)
		}
		result.Operations = append(result.Operations, *simResult)
		if !simResult.Success {
			result.AllPassed = false
		}
	}

	// Run validations
	e.mu.RLock()
	twin := e.twins[twinID]
	e.mu.RUnlock()
	if twin != nil {
		for _, v := range scenario.Validations {
			vResult := e.checkValidation(twin, v, params)
			result.Validations = append(result.Validations, vResult)
			if !vResult.Passed {
				result.AllPassed = false
			}
		}
	}

	return result, nil
}

// AnalyzeImpact predicts the impact of operations without executing them.
func (e *Engine) AnalyzeImpact(twinID string, operations []SimulationOp) (*ImpactAnalysis, error) {
	e.mu.RLock()
	twin, ok := e.twins[twinID]
	if !ok {
		e.mu.RUnlock()
		return nil, fmt.Errorf("twin %q not found", twinID)
	}
	e.mu.RUnlock()

	analysis := &ImpactAnalysis{
		TwinID:     twinID,
		Operations: operations,
		RiskLevel:  "low",
		AnalyzedAt: time.Now(),
	}

	affectedTopics := make(map[string]bool)
	affectedGroups := make(map[string]bool)

	for _, op := range operations {
		topicName, _ := op.Params["name"].(string)
		if topicName != "" {
			affectedTopics[topicName] = true
		}

		switch op.Type {
		case "delete_topic":
			analysis.DataLossRisk = true
			analysis.RiskLevel = "critical"
			analysis.Warnings = append(analysis.Warnings, fmt.Sprintf("Deleting topic %q will cause permanent data loss", topicName))
			// Check if consumers depend on this topic
			for gid := range twin.ConsumerGroups {
				affectedGroups[gid] = true
			}

		case "alter_partitions":
			newParts := intParam(op.Params, "partitions", 0)
			if topic, exists := twin.Topics[topicName]; exists {
				analysis.PartitionChanges += newParts - topic.Partitions
				if newParts > topic.Partitions*2 {
					analysis.Warnings = append(analysis.Warnings, fmt.Sprintf("Large partition increase for %q may cause key reordering", topicName))
					if analysis.RiskLevel == "low" {
						analysis.RiskLevel = "medium"
					}
				}
			}

		case "rebalance":
			analysis.DowntimeRisk = true
			analysis.Warnings = append(analysis.Warnings, "Rebalance will cause temporary consumer group disruption")
			if analysis.RiskLevel == "low" {
				analysis.RiskLevel = "medium"
			}
		}
	}

	for t := range affectedTopics {
		analysis.AffectedTopics = append(analysis.AffectedTopics, t)
	}
	for g := range affectedGroups {
		analysis.AffectedGroups = append(analysis.AffectedGroups, g)
	}

	// Generate recommendations
	if analysis.DataLossRisk {
		analysis.Recommendations = append(analysis.Recommendations, "Create a backup before proceeding with destructive operations")
	}
	if analysis.DowntimeRisk {
		analysis.Recommendations = append(analysis.Recommendations, "Schedule during maintenance window to minimize consumer impact")
	}
	if analysis.PartitionChanges > 0 {
		analysis.Recommendations = append(analysis.Recommendations, "Monitor consumer lag after partition changes")
	}

	return analysis, nil
}

// CreatePlayground creates a fresh twin from a simple topology definition.
func (e *Engine) CreatePlayground(clusterName string, topicDefs map[string]int, brokerCount int) (*ClusterSnapshot, error) {
	if clusterName == "" {
		clusterName = "playground"
	}
	if brokerCount <= 0 {
		brokerCount = 3
	}

	topics := make(map[string]TopicState)
	for name, partitions := range topicDefs {
		if partitions <= 0 {
			partitions = 3
		}
		topics[name] = TopicState{
			Name:              name,
			Partitions:        partitions,
			ReplicationFactor: min(3, brokerCount),
			Configs:           make(map[string]string),
		}
	}

	brokers := make([]BrokerState, brokerCount)
	for i := 0; i < brokerCount; i++ {
		brokers[i] = BrokerState{
			ID: i, Host: fmt.Sprintf("broker-%d", i), Port: 9092 + i,
		}
	}

	snap := e.CaptureSnapshot(clusterName, topics, nil, brokers)
	twin, err := e.CreateTwin(snap.ID)
	if err != nil {
		return nil, err
	}

	return twin, nil
}

func (e *Engine) checkValidation(twin *ClusterSnapshot, v Validation, params map[string]any) ValidationResult {
	resource := interpolateParam(v.Resource, params)
	expected := interpolateParam(v.Expected, params)

	vr := ValidationResult{
		Description: v.Description,
		Expected:    expected,
	}

	switch v.Check {
	case "topic_exists":
		_, exists := twin.Topics[resource]
		vr.Passed = exists
		if exists {
			vr.Actual = "exists"
		} else {
			vr.Actual = "not found"
		}

	case "topic_not_exists":
		_, exists := twin.Topics[resource]
		vr.Passed = !exists
		if exists {
			vr.Actual = "exists"
		} else {
			vr.Actual = "not found"
		}

	case "partition_count":
		topic, exists := twin.Topics[resource]
		if exists {
			vr.Actual = fmt.Sprintf("%d", topic.Partitions)
			vr.Passed = vr.Actual == expected
		} else {
			vr.Actual = "topic not found"
		}

	case "config_value":
		topic, exists := twin.Topics[resource]
		field := interpolateParam(v.Field, params)
		if exists {
			vr.Actual = topic.Configs[field]
			vr.Passed = vr.Actual == expected
		} else {
			vr.Actual = "topic not found"
		}
	}

	return vr
}

func interpolateParam(template string, params map[string]any) string {
	if params == nil {
		return template
	}
	result := template
	for k, v := range params {
		result = replaceAll(result, "${"+k+"}", fmt.Sprintf("%v", v))
	}
	return result
}

// findParamValue returns the original param value if the template is exactly "${key}".
func findParamValue(template string, params map[string]any) (any, bool) {
	for k, v := range params {
		if template == "${"+k+"}" {
			return v, true
		}
	}
	return nil, false
}

func replaceAll(s, old, new string) string {
	for {
		i := indexOf(s, old)
		if i < 0 {
			return s
		}
		s = s[:i] + new + s[i+len(old):]
	}
}

func indexOf(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// ResetTwin restores a twin to its source snapshot state.
func (e *Engine) ResetTwin(twinID string) (*ClusterSnapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	twin, ok := e.twins[twinID]
	if !ok {
		return nil, fmt.Errorf("twin %q not found", twinID)
	}

	var source *ClusterSnapshot
	for _, snap := range e.snapshots {
		if snap.SourceCluster == twin.SourceCluster {
			source = snap
			break
		}
	}
	if source == nil {
		return nil, fmt.Errorf("source snapshot not found")
	}

	// Deep copy source back into twin
	twin.Topics = make(map[string]TopicState, len(source.Topics))
	for k, v := range source.Topics {
		configs := make(map[string]string)
		for ck, cv := range v.Configs {
			configs[ck] = cv
		}
		v.Configs = configs
		twin.Topics[k] = v
	}
	twin.ConsumerGroups = make(map[string]ConsumerGroupState, len(source.ConsumerGroups))
	for k, v := range source.ConsumerGroups {
		twin.ConsumerGroups[k] = v
	}
	twin.Brokers = make([]BrokerState, len(source.Brokers))
	copy(twin.Brokers, source.Brokers)

	// Clear simulation history
	delete(e.results, twinID)

	return twin, nil
}
