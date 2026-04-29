package smartroute

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// ToolNode represents a tool in the dependency graph.
type ToolNode struct {
	Name        string   `json:"name"`
	Category    string   `json:"category"`
	DependsOn   []string `json:"depends_on,omitempty"`
	Produces    []string `json:"produces,omitempty"`
	Description string   `json:"description"`
}

// IntentPattern maps a high-level intent to a chain of tools.
type IntentPattern struct {
	ID          string            `json:"id"`
	Pattern     []string          `json:"pattern"`
	Description string            `json:"description"`
	ToolChain   []ToolStep        `json:"tool_chain"`
	Priority    int               `json:"priority"`
}

// ToolStep represents a single step in an execution plan.
type ToolStep struct {
	ID         string            `json:"id"`
	Tool       string            `json:"tool"`
	Args       map[string]any    `json:"args,omitempty"`
	DependsOn  []string          `json:"depends_on,omitempty"`
	PassFrom   map[string]string `json:"pass_from,omitempty"`
	Optional   bool              `json:"optional"`
	Timeout    time.Duration     `json:"timeout,omitempty"`
}

// ExecutionPlan represents a composed chain of tool calls.
type ExecutionPlan struct {
	ID          string     `json:"id"`
	Intent      string     `json:"intent"`
	Description string     `json:"description"`
	Steps       []ToolStep `json:"steps"`
	CreatedAt   time.Time  `json:"created_at"`
	Status      string     `json:"status"`
}

// StepResult holds the result of executing a single step.
type StepResult struct {
	StepID    string         `json:"step_id"`
	Tool      string         `json:"tool"`
	Output    map[string]any `json:"output,omitempty"`
	Error     string         `json:"error,omitempty"`
	Duration  time.Duration  `json:"duration"`
	Status    string         `json:"status"`
}

// ExecutionResult holds the complete result of an execution plan.
type ExecutionResult struct {
	PlanID      string       `json:"plan_id"`
	Steps       []StepResult `json:"steps"`
	Status      string       `json:"status"`
	Summary     string       `json:"summary,omitempty"`
	StartedAt   time.Time    `json:"started_at"`
	CompletedAt time.Time    `json:"completed_at"`
}

// Router is the intelligent middleware that analyzes intent and composes tool chains.
type Router struct {
	mu       sync.RWMutex
	tools    map[string]ToolNode
	patterns []IntentPattern
	plans    map[string]*ExecutionPlan
	results  map[string]*ExecutionResult
	logger   *slog.Logger
}

// NewRouter creates a new smart tool router with default patterns.
func NewRouter(logger *slog.Logger) *Router {
	r := &Router{
		tools:   make(map[string]ToolNode),
		plans:   make(map[string]*ExecutionPlan),
		results: make(map[string]*ExecutionResult),
		logger:  logger,
	}
	r.patterns = defaultPatterns()
	r.tools = defaultToolGraph()
	return r
}

// ClassifyAndPlan analyzes a high-level request and builds an execution plan.
func (r *Router) ClassifyAndPlan(request string) (*ExecutionPlan, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	lower := strings.ToLower(request)
	var matched *IntentPattern

	// Score patterns by keyword match count
	bestScore := 0
	for i := range r.patterns {
		score := 0
		for _, kw := range r.patterns[i].Pattern {
			if strings.Contains(lower, kw) {
				score++
			}
		}
		if score > bestScore {
			bestScore = score
			matched = &r.patterns[i]
		}
	}

	if matched == nil || bestScore == 0 {
		return nil, fmt.Errorf("no matching intent pattern for request: %q", request)
	}

	plan := &ExecutionPlan{
		ID:          fmt.Sprintf("plan_%d", time.Now().UnixNano()),
		Intent:      matched.ID,
		Description: matched.Description,
		Steps:       matched.ToolChain,
		CreatedAt:   time.Now(),
		Status:      "pending",
	}

	r.plans[plan.ID] = plan
	return plan, nil
}

// RecordStepResult records the result of executing a step.
func (r *Router) RecordStepResult(planID string, result StepResult) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	plan, ok := r.plans[planID]
	if !ok {
		return fmt.Errorf("plan %q not found", planID)
	}

	if plan.Status == "pending" {
		plan.Status = "running"
	}

	exec, ok := r.results[planID]
	if !ok {
		exec = &ExecutionResult{
			PlanID:    planID,
			Steps:     make([]StepResult, 0),
			Status:    "running",
			StartedAt: time.Now(),
		}
		r.results[planID] = exec
	}

	exec.Steps = append(exec.Steps, result)

	// Check if all steps are complete
	if len(exec.Steps) >= len(plan.Steps) {
		exec.Status = "completed"
		exec.CompletedAt = time.Now()
		plan.Status = "completed"

		// Check for any failed steps
		for _, s := range exec.Steps {
			if s.Status == "failed" {
				exec.Status = "partial"
				plan.Status = "partial"
				break
			}
		}
	}

	return nil
}

// GetNextSteps returns the next executable steps (dependencies satisfied).
func (r *Router) GetNextSteps(planID string) ([]ToolStep, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	plan, ok := r.plans[planID]
	if !ok {
		return nil, fmt.Errorf("plan %q not found", planID)
	}

	exec := r.results[planID]
	completedSteps := make(map[string]bool)
	if exec != nil {
		for _, s := range exec.Steps {
			if s.Status == "completed" || s.Status == "skipped" {
				completedSteps[s.StepID] = true
			}
		}
	}

	var ready []ToolStep
	for _, step := range plan.Steps {
		if completedSteps[step.ID] {
			continue
		}
		depsOK := true
		for _, dep := range step.DependsOn {
			if !completedSteps[dep] {
				depsOK = false
				break
			}
		}
		if depsOK {
			ready = append(ready, step)
		}
	}

	return ready, nil
}

// GetPlan retrieves an execution plan.
func (r *Router) GetPlan(planID string) (*ExecutionPlan, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	plan, ok := r.plans[planID]
	if !ok {
		return nil, fmt.Errorf("plan %q not found", planID)
	}
	return plan, nil
}

// GetResult retrieves execution results.
func (r *Router) GetResult(planID string) (*ExecutionResult, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result, ok := r.results[planID]
	if !ok {
		return nil, fmt.Errorf("no results for plan %q", planID)
	}
	return result, nil
}

// ListPlans returns all execution plans.
func (r *Router) ListPlans() []*ExecutionPlan {
	r.mu.RLock()
	defer r.mu.RUnlock()

	plans := make([]*ExecutionPlan, 0, len(r.plans))
	for _, p := range r.plans {
		plans = append(plans, p)
	}
	return plans
}

// GetToolGraph returns the tool dependency graph.
func (r *Router) GetToolGraph() map[string]ToolNode {
	r.mu.RLock()
	defer r.mu.RUnlock()

	graph := make(map[string]ToolNode, len(r.tools))
	for k, v := range r.tools {
		graph[k] = v
	}
	return graph
}

func defaultToolGraph() map[string]ToolNode {
	return map[string]ToolNode{
		"kafka_list_topics":       {Name: "kafka_list_topics", Category: "discovery", Produces: []string{"topic_list"}},
		"kafka_topic_describe":    {Name: "kafka_topic_describe", Category: "discovery", DependsOn: []string{"topic_list"}, Produces: []string{"topic_detail"}},
		"kafka_consumer_groups":   {Name: "kafka_consumer_groups", Category: "monitoring", Produces: []string{"group_list"}},
		"kafka_lag_diagnosis":     {Name: "kafka_lag_diagnosis", Category: "diagnosis", DependsOn: []string{"group_list"}, Produces: []string{"lag_diagnosis"}},
		"kafka_anomaly_detect":    {Name: "kafka_anomaly_detect", Category: "analytics", Produces: []string{"anomaly_report"}},
		"kafka_lineage_trace":     {Name: "kafka_lineage_trace", Category: "governance", DependsOn: []string{"topic_detail"}, Produces: []string{"lineage_graph"}},
		"kafka_consume":           {Name: "kafka_consume", Category: "data", DependsOn: []string{"topic_list"}, Produces: []string{"messages"}},
		"kafka_forecast_topic":    {Name: "kafka_forecast_topic", Category: "analytics", DependsOn: []string{"topic_detail"}, Produces: []string{"forecast"}},
		"kafka_cluster_info":      {Name: "kafka_cluster_info", Category: "discovery", Produces: []string{"cluster_info"}},
		"kafka_schema_versions":   {Name: "kafka_schema_versions", Category: "schema", Produces: []string{"schema_info"}},
	}
}

func defaultPatterns() []IntentPattern {
	return []IntentPattern{
		{
			ID:          "investigate_delay",
			Pattern:     []string{"delay", "slow", "behind", "late", "backlog"},
			Description: "Investigate message processing delays",
			Priority:    10,
			ToolChain: []ToolStep{
				{ID: "s1", Tool: "kafka_consumer_groups", Args: map[string]any{}},
				{ID: "s2", Tool: "kafka_lag_diagnosis", DependsOn: []string{"s1"}, PassFrom: map[string]string{"group": "s1.group_id"}},
				{ID: "s3", Tool: "kafka_anomaly_detect", DependsOn: []string{"s1"}},
				{ID: "s4", Tool: "kafka_lineage_trace", DependsOn: []string{"s2"}, Optional: true},
			},
		},
		{
			ID:          "capacity_analysis",
			Pattern:     []string{"capacity", "growth", "scale", "forecast", "storage"},
			Description: "Analyze cluster capacity and growth trends",
			Priority:    8,
			ToolChain: []ToolStep{
				{ID: "s1", Tool: "kafka_cluster_info"},
				{ID: "s2", Tool: "kafka_list_topics", DependsOn: []string{"s1"}},
				{ID: "s3", Tool: "kafka_forecast_topic", DependsOn: []string{"s2"}},
			},
		},
		{
			ID:          "schema_investigation",
			Pattern:     []string{"schema", "compatibility", "breaking", "evolution", "avro", "protobuf"},
			Description: "Investigate schema compatibility and evolution",
			Priority:    7,
			ToolChain: []ToolStep{
				{ID: "s1", Tool: "kafka_list_schemas"},
				{ID: "s2", Tool: "kafka_schema_versions", DependsOn: []string{"s1"}},
				{ID: "s3", Tool: "kafka_schema_compatibility", DependsOn: []string{"s2"}},
			},
		},
		{
			ID:          "incident_investigation",
			Pattern:     []string{"incident", "outage", "error", "failure", "down", "broken"},
			Description: "Investigate a Kafka incident end-to-end",
			Priority:    10,
			ToolChain: []ToolStep{
				{ID: "s1", Tool: "kafka_cluster_info"},
				{ID: "s2", Tool: "kafka_consumer_groups", DependsOn: []string{"s1"}},
				{ID: "s3", Tool: "kafka_lag_diagnosis", DependsOn: []string{"s2"}},
				{ID: "s4", Tool: "kafka_anomaly_detect", DependsOn: []string{"s1"}},
				{ID: "s5", Tool: "kafka_lineage_trace", DependsOn: []string{"s3"}, Optional: true},
			},
		},
		{
			ID:          "topic_exploration",
			Pattern:     []string{"explore", "discover", "overview", "what topics", "show me"},
			Description: "Explore and discover cluster topics",
			Priority:    5,
			ToolChain: []ToolStep{
				{ID: "s1", Tool: "kafka_cluster_info"},
				{ID: "s2", Tool: "kafka_list_topics", DependsOn: []string{"s1"}},
				{ID: "s3", Tool: "kafka_topic_describe", DependsOn: []string{"s2"}},
			},
		},
		{
			ID:          "cost_investigation",
			Pattern:     []string{"cost", "expensive", "spending", "billing", "optimize"},
			Description: "Investigate Kafka cost and optimization opportunities",
			Priority:    6,
			ToolChain: []ToolStep{
				{ID: "s1", Tool: "kafka_cluster_info"},
				{ID: "s2", Tool: "kafka_list_topics", DependsOn: []string{"s1"}},
				{ID: "s3", Tool: "kafka_cost_analyze", DependsOn: []string{"s2"}},
			},
		},
		{
			ID:          "data_quality_check",
			Pattern:     []string{"quality", "validation", "corrupt", "malformed", "data issue"},
			Description: "Check data quality across topics",
			Priority:    6,
			ToolChain: []ToolStep{
				{ID: "s1", Tool: "kafka_list_topics"},
				{ID: "s2", Tool: "kafka_consume", DependsOn: []string{"s1"}, Args: map[string]any{"count": 10}},
				{ID: "s3", Tool: "kafka_data_quality_score", DependsOn: []string{"s2"}},
			},
		},
	}
}
