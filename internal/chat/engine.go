package chat

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/randid"
)

const (
	maxSessionAge      = 24 * time.Hour
	maxTurnsPerSession = 200
	maxContextEntries  = 50
)

// Intent represents the classified intent from a user message.
type Intent struct {
	Type       string            `json:"type"`
	Entities   map[string]string `json:"entities,omitempty"`
	Confidence float64           `json:"confidence"`
	Reference  string            `json:"reference,omitempty"`
}

// Turn represents a single conversational exchange.
type Turn struct {
	ID        string         `json:"id"`
	Input     string         `json:"input"`
	Intent    Intent         `json:"intent"`
	ToolCalls []ToolCall     `json:"tool_calls,omitempty"`
	Response  string         `json:"response,omitempty"`
	Entities  map[string]any `json:"entities,omitempty"`
	Timestamp time.Time      `json:"timestamp"`
}

// ToolCall records a tool invocation within a turn.
type ToolCall struct {
	Tool     string         `json:"tool"`
	Args     map[string]any `json:"args,omitempty"`
	Duration time.Duration  `json:"duration"`
	Error    string         `json:"error,omitempty"`
}

// Session holds multi-turn conversation state.
type Session struct {
	ID        string         `json:"id"`
	AgentID   string         `json:"agent_id"`
	Turns     []Turn         `json:"turns"`
	Context   map[string]any `json:"context"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	Active    bool           `json:"active"`
}

// InvestigationTemplate defines a pre-built investigation workflow.
type InvestigationTemplate struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Category    string   `json:"category"`
	Steps       []string `json:"steps"`
	Tools       []string `json:"tools"`
}

// Engine manages chat sessions with context tracking and intent resolution.
type Engine struct {
	mu        sync.RWMutex
	sessions  map[string]*Session
	templates []InvestigationTemplate
	logger    *slog.Logger
}

// NewEngine creates a new chat engine with built-in investigation templates.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		sessions:  make(map[string]*Session),
		templates: defaultTemplates(),
		logger:    logger,
	}
}

// CreateSession starts a new chat session for an agent.
func (e *Engine) CreateSession(agentID string) *Session {
	e.mu.Lock()
	defer e.mu.Unlock()

	id := fmt.Sprintf("chat_%s_%s", agentID, randid.New(""))
	s := &Session{
		ID:        id,
		AgentID:   agentID,
		Turns:     make([]Turn, 0),
		Context:   make(map[string]any),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Active:    true,
	}
	e.sessions[id] = s
	return s
}

// GetSession retrieves an active session.
func (e *Engine) GetSession(sessionID string) (*Session, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	s, ok := e.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %q not found", sessionID)
	}
	if !s.Active {
		return nil, fmt.Errorf("session %q is closed", sessionID)
	}
	if time.Since(s.CreatedAt) > maxSessionAge {
		return nil, fmt.Errorf("session %q has expired", sessionID)
	}
	return s, nil
}

// AddTurn records a new conversational turn in the session.
func (e *Engine) AddTurn(sessionID, input string, intent Intent, toolCalls []ToolCall, response string, entities map[string]any) (*Turn, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	s, ok := e.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %q not found", sessionID)
	}
	if !s.Active {
		return nil, fmt.Errorf("session %q is closed", sessionID)
	}
	if len(s.Turns) >= maxTurnsPerSession {
		return nil, fmt.Errorf("session has reached maximum turns (%d)", maxTurnsPerSession)
	}

	turn := Turn{
		ID:        fmt.Sprintf("turn_%d", len(s.Turns)+1),
		Input:     input,
		Intent:    intent,
		ToolCalls: toolCalls,
		Response:  response,
		Entities:  entities,
		Timestamp: time.Now(),
	}

	s.Turns = append(s.Turns, turn)
	s.UpdatedAt = time.Now()

	// Update session context with entities from this turn
	for k, v := range entities {
		s.Context[k] = v
	}

	// Trim context to max size
	if len(s.Context) > maxContextEntries {
		oldest := ""
		for k := range s.Context {
			if oldest == "" || k < oldest {
				oldest = k
			}
		}
		delete(s.Context, oldest)
	}

	return &turn, nil
}

// ResolveReference resolves references like "that topic", "the same group" from session context.
func (e *Engine) ResolveReference(sessionID, input string) (map[string]string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	s, ok := e.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %q not found", sessionID)
	}

	resolved := make(map[string]string)
	lower := strings.ToLower(input)

	// Resolve topic references
	if containsAny(lower, "that topic", "the topic", "same topic", "this topic", "it") {
		if topic, ok := s.Context["last_topic"]; ok {
			resolved["topic"] = fmt.Sprint(topic)
		}
	}

	// Resolve consumer group references
	if containsAny(lower, "that group", "the group", "same group", "this group") {
		if group, ok := s.Context["last_group"]; ok {
			resolved["group"] = fmt.Sprint(group)
		}
	}

	// Resolve cluster references
	if containsAny(lower, "that cluster", "the cluster", "same cluster") {
		if cluster, ok := s.Context["last_cluster"]; ok {
			resolved["cluster"] = fmt.Sprint(cluster)
		}
	}

	// Resolve schema references
	if containsAny(lower, "that schema", "the schema", "its schema") {
		if schema, ok := s.Context["last_schema"]; ok {
			resolved["schema"] = fmt.Sprint(schema)
		}
	}

	return resolved, nil
}

// ClassifyIntent determines the intent from user input with context awareness.
func (e *Engine) ClassifyIntent(sessionID, input string) Intent {
	lower := strings.ToLower(input)
	intent := Intent{
		Entities:   make(map[string]string),
		Confidence: 0.8,
	}

	// Classify by keywords
	switch {
	case containsAny(lower, "lag", "behind", "slow consumer", "falling behind"):
		intent.Type = "lag_diagnosis"
	case containsAny(lower, "anomal", "unusual", "spike", "drop", "abnormal"):
		intent.Type = "anomaly_detection"
	case containsAny(lower, "schema", "avro", "protobuf", "field", "evolve"):
		intent.Type = "schema_inspection"
	case containsAny(lower, "consumer", "group", "member", "offset"):
		intent.Type = "group_inspection"
	case containsAny(lower, "topic", "describe", "partition"):
		intent.Type = "topic_inspection"
	case containsAny(lower, "produce", "send", "write", "publish"):
		intent.Type = "produce_message"
	case containsAny(lower, "consume", "read", "fetch", "poll"):
		intent.Type = "consume_message"
	case containsAny(lower, "lineage", "flow", "upstream", "downstream"):
		intent.Type = "lineage_tracking"
	case containsAny(lower, "capacity", "forecast", "predict", "growth"):
		intent.Type = "capacity_planning"
	case containsAny(lower, "incident", "outage", "triage", "investigate"):
		intent.Type = "incident_triage"
	case containsAny(lower, "cost", "expense", "spending", "bill"):
		intent.Type = "cost_analysis"
	default:
		intent.Type = "general_query"
		intent.Confidence = 0.5
	}

	// Check for references to previous context
	if sessionID != "" {
		refs, err := e.ResolveReference(sessionID, input)
		if err == nil && len(refs) > 0 {
			for k, v := range refs {
				intent.Entities[k] = v
				intent.Reference = k + "=" + v
			}
			intent.Confidence += 0.1
			if intent.Confidence > 1.0 {
				intent.Confidence = 1.0
			}
		}
	}

	return intent
}

// CloseSession ends a chat session.
func (e *Engine) CloseSession(sessionID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	s, ok := e.sessions[sessionID]
	if !ok {
		return fmt.Errorf("session %q not found", sessionID)
	}
	s.Active = false
	s.UpdatedAt = time.Now()
	return nil
}

// ListSessions returns all sessions for an agent.
func (e *Engine) ListSessions(agentID string) []*Session {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []*Session
	for _, s := range e.sessions {
		if agentID == "" || s.AgentID == agentID {
			result = append(result, s)
		}
	}
	return result
}

// GetSessionContext returns the accumulated context for a session.
func (e *Engine) GetSessionContext(sessionID string) (map[string]any, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	s, ok := e.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %q not found", sessionID)
	}
	ctx := make(map[string]any, len(s.Context))
	for k, v := range s.Context {
		ctx[k] = v
	}
	ctx["turn_count"] = len(s.Turns)
	ctx["session_age"] = time.Since(s.CreatedAt).String()
	return ctx, nil
}

// GetTemplate retrieves an investigation template by ID.
func (e *Engine) GetTemplate(id string) (*InvestigationTemplate, error) {
	for i := range e.templates {
		if e.templates[i].ID == id {
			return &e.templates[i], nil
		}
	}
	return nil, fmt.Errorf("template %q not found", id)
}

// ListTemplates returns available investigation templates.
func (e *Engine) ListTemplates(category string) []InvestigationTemplate {
	if category == "" {
		return e.templates
	}
	var result []InvestigationTemplate
	for _, t := range e.templates {
		if t.Category == category {
			result = append(result, t)
		}
	}
	return result
}

// CleanupExpired removes expired sessions.
func (e *Engine) CleanupExpired() int {
	e.mu.Lock()
	defer e.mu.Unlock()

	count := 0
	for id, s := range e.sessions {
		if time.Since(s.CreatedAt) > maxSessionAge {
			delete(e.sessions, id)
			count++
		}
	}
	return count
}

func containsAny(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func defaultTemplates() []InvestigationTemplate {
	return []InvestigationTemplate{
		{
			ID:          "incident_triage",
			Name:        "Incident Triage",
			Description: "Systematic investigation of a Kafka incident: identify affected topics, check consumer lag, detect anomalies, and correlate with recent changes.",
			Category:    "incident",
			Steps: []string{
				"1. List all topics and identify affected ones",
				"2. Check consumer group lag for affected topics",
				"3. Run anomaly detection on throughput metrics",
				"4. Check recent config changes via audit log",
				"5. Trace message flow for affected topics",
				"6. Generate incident summary with root cause",
			},
			Tools: []string{"kafka_list_topics", "kafka_consumer_groups", "kafka_anomaly_detect", "kafka_audit_log", "kafka_lineage_trace"},
		},
		{
			ID:          "capacity_review",
			Name:        "Capacity Review",
			Description: "Comprehensive capacity assessment: current utilization, growth trends, forecast projections, and scaling recommendations.",
			Category:    "capacity",
			Steps: []string{
				"1. Get cluster overview with partition counts",
				"2. Analyze topic throughput patterns",
				"3. Check consumer group velocities",
				"4. Run capacity forecast for next 30 days",
				"5. Identify partition hotspots and skew",
				"6. Generate capacity report with recommendations",
			},
			Tools: []string{"kafka_cluster_info", "kafka_topic_describe", "kafka_consumer_groups", "kafka_forecast_topic", "kafka_capacity_plan"},
		},
		{
			ID:          "schema_audit",
			Name:        "Schema Audit",
			Description: "Audit schema evolution: check compatibility, identify breaking changes, review field usage, and validate contracts.",
			Category:    "schema",
			Steps: []string{
				"1. List all registered schemas",
				"2. Check schema compatibility for each subject",
				"3. Identify schemas with breaking changes",
				"4. Review field-level data contract compliance",
				"5. Check for unused or deprecated fields",
				"6. Generate schema health report",
			},
			Tools: []string{"kafka_list_schemas", "kafka_schema_compatibility", "kafka_schema_versions", "kafka_data_contracts"},
		},
		{
			ID:          "consumer_health",
			Name:        "Consumer Health Check",
			Description: "Deep-dive into consumer group health: lag analysis, rebalance detection, partition assignment review.",
			Category:    "incident",
			Steps: []string{
				"1. List all consumer groups and their states",
				"2. Identify groups with high lag",
				"3. Diagnose root cause of lag",
				"4. Check for frequent rebalances",
				"5. Review partition assignment distribution",
				"6. Provide remediation recommendations",
			},
			Tools: []string{"kafka_consumer_groups", "kafka_lag_diagnosis", "kafka_selfheal_status"},
		},
		{
			ID:          "data_lineage",
			Name:        "Data Lineage Mapping",
			Description: "Map the complete data flow: trace topics, identify producers/consumers, build dependency graph.",
			Category:    "governance",
			Steps: []string{
				"1. Identify the target topic",
				"2. Trace upstream data sources",
				"3. Trace downstream consumers",
				"4. Build lineage graph",
				"5. Identify PII fields in the flow",
				"6. Document data flow for compliance",
			},
			Tools: []string{"kafka_lineage_trace", "kafka_lineage_graph", "kafka_topic_describe"},
		},
	}
}
