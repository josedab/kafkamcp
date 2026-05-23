package chat

import (
	"log/slog"
	"testing"
	"time"
)

func newTestEngine() *Engine {
	return NewEngine(slog.Default())
}

func TestCreateSession(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	s := e.CreateSession("agent-1")

	if s.ID == "" {
		t.Fatal("session ID should not be empty")
	}
	if s.AgentID != "agent-1" {
		t.Fatalf("expected agent-1, got %s", s.AgentID)
	}
	if !s.Active {
		t.Fatal("session should be active")
	}
}

func TestAddTurnAndResolveReference(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	s := e.CreateSession("agent-1")

	entities := map[string]any{"last_topic": "orders", "last_group": "order-processor"}
	_, err := e.AddTurn(s.ID, "show me the orders topic", Intent{Type: "topic_inspection"}, nil, "Topic orders has 12 partitions", entities)
	if err != nil {
		t.Fatalf("AddTurn failed: %v", err)
	}

	// Resolve references
	refs, err := e.ResolveReference(s.ID, "what is the lag on that group?")
	if err != nil {
		t.Fatalf("ResolveReference failed: %v", err)
	}
	if refs["group"] != "order-processor" {
		t.Fatalf("expected group=order-processor, got %v", refs)
	}
}

func TestClassifyIntent(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	tests := []struct {
		input    string
		expected string
	}{
		{"why is my consumer lagging?", "lag_diagnosis"},
		{"show me any anomalies", "anomaly_detection"},
		{"describe the orders topic", "topic_inspection"},
		{"list all consumer groups", "group_inspection"},
		{"what is the schema for events?", "schema_inspection"},
		{"trace the data lineage", "lineage_tracking"},
		{"forecast capacity for next month", "capacity_planning"},
		{"hello", "general_query"},
	}

	for _, tt := range tests {
		intent := e.ClassifyIntent("", tt.input)
		if intent.Type != tt.expected {
			t.Errorf("input=%q: expected %s, got %s", tt.input, tt.expected, intent.Type)
		}
	}
}

func TestCloseSession(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	s := e.CreateSession("agent-1")

	if err := e.CloseSession(s.ID); err != nil {
		t.Fatalf("CloseSession failed: %v", err)
	}

	_, err := e.GetSession(s.ID)
	if err == nil {
		t.Fatal("expected error accessing closed session")
	}
}

func TestListTemplates(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	all := e.ListTemplates("")
	if len(all) != 5 {
		t.Fatalf("expected 5 templates, got %d", len(all))
	}

	incidents := e.ListTemplates("incident")
	if len(incidents) != 2 {
		t.Fatalf("expected 2 incident templates, got %d", len(incidents))
	}
}

func TestSessionContextAccumulates(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	s := e.CreateSession("agent-1")

	_, _ = e.AddTurn(s.ID, "turn 1", Intent{Type: "topic_inspection"}, nil, "", map[string]any{"last_topic": "orders"})
	_, _ = e.AddTurn(s.ID, "turn 2", Intent{Type: "group_inspection"}, nil, "", map[string]any{"last_group": "processor"})

	ctx, err := e.GetSessionContext(s.ID)
	if err != nil {
		t.Fatalf("GetSessionContext failed: %v", err)
	}
	if ctx["last_topic"] != "orders" {
		t.Errorf("expected last_topic=orders, got %v", ctx["last_topic"])
	}
	if ctx["last_group"] != "processor" {
		t.Errorf("expected last_group=processor, got %v", ctx["last_group"])
	}
}

func TestListSessions_AllWhenEmpty(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	e.CreateSession("agent-1")
	e.CreateSession("agent-2")
	e.CreateSession("agent-1")

	all := e.ListSessions("")
	if len(all) != 3 {
		t.Errorf("expected 3 sessions, got %d", len(all))
	}
}

func TestListSessions_FilterByAgentID(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	e.CreateSession("agent-1")
	e.CreateSession("agent-2")
	e.CreateSession("agent-1")

	filtered := e.ListSessions("agent-1")
	if len(filtered) != 2 {
		t.Errorf("expected 2 sessions for agent-1, got %d", len(filtered))
	}
	for _, s := range filtered {
		if s.AgentID != "agent-1" {
			t.Errorf("expected agent-1, got %s", s.AgentID)
		}
	}
}

func TestListSessions_UnknownAgent(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	e.CreateSession("agent-1")

	result := e.ListSessions("unknown-agent")
	if len(result) != 0 {
		t.Errorf("expected 0 sessions for unknown agent, got %d", len(result))
	}
}

func TestGetTemplate_Exists(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	tmpl, err := e.GetTemplate("incident_triage")
	if err != nil {
		t.Fatalf("GetTemplate: %v", err)
	}
	if tmpl.ID != "incident_triage" {
		t.Errorf("expected ID incident_triage, got %s", tmpl.ID)
	}
}

func TestGetTemplate_NotFound(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	_, err := e.GetTemplate("nonexistent")
	if err == nil {
		t.Error("expected error for unknown template")
	}
}

func TestCleanupExpired_RemovesOldSessions(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	// Create a session and manually backdate it
	s := e.CreateSession("agent-1")
	e.mu.Lock()
	e.sessions[s.ID].CreatedAt = time.Now().Add(-25 * time.Hour)
	e.mu.Unlock()

	// Create a fresh session
	e.CreateSession("agent-2")

	count := e.CleanupExpired()
	if count != 1 {
		t.Errorf("expected 1 expired session cleaned, got %d", count)
	}

	all := e.ListSessions("")
	if len(all) != 1 {
		t.Errorf("expected 1 remaining session, got %d", len(all))
	}
}

func TestCleanupExpired_KeepsActiveSessions(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	e.CreateSession("agent-1")
	e.CreateSession("agent-2")

	count := e.CleanupExpired()
	if count != 0 {
		t.Errorf("expected 0 expired sessions, got %d", count)
	}
}

func TestCleanupExpired_EmptyEngine(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	count := e.CleanupExpired()
	if count != 0 {
		t.Errorf("expected 0, got %d", count)
	}
}

func TestClassifyIntent_QueryIntents(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	tests := []struct {
		input    string
		expected string
	}{
		{"list all topics", "topic_inspection"},
		{"show me consumer groups", "group_inspection"},
		{"describe the partitions", "topic_inspection"},
	}
	for _, tt := range tests {
		intent := e.ClassifyIntent("", tt.input)
		if intent.Type != tt.expected {
			t.Errorf("input=%q: expected %s, got %s", tt.input, tt.expected, intent.Type)
		}
	}
}

func TestClassifyIntent_ActionIntents(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	tests := []struct {
		input    string
		expected string
	}{
		{"produce a message to the queue", "produce_message"},
		{"publish data now", "produce_message"},
		{"write a message", "produce_message"},
	}
	for _, tt := range tests {
		intent := e.ClassifyIntent("", tt.input)
		if intent.Type != tt.expected {
			t.Errorf("input=%q: expected %s, got %s", tt.input, tt.expected, intent.Type)
		}
	}
}

func TestClassifyIntent_AmbiguousReturnsGeneral(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	intent := e.ClassifyIntent("", "hello there")
	if intent.Type != "general_query" {
		t.Errorf("expected general_query, got %s", intent.Type)
	}
	if intent.Confidence >= 0.8 {
		t.Errorf("expected lower confidence for ambiguous input, got %f", intent.Confidence)
	}
}

func TestResolveReference_LastTopic(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	s := e.CreateSession("agent-1")
	_, _ = e.AddTurn(s.ID, "describe orders", Intent{}, nil, "", map[string]any{"last_topic": "orders"})

	refs, err := e.ResolveReference(s.ID, "what is the lag on that topic?")
	if err != nil {
		t.Fatalf("ResolveReference: %v", err)
	}
	if refs["topic"] != "orders" {
		t.Errorf("expected topic=orders, got %v", refs["topic"])
	}
}

func TestResolveReference_PreviousCluster(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	s := e.CreateSession("agent-1")
	_, _ = e.AddTurn(s.ID, "check cluster", Intent{}, nil, "", map[string]any{"last_cluster": "prod-1"})

	refs, err := e.ResolveReference(s.ID, "what about that cluster?")
	if err != nil {
		t.Fatalf("ResolveReference: %v", err)
	}
	if refs["cluster"] != "prod-1" {
		t.Errorf("expected cluster=prod-1, got %v", refs["cluster"])
	}
}

func TestResolveReference_UnknownSession(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	_, err := e.ResolveReference("nonexistent", "that topic")
	if err == nil {
		t.Error("expected error for unknown session")
	}
}

func TestResolveReference_NoContext(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	s := e.CreateSession("agent-1")

	refs, err := e.ResolveReference(s.ID, "what about that topic?")
	if err != nil {
		t.Fatalf("ResolveReference: %v", err)
	}
	if _, ok := refs["topic"]; ok {
		t.Error("expected no topic resolution without context")
	}
}

func TestClassifyIntent_WithSessionContext(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	s := e.CreateSession("agent-1")
	_, _ = e.AddTurn(s.ID, "check orders", Intent{}, nil, "", map[string]any{"last_topic": "orders"})

	intent := e.ClassifyIntent(s.ID, "what about that topic's lag?")
	if intent.Type != "lag_diagnosis" {
		t.Errorf("expected lag_diagnosis, got %s", intent.Type)
	}
	if intent.Entities["topic"] != "orders" {
		t.Errorf("expected entity topic=orders, got %v", intent.Entities)
	}
}
