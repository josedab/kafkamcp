package chat

import (
	"testing"
	"log/slog"
)

func newTestEngine() *Engine {
	return NewEngine(slog.Default())
}

func TestCreateSession(t *testing.T) {
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
	e := newTestEngine()
	s := e.CreateSession("agent-1")

	e.AddTurn(s.ID, "turn 1", Intent{Type: "topic_inspection"}, nil, "", map[string]any{"last_topic": "orders"})
	e.AddTurn(s.ID, "turn 2", Intent{Type: "group_inspection"}, nil, "", map[string]any{"last_group": "processor"})

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
