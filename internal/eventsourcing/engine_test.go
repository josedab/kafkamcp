package eventsourcing

import (
	"testing"
	"time"
)

func TestNewEngine(t *testing.T) {
	e := NewEngine(nil)
	if e == nil {
		t.Fatal("NewEngine returned nil")
	}
	stats := e.Stats()
	if stats["total_events"].(int) != 0 {
		t.Error("expected 0 events")
	}
}

func TestRecord(t *testing.T) {
	e := NewEngine(nil)
	err := e.Record(ActionEvent{
		AgentID:    "agent-1",
		SessionID:  "session-1",
		Tool:       "kafka_consume",
		DurationMs: 50,
		Result:     ActionResult{Status: "success", MessageCount: 10},
	})
	if err != nil {
		t.Fatalf("Record error: %v", err)
	}

	stats := e.Stats()
	if stats["total_events"].(int) != 1 {
		t.Errorf("expected 1 event, got %v", stats["total_events"])
	}
}

func TestRecordValidation(t *testing.T) {
	e := NewEngine(nil)
	if err := e.Record(ActionEvent{Tool: "test"}); err == nil {
		t.Error("expected error for missing agent_id")
	}
	if err := e.Record(ActionEvent{AgentID: "a"}); err == nil {
		t.Error("expected error for missing tool")
	}
}

func TestGetSession(t *testing.T) {
	e := NewEngine(nil)
	_ = e.Record(ActionEvent{AgentID: "a", SessionID: "s1", Tool: "kafka_consume", Result: ActionResult{Status: "success"}})
	_ = e.Record(ActionEvent{AgentID: "a", SessionID: "s1", Tool: "kafka_produce", Result: ActionResult{Status: "success"}})
	_ = e.Record(ActionEvent{AgentID: "a", SessionID: "s1", Tool: "kafka_consume", Result: ActionResult{Status: "error"}})

	sv, ok := e.GetSession("s1")
	if !ok {
		t.Fatal("session not found")
	}
	if sv.TotalActions != 3 {
		t.Errorf("TotalActions = %d, want 3", sv.TotalActions)
	}
	if sv.ErrorCount != 1 {
		t.Errorf("ErrorCount = %d, want 1", sv.ErrorCount)
	}
	if sv.ToolCounts["kafka_consume"] != 2 {
		t.Errorf("kafka_consume count = %d, want 2", sv.ToolCounts["kafka_consume"])
	}
}

func TestGetToolUsage(t *testing.T) {
	e := NewEngine(nil)
	_ = e.Record(ActionEvent{AgentID: "a", Tool: "kafka_consume", DurationMs: 100, Result: ActionResult{Status: "success"}})
	_ = e.Record(ActionEvent{AgentID: "b", Tool: "kafka_consume", DurationMs: 200, Result: ActionResult{Status: "success"}})
	_ = e.Record(ActionEvent{AgentID: "a", Tool: "kafka_produce", DurationMs: 50, Result: ActionResult{Status: "error"}})

	usage := e.GetToolUsage()
	if len(usage) != 2 {
		t.Fatalf("expected 2 tools, got %d", len(usage))
	}
}

func TestExportJSONL(t *testing.T) {
	e := NewEngine(nil)
	for i := 0; i < 5; i++ {
		_ = e.Record(ActionEvent{
			AgentID: "a", Tool: "kafka_consume",
			Result:    ActionResult{Status: "success"},
			Timestamp: time.Now(),
		})
	}

	lines, err := e.ExportJSONL(ExportFilter{AgentID: "a", Limit: 3})
	if err != nil {
		t.Fatalf("ExportJSONL error: %v", err)
	}
	if len(lines) != 3 {
		t.Errorf("expected 3 lines, got %d", len(lines))
	}
}

func TestExportJSONLFilter(t *testing.T) {
	e := NewEngine(nil)
	_ = e.Record(ActionEvent{AgentID: "a", Tool: "kafka_consume", Result: ActionResult{Status: "success"}})
	_ = e.Record(ActionEvent{AgentID: "b", Tool: "kafka_produce", Result: ActionResult{Status: "error"}})

	lines, _ := e.ExportJSONL(ExportFilter{AgentID: "b"})
	if len(lines) != 1 {
		t.Errorf("expected 1 line for agent b, got %d", len(lines))
	}
}
