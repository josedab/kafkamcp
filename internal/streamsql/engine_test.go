package streamsql

import (
	"log/slog"
	"os"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestParseSimpleSelect(t *testing.T) {
	e := NewEngine(testLogger())

	query, err := e.Parse("SELECT * FROM orders WHERE status = active LIMIT 10", "agent-1")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if query.SourceTopic != "orders" {
		t.Errorf("expected source topic orders, got %s", query.SourceTopic)
	}
	if query.WhereClause == nil {
		t.Error("expected WHERE clause")
	}
	if query.Limit != 10 {
		t.Errorf("expected limit 10, got %d", query.Limit)
	}
}

func TestParseAggregation(t *testing.T) {
	e := NewEngine(testLogger())

	query, err := e.Parse("SELECT region, COUNT(*) count, SUM(amount) total FROM orders GROUP BY region", "agent-1")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(query.AggFunctions) != 2 {
		t.Errorf("expected 2 aggregate functions, got %d", len(query.AggFunctions))
	}
	if len(query.GroupBy) != 1 || query.GroupBy[0] != "region" {
		t.Errorf("expected GROUP BY region, got %v", query.GroupBy)
	}
}

func TestExecuteSimple(t *testing.T) {
	e := NewEngine(testLogger())

	query, _ := e.Parse("SELECT * FROM test_topic", "agent-1")
	messages := []map[string]any{
		{"name": "alice", "age": 30.0},
		{"name": "bob", "age": 25.0},
	}

	result, err := e.Execute(query.ID, messages)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if result.RowCount != 2 {
		t.Errorf("expected 2 rows, got %d", result.RowCount)
	}
}

func TestExecuteWithWhere(t *testing.T) {
	e := NewEngine(testLogger())

	query, _ := e.Parse("SELECT * FROM test_topic WHERE age > 28", "agent-1")
	messages := []map[string]any{
		{"name": "alice", "age": 30.0},
		{"name": "bob", "age": 25.0},
		{"name": "carol", "age": 35.0},
	}

	result, _ := e.Execute(query.ID, messages)
	if result.RowCount != 2 {
		t.Errorf("expected 2 rows after filter, got %d", result.RowCount)
	}
}

func TestExecuteAggregation(t *testing.T) {
	e := NewEngine(testLogger())

	query, _ := e.Parse("SELECT region, COUNT(*) count FROM test_topic GROUP BY region", "agent-1")
	messages := []map[string]any{
		{"region": "us", "amount": 100.0},
		{"region": "eu", "amount": 200.0},
		{"region": "us", "amount": 150.0},
	}

	result, _ := e.Execute(query.ID, messages)
	if result.RowCount != 2 {
		t.Errorf("expected 2 groups, got %d", result.RowCount)
	}
}

func TestExplain(t *testing.T) {
	e := NewEngine(testLogger())

	query, _ := e.Parse("SELECT COUNT(*) FROM orders WHERE status = active WINDOW TUMBLING 5m", "agent-1")
	plan, err := e.Explain(query.ID)
	if err != nil {
		t.Fatalf("Explain failed: %v", err)
	}
	if len(plan.Steps) == 0 {
		t.Error("expected non-empty execution plan")
	}
}
