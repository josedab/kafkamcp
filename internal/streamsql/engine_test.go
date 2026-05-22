package streamsql

import (
	"log/slog"
	"os"
	"sync"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestParseSimpleSelect(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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

// --- GetQuery tests ---

func TestGetQueryFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	q, _ := e.Parse("SELECT * FROM test_topic", "agent-1")

	got, err := e.GetQuery(q.ID)
	if err != nil {
		t.Fatalf("GetQuery failed: %v", err)
	}
	if got.SQL != q.SQL {
		t.Errorf("expected SQL %q, got %q", q.SQL, got.SQL)
	}
}

func TestGetQueryNotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, err := e.GetQuery("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent query")
	}
}

// --- ListQueries tests ---

func TestListQueriesEmpty(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	result := e.ListQueries(10)
	if len(result) != 0 {
		t.Errorf("expected 0 queries, got %d", len(result))
	}
}

func TestListQueries(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, _ = e.Parse("SELECT * FROM t1", "a1")
	_, _ = e.Parse("SELECT * FROM t2", "a2")
	_, _ = e.Parse("SELECT * FROM t3", "a3")

	result := e.ListQueries(10)
	if len(result) != 3 {
		t.Errorf("expected 3 queries, got %d", len(result))
	}
}

func TestListQueriesWithLimit(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, _ = e.Parse("SELECT * FROM t1", "a1")
	_, _ = e.Parse("SELECT * FROM t2", "a2")
	_, _ = e.Parse("SELECT * FROM t3", "a3")

	result := e.ListQueries(2)
	if len(result) != 2 {
		t.Errorf("expected 2 queries with limit, got %d", len(result))
	}
}

func TestListQueriesDefaultLimit(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, _ = e.Parse("SELECT * FROM t1", "a1")

	result := e.ListQueries(0)
	if len(result) != 1 {
		t.Errorf("expected 1 query with default limit, got %d", len(result))
	}
}

// --- DeleteQuery tests ---

func TestDeleteQuery(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	q, _ := e.Parse("SELECT * FROM test_topic", "agent-1")

	err := e.DeleteQuery(q.ID)
	if err != nil {
		t.Fatalf("DeleteQuery failed: %v", err)
	}

	_, err = e.GetQuery(q.ID)
	if err == nil {
		t.Error("expected error after delete")
	}
}

func TestDeleteQueryNotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	err := e.DeleteQuery("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent query")
	}
}

func TestDeleteThenList(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	q1, _ := e.Parse("SELECT * FROM t1", "a1")
	_, _ = e.Parse("SELECT * FROM t2", "a2")

	_ = e.DeleteQuery(q1.ID)
	result := e.ListQueries(10)
	if len(result) != 1 {
		t.Errorf("expected 1 query after delete, got %d", len(result))
	}
}

// --- matchWhere tests for all operators ---

func TestMatchWhereEquals(t *testing.T) {
	t.Parallel()
	msg := map[string]any{"status": "active"}
	w := &WhereClause{Field: "status", Operator: "=", Value: "active"}
	if !matchWhere(msg, w) {
		t.Error("expected match for =")
	}
	w.Value = "inactive"
	if matchWhere(msg, w) {
		t.Error("expected no match for = with different value")
	}
}

func TestMatchWhereNotEquals(t *testing.T) {
	t.Parallel()
	msg := map[string]any{"status": "active"}
	w := &WhereClause{Field: "status", Operator: "!=", Value: "inactive"}
	if !matchWhere(msg, w) {
		t.Error("expected match for !=")
	}
	w.Value = "active"
	if matchWhere(msg, w) {
		t.Error("expected no match for != with same value")
	}
}

func TestMatchWhereGreaterThan(t *testing.T) {
	t.Parallel()
	msg := map[string]any{"age": 30.0}
	w := &WhereClause{Field: "age", Operator: ">", Value: "25"}
	if !matchWhere(msg, w) {
		t.Error("expected match for > 25")
	}
	w.Value = "30"
	if matchWhere(msg, w) {
		t.Error("expected no match for > 30 when value is 30")
	}
}

func TestMatchWhereLessThan(t *testing.T) {
	t.Parallel()
	msg := map[string]any{"age": 20.0}
	w := &WhereClause{Field: "age", Operator: "<", Value: "25"}
	if !matchWhere(msg, w) {
		t.Error("expected match for < 25")
	}
}

func TestMatchWhereGreaterOrEqual(t *testing.T) {
	t.Parallel()
	msg := map[string]any{"age": 30.0}
	w := &WhereClause{Field: "age", Operator: ">=", Value: "30"}
	if !matchWhere(msg, w) {
		t.Error("expected match for >= 30")
	}
}

func TestMatchWhereLessOrEqual(t *testing.T) {
	t.Parallel()
	msg := map[string]any{"age": 25.0}
	w := &WhereClause{Field: "age", Operator: "<=", Value: "25"}
	if !matchWhere(msg, w) {
		t.Error("expected match for <= 25")
	}
}

func TestMatchWhereLike(t *testing.T) {
	t.Parallel()
	msg := map[string]any{"name": "hello-world"}
	w := &WhereClause{Field: "name", Operator: "LIKE", Value: "hello%"}
	if !matchWhere(msg, w) {
		t.Error("expected match for LIKE hello%")
	}
	w.Value = "%world"
	if !matchWhere(msg, w) {
		t.Error("expected match for LIKE %world")
	}
	w.Value = "nomatch%"
	if matchWhere(msg, w) {
		t.Error("expected no match for LIKE nomatch%")
	}
}

func TestMatchWhereLikeRegexSpecialChars(t *testing.T) {
	t.Parallel()
	msg := map[string]any{"name": "test.value"}
	w := &WhereClause{Field: "name", Operator: "LIKE", Value: "test%"}
	if !matchWhere(msg, w) {
		t.Error("expected match for LIKE with dots in value")
	}
}

func TestMatchWhereFieldMissing(t *testing.T) {
	t.Parallel()
	msg := map[string]any{"name": "test"}
	w := &WhereClause{Field: "missing", Operator: "=", Value: "test"}
	if matchWhere(msg, w) {
		t.Error("expected no match for missing field")
	}
}

func TestMatchWhereDefaultOperator(t *testing.T) {
	t.Parallel()
	msg := map[string]any{"status": "active"}
	w := &WhereClause{Field: "status", Operator: "UNKNOWN", Value: "active"}
	if !matchWhere(msg, w) {
		t.Error("expected default operator to behave like =")
	}
}

func TestMatchWhereWithBoolValue(t *testing.T) {
	t.Parallel()
	msg := map[string]any{"active": true}
	w := &WhereClause{Field: "active", Operator: "=", Value: "true"}
	if !matchWhere(msg, w) {
		t.Error("expected match for bool true")
	}
}

func TestMatchWhereWithStringNumeric(t *testing.T) {
	t.Parallel()
	msg := map[string]any{"count": "42"}
	w := &WhereClause{Field: "count", Operator: ">", Value: "10"}
	if !matchWhere(msg, w) {
		t.Error("expected match for string numeric comparison")
	}
}

// --- toFloat64 tests ---

func TestToFloat64(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    any
		expected float64
		wantErr  bool
	}{
		{float64(3.14), 3.14, false},
		{float32(2.5), 2.5, false},
		{int(42), 42, false},
		{int64(100), 100, false},
		{"3.14", 3.14, false},
		{"-5.5", -5.5, false},
		{"1e3", 1000, false},
		{"notanumber", 0, true},
		{nil, 0, true},
		{true, 0, true},
		{[]int{1, 2}, 0, true},
	}

	for _, tt := range tests {
		result, err := toFloat64(tt.input)
		if tt.wantErr {
			if err == nil {
				t.Errorf("toFloat64(%v) expected error, got nil", tt.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("toFloat64(%v) unexpected error: %v", tt.input, err)
			continue
		}
		diff := result - tt.expected
		if diff > 0.01 || diff < -0.01 {
			t.Errorf("toFloat64(%v) = %f, expected %f", tt.input, result, tt.expected)
		}
	}
}

// --- Concurrent GetQuery ---

func TestConcurrentGetQuery(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	q, _ := e.Parse("SELECT * FROM orders", "agent-1")

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := e.GetQuery(q.ID)
			if err != nil {
				t.Errorf("concurrent GetQuery failed: %v", err)
			}
			if got.SQL != q.SQL {
				t.Errorf("unexpected SQL in concurrent read")
			}
		}()
	}
	wg.Wait()
}
