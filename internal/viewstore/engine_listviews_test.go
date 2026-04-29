package viewstore

import (
	"testing"
)

func TestListViews_EmptyEngine(t *testing.T) {
	e := NewEngine(testLogger())
	views := e.ListViews()
	if len(views) != 0 {
		t.Errorf("expected 0 views, got %d", len(views))
	}
}

func TestListViews_MultipleViews(t *testing.T) {
	e := NewEngine(testLogger())
	e.CreateView("view-a", "topic-a", "_key", "", "", 0, 0, nil, nil, "", "", "agent-1")
	e.CreateView("view-b", "topic-b", "_key", "count", "", 0, 0, nil, nil, "", "", "agent-2")
	e.CreateView("view-c", "topic-c", "_key", "sum", "", 0, 0, nil, nil, "", "", "agent-1")

	views := e.ListViews()
	if len(views) != 3 {
		t.Fatalf("expected 3 views, got %d", len(views))
	}

	names := make(map[string]bool)
	for _, v := range views {
		names[v.Name] = true
	}
	for _, expected := range []string{"view-a", "view-b", "view-c"} {
		if !names[expected] {
			t.Errorf("missing expected view: %s", expected)
		}
	}
}

func TestListViews_Immutability(t *testing.T) {
	e := NewEngine(testLogger())
	e.CreateView("immutable-test", "topic-a", "_key", "", "", 0, 0, nil, nil, "", "", "agent-1")

	views := e.ListViews()
	if len(views) != 1 {
		t.Fatalf("expected 1 view, got %d", len(views))
	}
	// Modifying returned slice should not affect engine
	views[0].Name = "MODIFIED"

	freshViews := e.ListViews()
	if freshViews[0].Name == "MODIFIED" {
		t.Error("ListViews should return copies, not references")
	}
}

func TestApplyAggregation_Count(t *testing.T) {
	e := NewEngine(testLogger())
	existing := map[string]any{"_count": float64(2)}
	incoming := map[string]any{"x": 1}

	result := e.applyAggregation("count", existing, incoming)
	if result["_count"] != float64(3) {
		t.Errorf("expected _count=3, got %v", result["_count"])
	}
}

func TestApplyAggregation_Sum(t *testing.T) {
	e := NewEngine(testLogger())
	existing := map[string]any{"amount": float64(10)}
	incoming := map[string]any{"amount": float64(5)}

	result := e.applyAggregation("sum", existing, incoming)
	if result["amount"] != float64(15) {
		t.Errorf("expected amount=15, got %v", result["amount"])
	}
}

func TestApplyAggregation_Avg(t *testing.T) {
	e := NewEngine(testLogger())
	existing := map[string]any{"_count": float64(1), "_sum_price": float64(10), "price": float64(10)}
	incoming := map[string]any{"price": float64(30)}

	result := e.applyAggregation("avg", existing, incoming)
	if result["_count"] != float64(2) {
		t.Errorf("expected _count=2, got %v", result["_count"])
	}
	if result["price"] != float64(20) {
		t.Errorf("expected avg price=20, got %v", result["price"])
	}
}

func TestApplyAggregation_Min(t *testing.T) {
	e := NewEngine(testLogger())
	existing := map[string]any{"latency": float64(5)}
	incoming := map[string]any{"latency": float64(3)}

	result := e.applyAggregation("min", existing, incoming)
	if result["latency"] != float64(3) {
		t.Errorf("expected min=3, got %v", result["latency"])
	}

	// Existing is already min
	result = e.applyAggregation("min", result, map[string]any{"latency": float64(10)})
	if result["latency"] != float64(3) {
		t.Errorf("expected min=3 (unchanged), got %v", result["latency"])
	}
}

func TestApplyAggregation_Max(t *testing.T) {
	e := NewEngine(testLogger())
	existing := map[string]any{"latency": float64(5)}
	incoming := map[string]any{"latency": float64(8)}

	result := e.applyAggregation("max", existing, incoming)
	if result["latency"] != float64(8) {
		t.Errorf("expected max=8, got %v", result["latency"])
	}

	// Existing is already max
	result = e.applyAggregation("max", result, map[string]any{"latency": float64(2)})
	if result["latency"] != float64(8) {
		t.Errorf("expected max=8 (unchanged), got %v", result["latency"])
	}
}

func TestApplyAggregation_Default(t *testing.T) {
	e := NewEngine(testLogger())
	existing := map[string]any{"name": "old"}
	incoming := map[string]any{"name": "new", "extra": "field"}

	result := e.applyAggregation("unknown_type", existing, incoming)
	if result["name"] != "new" {
		t.Errorf("default should overwrite, expected 'new', got %v", result["name"])
	}
	if result["extra"] != "field" {
		t.Errorf("default should copy new fields, got %v", result["extra"])
	}
}

func TestApplyAggregation_NonNumericValues(t *testing.T) {
	e := NewEngine(testLogger())
	existing := map[string]any{"name": "alice"}
	incoming := map[string]any{"name": "bob"}

	// Sum with non-numeric should not panic
	result := e.applyAggregation("sum", existing, incoming)
	if result["name"] != "alice" {
		t.Errorf("non-numeric fields should be unchanged in sum, got %v", result["name"])
	}
}
