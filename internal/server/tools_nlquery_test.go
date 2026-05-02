package server

import (
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/nlquery"
)

func TestApplyAggregation_Count(t *testing.T) {
	t.Parallel()
	data := []map[string]any{
		{"topic": "t1", "value": map[string]any{"status": "ok"}},
		{"topic": "t2", "value": map[string]any{"status": "ok"}},
		{"topic": "t3", "value": map[string]any{"status": "err"}},
	}

	result := applyAggregation(data, &nlquery.AggregationStep{Function: "count"})
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if result[0]["count"] != 3 {
		t.Errorf("expected count=3, got %v", result[0]["count"])
	}
}

func TestApplyAggregation_CountWithGroupBy(t *testing.T) {
	t.Parallel()
	data := []map[string]any{
		{"topic": "orders", "value": map[string]any{"status": "ok"}},
		{"topic": "orders", "value": map[string]any{"status": "ok"}},
		{"topic": "events", "value": map[string]any{"status": "err"}},
	}

	result := applyAggregation(data, &nlquery.AggregationStep{Function: "count", GroupBy: "topic"})
	if len(result) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(result))
	}

	groups := make(map[string]int)
	for _, r := range result {
		key := r["topic"].(string)
		groups[key] = r["count"].(int)
	}
	if groups["orders"] != 2 {
		t.Errorf("expected orders=2, got %d", groups["orders"])
	}
	if groups["events"] != 1 {
		t.Errorf("expected events=1, got %d", groups["events"])
	}
}

func TestApplyAggregation_Sum(t *testing.T) {
	t.Parallel()
	data := []map[string]any{
		{"value": map[string]any{"amount": 10.0}},
		{"value": map[string]any{"amount": 20.0}},
		{"value": map[string]any{"amount": 30.0}},
	}

	result := applyAggregation(data, &nlquery.AggregationStep{Function: "sum", Field: "amount"})
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if result[0]["sum"] != 60.0 {
		t.Errorf("expected sum=60, got %v", result[0]["sum"])
	}
	if result[0]["sample_count"] != 3 {
		t.Errorf("expected sample_count=3, got %v", result[0]["sample_count"])
	}
}

func TestApplyAggregation_Avg(t *testing.T) {
	t.Parallel()
	data := []map[string]any{
		{"value": map[string]any{"price": 10.0}},
		{"value": map[string]any{"price": 30.0}},
	}

	result := applyAggregation(data, &nlquery.AggregationStep{Function: "avg", Field: "price"})
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if result[0]["avg"] != 20.0 {
		t.Errorf("expected avg=20, got %v", result[0]["avg"])
	}
}

func TestApplyAggregation_Min(t *testing.T) {
	t.Parallel()
	data := []map[string]any{
		{"value": map[string]any{"latency": 5.0}},
		{"value": map[string]any{"latency": 2.0}},
		{"value": map[string]any{"latency": 8.0}},
	}

	result := applyAggregation(data, &nlquery.AggregationStep{Function: "min", Field: "latency"})
	if result[0]["min"] != 2.0 {
		t.Errorf("expected min=2, got %v", result[0]["min"])
	}
}

func TestApplyAggregation_Max(t *testing.T) {
	t.Parallel()
	data := []map[string]any{
		{"value": map[string]any{"latency": 5.0}},
		{"value": map[string]any{"latency": 2.0}},
		{"value": map[string]any{"latency": 8.0}},
	}

	result := applyAggregation(data, &nlquery.AggregationStep{Function: "max", Field: "latency"})
	if result[0]["max"] != 8.0 {
		t.Errorf("expected max=8, got %v", result[0]["max"])
	}
}

func TestApplyAggregation_GroupBy(t *testing.T) {
	t.Parallel()
	data := []map[string]any{
		{"value": map[string]any{"region": "us"}},
		{"value": map[string]any{"region": "eu"}},
		{"value": map[string]any{"region": "us"}},
	}

	result := applyAggregation(data, &nlquery.AggregationStep{Function: "group_by", GroupBy: "region"})
	groups := make(map[string]int)
	for _, r := range result {
		key := r["region"].(string)
		groups[key] = r["count"].(int)
	}
	if groups["us"] != 2 || groups["eu"] != 1 {
		t.Errorf("unexpected group_by result: %v", groups)
	}
}

func TestApplyAggregation_EmptyData(t *testing.T) {
	t.Parallel()
	result := applyAggregation(nil, &nlquery.AggregationStep{Function: "count"})
	if result != nil {
		t.Errorf("expected nil for empty data, got %v", result)
	}

	result = applyAggregation([]map[string]any{}, &nlquery.AggregationStep{Function: "count"})
	if len(result) != 0 {
		t.Errorf("expected empty for empty slice, got %v", result)
	}
}

func TestApplyAggregation_NilStep(t *testing.T) {
	t.Parallel()
	data := []map[string]any{{"key": "val"}}
	result := applyAggregation(data, nil)
	if len(result) != 1 {
		t.Errorf("expected original data returned, got %d items", len(result))
	}
}

func TestApplyAggregation_NonNumericFields(t *testing.T) {
	t.Parallel()
	data := []map[string]any{
		{"value": map[string]any{"name": "alice"}},
		{"value": map[string]any{"name": "bob"}},
	}

	result := applyAggregation(data, &nlquery.AggregationStep{Function: "sum", Field: "name"})
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if result[0]["sum"] != nil {
		t.Errorf("expected nil sum for non-numeric, got %v", result[0]["sum"])
	}
}

func TestApplyAggregation_MissingField(t *testing.T) {
	t.Parallel()
	data := []map[string]any{{"topic": "t1"}}
	result := applyAggregation(data, &nlquery.AggregationStep{Function: "avg", Field: ""})
	if len(result) != 1 {
		t.Fatalf("expected original data returned when field empty, got %d", len(result))
	}
}

func TestApplyAggregation_GroupByEmptyGroupByField(t *testing.T) {
	t.Parallel()
	data := []map[string]any{{"topic": "t1"}}
	result := applyAggregation(data, &nlquery.AggregationStep{Function: "group_by", GroupBy: ""})
	if len(result) != 1 {
		t.Errorf("expected original data returned, got %d", len(result))
	}
}

func TestExtractNestedField_TopLevel(t *testing.T) {
	t.Parallel()
	row := map[string]any{"topic": "orders", "partition": 0}
	if v := extractNestedField(row, "topic"); v != "orders" {
		t.Errorf("expected 'orders', got %v", v)
	}
}

func TestExtractNestedField_NestedValue(t *testing.T) {
	t.Parallel()
	row := map[string]any{
		"value": map[string]any{"status": "active", "count": 42},
	}
	if v := extractNestedField(row, "status"); v != "active" {
		t.Errorf("expected 'active', got %v", v)
	}
	if v := extractNestedField(row, "count"); v != 42 {
		t.Errorf("expected 42, got %v", v)
	}
}

func TestExtractNestedField_MissingField(t *testing.T) {
	t.Parallel()
	row := map[string]any{"topic": "orders"}
	if v := extractNestedField(row, "nonexistent"); v != nil {
		t.Errorf("expected nil for missing field, got %v", v)
	}
}

func TestExtractNestedField_ValueNotMap(t *testing.T) {
	t.Parallel()
	row := map[string]any{"value": "plain-string"}
	if v := extractNestedField(row, "nested_key"); v != nil {
		t.Errorf("expected nil when value is not a map, got %v", v)
	}
}

func TestToFloat64(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input any
		want  float64
		ok    bool
	}{
		{"float64", float64(3.14), 3.14, true},
		{"float32", float32(2.5), 2.5, true},
		{"int", int(42), 42.0, true},
		{"int64", int64(100), 100.0, true},
		{"int32", int32(7), 7.0, true},
		{"string", "not-a-number", 0, false},
		{"bool", true, 0, false},
		{"nil", nil, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := toFloat64(tt.input)
			if ok != tt.ok {
				t.Errorf("toFloat64(%v) ok = %v, want %v", tt.input, ok, tt.ok)
			}
			if ok && got != tt.want {
				t.Errorf("toFloat64(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
