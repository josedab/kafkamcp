package pipeline

import (
	"testing"
)

// --- executeFilter ---

func TestExecuteFilterEquals(t *testing.T) {
	step := Step{Type: "filter", Config: map[string]string{"field": "key", "operator": "equals", "value": "mykey"}}
	msg := PipelineMessage{Key: "mykey", Value: "val"}
	_, keep, _ := executeFilter(step, msg)
	if !keep {
		t.Error("expected match for equals")
	}
}

func TestExecuteFilterNotEquals(t *testing.T) {
	step := Step{Type: "filter", Config: map[string]string{"field": "key", "operator": "not_equals", "value": "other"}}
	msg := PipelineMessage{Key: "mykey"}
	_, keep, _ := executeFilter(step, msg)
	if !keep {
		t.Error("expected match for not_equals")
	}
}

func TestExecuteFilterContains(t *testing.T) {
	step := Step{Type: "filter", Config: map[string]string{"field": "value", "operator": "contains", "value": "err"}}
	msg := PipelineMessage{Value: "an error occurred"}
	_, keep, _ := executeFilter(step, msg)
	if !keep {
		t.Error("expected match for contains")
	}
}

func TestExecuteFilterStartsWith(t *testing.T) {
	step := Step{Type: "filter", Config: map[string]string{"field": "value", "operator": "starts_with", "value": "hello"}}
	msg := PipelineMessage{Value: "hello world"}
	_, keep, _ := executeFilter(step, msg)
	if !keep {
		t.Error("expected match for starts_with")
	}
}

func TestExecuteFilterEndsWith(t *testing.T) {
	step := Step{Type: "filter", Config: map[string]string{"field": "value", "operator": "ends_with", "value": "world"}}
	msg := PipelineMessage{Value: "hello world"}
	_, keep, _ := executeFilter(step, msg)
	if !keep {
		t.Error("expected match for ends_with")
	}
}

func TestExecuteFilterRegex(t *testing.T) {
	step := Step{Type: "filter", Config: map[string]string{"field": "value", "operator": "regex", "value": `^\d+$`}}
	msg := PipelineMessage{Value: "12345"}
	_, keep, _ := executeFilter(step, msg)
	if !keep {
		t.Error("expected match for regex")
	}
}

func TestExecuteFilterRegexNoMatch(t *testing.T) {
	step := Step{Type: "filter", Config: map[string]string{"field": "value", "operator": "regex", "value": `^\d+$`}}
	msg := PipelineMessage{Value: "not a number"}
	_, keep, _ := executeFilter(step, msg)
	if keep {
		t.Error("expected no match for regex")
	}
}

func TestExecuteFilterInvalidRegex(t *testing.T) {
	step := Step{Type: "filter", Config: map[string]string{"field": "value", "operator": "regex", "value": "[invalid"}}
	msg := PipelineMessage{Value: "test"}
	_, keep, _ := executeFilter(step, msg)
	if keep {
		t.Error("expected no match for invalid regex")
	}
}

func TestExecuteFilterExists(t *testing.T) {
	step := Step{Type: "filter", Config: map[string]string{"field": "value", "operator": "exists"}}
	msg := PipelineMessage{Value: "something"}
	_, keep, _ := executeFilter(step, msg)
	if !keep {
		t.Error("expected match for exists")
	}
}

func TestExecuteFilterNotExists(t *testing.T) {
	step := Step{Type: "filter", Config: map[string]string{"field": "value", "operator": "not_exists"}}
	msg := PipelineMessage{Value: ""}
	_, keep, _ := executeFilter(step, msg)
	if !keep {
		t.Error("expected match for not_exists on empty")
	}
}

func TestExecuteFilterNotContains(t *testing.T) {
	step := Step{Type: "filter", Config: map[string]string{"field": "value", "operator": "not_contains", "value": "error"}}
	msg := PipelineMessage{Value: "all good"}
	_, keep, _ := executeFilter(step, msg)
	if !keep {
		t.Error("expected match for not_contains")
	}
}

func TestExecuteFilterOnHeader(t *testing.T) {
	step := Step{Type: "filter", Config: map[string]string{"field": "env", "operator": "eq", "value": "prod"}}
	msg := PipelineMessage{Headers: map[string]string{"env": "prod"}}
	_, keep, _ := executeFilter(step, msg)
	if !keep {
		t.Error("expected match for header field")
	}
}

func TestExecuteFilterNilHeaders(t *testing.T) {
	step := Step{Type: "filter", Config: map[string]string{"field": "env", "operator": "eq", "value": "prod"}}
	msg := PipelineMessage{}
	_, keep, _ := executeFilter(step, msg)
	if keep {
		t.Error("expected no match with nil headers")
	}
}

func TestExecuteFilterDefaultOperator(t *testing.T) {
	step := Step{Type: "filter", Config: map[string]string{"field": "value", "operator": "unknown_op", "value": "test"}}
	msg := PipelineMessage{Value: "this is a test"}
	_, keep, _ := executeFilter(step, msg)
	if !keep {
		t.Error("expected default operator (contains) to match")
	}
}

// --- executeMap ---

func TestExecuteMapUppercaseKey(t *testing.T) {
	step := Step{Type: "map", Config: map[string]string{"expression": "uppercase", "target": "key"}}
	msg := PipelineMessage{Key: "hello"}
	result, _, _ := executeMap(step, msg)
	if result.Key != "HELLO" {
		t.Errorf("expected HELLO, got %s", result.Key)
	}
}

func TestExecuteMapLowercase(t *testing.T) {
	step := Step{Type: "map", Config: map[string]string{"expression": "lowercase", "target": "value"}}
	msg := PipelineMessage{Value: "HELLO"}
	result, _, _ := executeMap(step, msg)
	if result.Value != "hello" {
		t.Errorf("expected hello, got %s", result.Value)
	}
}

func TestExecuteMapLowercaseKey(t *testing.T) {
	step := Step{Type: "map", Config: map[string]string{"expression": "lowercase", "target": "key"}}
	msg := PipelineMessage{Key: "HELLO"}
	result, _, _ := executeMap(step, msg)
	if result.Key != "hello" {
		t.Errorf("expected hello, got %s", result.Key)
	}
}

func TestExecuteMapTrim(t *testing.T) {
	step := Step{Type: "map", Config: map[string]string{"expression": "trim", "target": "value"}}
	msg := PipelineMessage{Value: "  hello  "}
	result, _, _ := executeMap(step, msg)
	if result.Value != "hello" {
		t.Errorf("expected hello, got %s", result.Value)
	}
}

func TestExecuteMapTrimKey(t *testing.T) {
	step := Step{Type: "map", Config: map[string]string{"expression": "trim", "target": "key"}}
	msg := PipelineMessage{Key: "  key  "}
	result, _, _ := executeMap(step, msg)
	if result.Key != "key" {
		t.Errorf("expected key, got %s", result.Key)
	}
}

func TestExecuteMapPrefix(t *testing.T) {
	step := Step{Type: "map", Config: map[string]string{"expression": "prefix", "target": "value", "value": "pre_"}}
	msg := PipelineMessage{Value: "data"}
	result, _, _ := executeMap(step, msg)
	if result.Value != "pre_data" {
		t.Errorf("expected pre_data, got %s", result.Value)
	}
}

func TestExecuteMapPrefixKey(t *testing.T) {
	step := Step{Type: "map", Config: map[string]string{"expression": "prefix", "target": "key", "value": "k_"}}
	msg := PipelineMessage{Key: "1"}
	result, _, _ := executeMap(step, msg)
	if result.Key != "k_1" {
		t.Errorf("expected k_1, got %s", result.Key)
	}
}

func TestExecuteMapReplace(t *testing.T) {
	step := Step{Type: "map", Config: map[string]string{"expression": "replace", "target": "value", "old": "foo", "new": "bar"}}
	msg := PipelineMessage{Value: "foo and foo"}
	result, _, _ := executeMap(step, msg)
	if result.Value != "bar and bar" {
		t.Errorf("expected 'bar and bar', got %s", result.Value)
	}
}

func TestExecuteMapReplaceKey(t *testing.T) {
	step := Step{Type: "map", Config: map[string]string{"expression": "replace", "target": "key", "old": "a", "new": "b"}}
	msg := PipelineMessage{Key: "aaa"}
	result, _, _ := executeMap(step, msg)
	if result.Key != "bbb" {
		t.Errorf("expected bbb, got %s", result.Key)
	}
}

// --- executeEnrich ---

func TestExecuteEnrichAddsHeaders(t *testing.T) {
	step := Step{Type: "enrich", Name: "enricher", Config: map[string]string{"header_env": "production"}}
	msg := PipelineMessage{Value: "test"}
	result, _, _ := executeEnrich(step, msg)
	if result.Headers["env"] != "production" {
		t.Errorf("expected header env=production, got %v", result.Headers)
	}
	if result.Headers["x-pipeline-processed"] != "true" {
		t.Error("expected x-pipeline-processed header")
	}
}

func TestExecuteEnrichJSONFields(t *testing.T) {
	step := Step{Type: "enrich", Config: map[string]string{
		"add_fields": `{"source":"enriched"}`,
	}}
	msg := PipelineMessage{Value: `{"name":"test"}`}
	result, _, _ := executeEnrich(step, msg)
	if result.Value == `{"name":"test"}` {
		t.Error("expected value to be enriched with new fields")
	}
}

func TestExecuteEnrichInvalidJSON(t *testing.T) {
	step := Step{Type: "enrich", Config: map[string]string{
		"add_fields": `{"source":"enriched"}`,
	}}
	msg := PipelineMessage{Value: "not json"}
	result, _, _ := executeEnrich(step, msg)
	if result.Value != "not json" {
		t.Error("expected value unchanged for non-JSON")
	}
}

// --- executeTransform ---

func TestExecuteTransformJsonToCsv(t *testing.T) {
	step := Step{Type: "transform", Config: map[string]string{"format": "json_to_csv"}}
	msg := PipelineMessage{Value: `{"a":"1","b":"2"}`}
	result, keep, err := executeTransform(step, msg)
	if err != nil {
		t.Fatalf("executeTransform failed: %v", err)
	}
	if !keep {
		t.Error("expected keep=true")
	}
	if result.Value == `{"a":"1","b":"2"}` {
		t.Error("expected transformed CSV output")
	}
}

func TestExecuteTransformWrapJson(t *testing.T) {
	step := Step{Type: "transform", Config: map[string]string{"format": "wrap_json", "wrapper": "payload"}}
	msg := PipelineMessage{Value: `"hello"`}
	result, _, _ := executeTransform(step, msg)
	expected := `{"payload":"hello"}`
	if result.Value != expected {
		t.Errorf("expected %s, got %s", expected, result.Value)
	}
}

func TestExecuteTransformWrapJsonDefaultWrapper(t *testing.T) {
	step := Step{Type: "transform", Config: map[string]string{"format": "wrap_json"}}
	msg := PipelineMessage{Value: `42`}
	result, _, _ := executeTransform(step, msg)
	expected := `{"data":42}`
	if result.Value != expected {
		t.Errorf("expected %s, got %s", expected, result.Value)
	}
}

func TestExecuteTransformExtractField(t *testing.T) {
	step := Step{Type: "transform", Config: map[string]string{"format": "extract_field", "field": "name"}}
	msg := PipelineMessage{Value: `{"name":"test","age":25}`}
	result, _, _ := executeTransform(step, msg)
	if result.Value != `"test"` {
		t.Errorf("expected \"test\", got %s", result.Value)
	}
}

func TestExecuteTransformInvalidJSON(t *testing.T) {
	step := Step{Type: "transform", Config: map[string]string{"format": "json_to_csv"}}
	msg := PipelineMessage{Value: "not json"}
	result, _, _ := executeTransform(step, msg)
	if result.Value != "not json" {
		t.Error("expected value unchanged for invalid JSON")
	}
}

func TestExecuteTransformEmptyPayload(t *testing.T) {
	step := Step{Type: "transform", Config: map[string]string{"format": "extract_field", "field": "x"}}
	msg := PipelineMessage{Value: ""}
	result, _, _ := executeTransform(step, msg)
	if result.Value != "" {
		t.Error("expected empty value")
	}
}

// --- GetPipeline ---

func TestGetPipelineFound(t *testing.T) {
	e := newTestEngine()
	steps := []Step{{Type: "filter", Name: "f", Config: map[string]string{"field": "value", "operator": "exists"}}}
	pipe, _ := e.CreatePipeline("test", "src", "", "agent", steps)

	got, err := e.GetPipeline(pipe.ID)
	if err != nil {
		t.Fatalf("GetPipeline failed: %v", err)
	}
	if got.Name != "test" {
		t.Errorf("expected test, got %s", got.Name)
	}
}

func TestGetPipelineNotFound(t *testing.T) {
	e := newTestEngine()
	_, err := e.GetPipeline("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent pipeline")
	}
}

// --- GetRun ---

func TestGetRunFound(t *testing.T) {
	e := newTestEngine()
	steps := []Step{{Type: "filter", Name: "f", Config: map[string]string{"field": "value", "operator": "exists"}}}
	pipe, _ := e.CreatePipeline("test", "src", "", "agent", steps)
	run, _, _ := e.Execute(pipe.ID, []PipelineMessage{{Value: "hi"}})

	got, err := e.GetRun(run.ID)
	if err != nil {
		t.Fatalf("GetRun failed: %v", err)
	}
	if got.Status != "completed" {
		t.Errorf("expected completed, got %s", got.Status)
	}
}

func TestGetRunNotFound(t *testing.T) {
	e := newTestEngine()
	_, err := e.GetRun("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent run")
	}
}

// --- ListRuns ---

func TestListRunsAll(t *testing.T) {
	e := newTestEngine()
	steps := []Step{{Type: "filter", Name: "f", Config: map[string]string{"field": "value", "operator": "exists"}}}
	pipe, _ := e.CreatePipeline("test", "src", "", "agent", steps)
	e.Execute(pipe.ID, []PipelineMessage{{Value: "hi"}})
	e.Execute(pipe.ID, []PipelineMessage{{Value: "hello"}})

	runs := e.ListRuns("", 0)
	if len(runs) != 2 {
		t.Errorf("expected 2 runs, got %d", len(runs))
	}
}

func TestListRunsFilterByPipeline(t *testing.T) {
	e := newTestEngine()
	steps := []Step{{Type: "filter", Name: "f", Config: map[string]string{"field": "value", "operator": "exists"}}}
	pipe1, _ := e.CreatePipeline("test1", "src", "", "agent", steps)
	pipe2, _ := e.CreatePipeline("test2", "src", "", "agent", steps)
	e.Execute(pipe1.ID, []PipelineMessage{{Value: "hi"}})
	e.Execute(pipe2.ID, []PipelineMessage{{Value: "hello"}})

	runs := e.ListRuns(pipe1.ID, 0)
	if len(runs) != 1 {
		t.Errorf("expected 1 run for pipe1, got %d", len(runs))
	}
}

func TestListRunsEmpty(t *testing.T) {
	e := newTestEngine()
	runs := e.ListRuns("", 0)
	if len(runs) != 0 {
		t.Errorf("expected 0, got %d", len(runs))
	}
}

func TestListRunsWithLimit(t *testing.T) {
	e := newTestEngine()
	steps := []Step{{Type: "filter", Name: "f", Config: map[string]string{"field": "value", "operator": "exists"}}}
	pipe, _ := e.CreatePipeline("test", "src", "", "agent", steps)
	for i := 0; i < 5; i++ {
		e.Execute(pipe.ID, []PipelineMessage{{Value: "hi"}})
	}

	runs := e.ListRuns("", 2)
	if len(runs) != 2 {
		t.Errorf("expected 2, got %d", len(runs))
	}
}

// --- executeStep dispatch ---

func TestExecuteStepAggregate(t *testing.T) {
	step := Step{Type: "aggregate"}
	msg := PipelineMessage{Value: "test"}
	result, keep, err := executeStep(step, msg)
	if err != nil {
		t.Fatalf("executeStep failed: %v", err)
	}
	if !keep {
		t.Error("aggregate should pass through")
	}
	if result.Value != "test" {
		t.Error("aggregate should not modify message")
	}
}

func TestExecuteStepUnknownType(t *testing.T) {
	step := Step{Type: "unknown"}
	msg := PipelineMessage{Value: "test"}
	_, keep, _ := executeStep(step, msg)
	if !keep {
		t.Error("unknown type should pass through")
	}
}

// --- Edge cases ---

func TestCreatePipelineEmptySteps(t *testing.T) {
	e := newTestEngine()
	_, err := e.CreatePipeline("test", "src", "", "agent", []Step{})
	if err == nil {
		t.Error("expected error for empty steps")
	}
}

func TestCreatePipelineMissingName(t *testing.T) {
	e := newTestEngine()
	_, err := e.CreatePipeline("", "src", "", "agent", []Step{{Type: "filter"}})
	if err == nil {
		t.Error("expected error for missing name")
	}
}

func TestDeletePipelineNotFound(t *testing.T) {
	e := newTestEngine()
	err := e.DeletePipeline("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent pipeline")
	}
}

func TestExecutePausedPipeline(t *testing.T) {
	e := newTestEngine()
	steps := []Step{{Type: "filter", Name: "f", Config: map[string]string{"field": "value", "operator": "exists"}}}
	pipe, _ := e.CreatePipeline("test", "src", "", "agent", steps)
	pipe.Status = "paused"

	_, _, err := e.Execute(pipe.ID, []PipelineMessage{{Value: "hi"}})
	if err == nil {
		t.Error("expected error for paused pipeline")
	}
}
