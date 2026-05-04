package toolcompose

import (
	"log/slog"
	"testing"
)

func TestParseCompositionYAML(t *testing.T) {
	yaml := `
name: "test-composition"
description: "Test composite tool"
category: "test"
parameters:
  - name: topic
    type: string
    required: true
steps:
  - id: step1
    name: "List Topics"
    tool: "kafka_list_topics"
  - id: step2
    name: "Describe Topic"
    tool: "kafka_describe_topic"
    args:
      topic: "${topic}"
    depends_on: [step1]
  - id: step3
    name: "Check Health"
    tool: "kafka_health_score"
    depends_on: [step2]
`
	comp, err := ParseCompositionYAML(yaml)
	if err != nil {
		t.Fatalf("ParseCompositionYAML: %v", err)
	}
	if comp.Name != "test-composition" {
		t.Errorf("Name = %q", comp.Name)
	}
	if len(comp.Steps) != 3 {
		t.Errorf("Steps = %d, want 3", len(comp.Steps))
	}
}

func TestDetectCycles(t *testing.T) {
	// Valid DAG
	steps := []CompositionStep{
		{ID: "a", Tool: "t1"},
		{ID: "b", Tool: "t2", DependsOn: []string{"a"}},
		{ID: "c", Tool: "t3", DependsOn: []string{"b"}},
	}
	if err := detectCycles(steps); err != nil {
		t.Errorf("expected no cycle: %v", err)
	}

	// Cycle: a -> b -> c -> a
	cycleSteps := []CompositionStep{
		{ID: "a", Tool: "t1", DependsOn: []string{"c"}},
		{ID: "b", Tool: "t2", DependsOn: []string{"a"}},
		{ID: "c", Tool: "t3", DependsOn: []string{"b"}},
	}
	if err := detectCycles(cycleSteps); err == nil {
		t.Error("expected cycle detection")
	}
}

func TestValidateComposition(t *testing.T) {
	tests := []struct {
		name    string
		comp    Composition
		wantErr bool
	}{
		{"valid", Composition{Name: "x", Steps: []CompositionStep{{ID: "a", Tool: "t"}}}, false},
		{"no name", Composition{Steps: []CompositionStep{{ID: "a", Tool: "t"}}}, true},
		{"no steps", Composition{Name: "x"}, true},
		{"missing dep", Composition{Name: "x", Steps: []CompositionStep{{ID: "a", Tool: "t", DependsOn: []string{"b"}}}}, true},
		{"self dep", Composition{Name: "x", Steps: []CompositionStep{{ID: "a", Tool: "t", DependsOn: []string{"a"}}}}, true},
		{"dup id", Composition{Name: "x", Steps: []CompositionStep{{ID: "a", Tool: "t"}, {ID: "a", Tool: "t2"}}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateComposition(&tt.comp)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateComposition err = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestEngineCreateExecute(t *testing.T) {
	e := NewEngine(slog.Default())

	yaml := `
name: "test-flow"
parameters:
  - name: topic
    required: true
steps:
  - id: s1
    name: "Step 1"
    tool: "kafka_list_topics"
  - id: s2
    name: "Step 2"
    tool: "kafka_describe_topic"
    args:
      topic: "${topic}"
    depends_on: [s1]
`
	comp, err := e.Create(yaml, "tester")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if comp.ID == "" {
		t.Error("expected non-empty ID")
	}

	exec, err := e.Execute(comp.ID, "tester", map[string]any{"topic": "orders"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if exec.Status != "running" {
		t.Errorf("Status = %q, want running", exec.Status)
	}
	if len(exec.Steps) != 2 {
		t.Errorf("Steps = %d, want 2", len(exec.Steps))
	}
}

func TestBuiltinCompositions(t *testing.T) {
	e := NewEngine(slog.Default())
	comps := e.List("")
	if len(comps) < 2 {
		t.Errorf("expected at least 2 builtin compositions, got %d", len(comps))
	}
}

func TestSearch(t *testing.T) {
	e := NewEngine(slog.Default())
	results := e.Search("schema")
	if len(results) == 0 {
		t.Error("expected search results for 'schema'")
	}
}
