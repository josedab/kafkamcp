package workflow

import (
	"testing"
)

func TestListTemplates(t *testing.T) {
	e := NewEngine(nil)
	all := e.ListTemplates("")
	if len(all) < 10 {
		t.Errorf("expected at least 10 built-in templates, got %d", len(all))
	}

	ops := e.ListTemplates("operations")
	if len(ops) == 0 {
		t.Error("expected operations templates")
	}
	for _, tmpl := range ops {
		if tmpl.Category != "operations" {
			t.Errorf("expected operations category, got %s", tmpl.Category)
		}
	}
}

func TestGetTemplate(t *testing.T) {
	e := NewEngine(nil)
	tmpl, err := e.GetTemplate("dlq-investigation")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tmpl.Name != "dlq-investigation" {
		t.Errorf("expected dlq-investigation, got %s", tmpl.Name)
	}

	_, err = e.GetTemplate("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent template")
	}
}

func TestStartExecution(t *testing.T) {
	e := NewEngine(nil)

	// Missing required parameter
	_, err := e.StartExecution("dlq-investigation", map[string]any{})
	if err == nil {
		t.Error("expected error for missing required parameter")
	}

	// Valid execution
	exec, err := e.StartExecution("dlq-investigation", map[string]any{"topic": "my-dlq"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exec.Status != "running" {
		t.Errorf("expected running status, got %s", exec.Status)
	}
	if exec.TotalSteps != 3 {
		t.Errorf("expected 3 steps, got %d", exec.TotalSteps)
	}
}

func TestResolveStepArgs(t *testing.T) {
	e := NewEngine(nil)
	step := Step{Args: map[string]string{"topic": "${topic}", "limit": "${limit}"}}
	params := map[string]any{"topic": "my-topic", "limit": 50}

	resolved := e.ResolveStepArgs(step, params)
	if resolved["topic"] != "my-topic" {
		t.Errorf("expected my-topic, got %v", resolved["topic"])
	}
}

func TestInterpolate(t *testing.T) {
	result := interpolate("${name} has ${count} items", map[string]any{"name": "test", "count": 42})
	if result != "test has 42 items" {
		t.Errorf("unexpected: %s", result)
	}
}
