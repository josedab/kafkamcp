package workflow

import (
	"log/slog"
	"testing"
)

func TestNewEngineHasBuiltinTemplates(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	templates := e.ListTemplates("")
	if len(templates) < 5 {
		t.Errorf("expected at least 5 builtin templates, got %d", len(templates))
	}
}

func TestListTemplatesByCategory(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	ops := e.ListTemplates("operations")
	for _, tmpl := range ops {
		if tmpl.Category != "operations" {
			t.Errorf("expected category 'operations', got %q for template %q", tmpl.Category, tmpl.Name)
		}
	}
	if len(ops) == 0 {
		t.Error("expected at least 1 operations template")
	}
}

func TestListTemplatesEmptyCategory(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	all := e.ListTemplates("")
	ops := e.ListTemplates("operations")
	if len(all) <= len(ops) {
		t.Error("expected all templates to be more than operations-only")
	}
}

func TestListTemplatesNonexistentCategory(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	result := e.ListTemplates("nonexistent")
	if len(result) != 0 {
		t.Errorf("expected 0 templates for nonexistent category, got %d", len(result))
	}
}

func TestGetTemplateExists(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	tmpl, err := e.GetTemplate("dlq-investigation")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tmpl.Name != "dlq-investigation" {
		t.Errorf("expected template name 'dlq-investigation', got %q", tmpl.Name)
	}
	if len(tmpl.Steps) == 0 {
		t.Error("expected steps in template")
	}
	if len(tmpl.Parameters) == 0 {
		t.Error("expected parameters in template")
	}
}

func TestGetTemplateNotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, err := e.GetTemplate("nonexistent-template")
	if err == nil {
		t.Error("expected error for nonexistent template")
	}
}

func TestRegisterTemplate(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	err := e.RegisterTemplate(Template{
		Name:        "custom-workflow",
		Description: "A custom test workflow",
		Category:    "testing",
		Steps: []Step{
			{ID: "step1", Name: "Step 1", Tool: "some_tool"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tmpl, err := e.GetTemplate("custom-workflow")
	if err != nil {
		t.Fatalf("failed to get registered template: %v", err)
	}
	if tmpl.Category != "testing" {
		t.Errorf("expected category 'testing', got %q", tmpl.Category)
	}
}

func TestRegisterTemplateEmptyName(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	err := e.RegisterTemplate(Template{Steps: []Step{{ID: "s1", Tool: "t"}}})
	if err == nil {
		t.Error("expected error for empty template name")
	}
}

func TestRegisterTemplateNoSteps(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	err := e.RegisterTemplate(Template{Name: "empty-workflow"})
	if err == nil {
		t.Error("expected error for template with no steps")
	}
}

func TestStartExecutionExtended(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	exec, err := e.StartExecution("dlq-investigation", map[string]any{
		"topic": "orders-dlq",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exec.Status != "running" {
		t.Errorf("expected status 'running', got %q", exec.Status)
	}
	if exec.TotalSteps < 1 {
		t.Error("expected at least 1 step")
	}
	if exec.CurrentStep != 0 {
		t.Errorf("expected current step 0, got %d", exec.CurrentStep)
	}
}

func TestStartExecutionMissingRequired(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, err := e.StartExecution("dlq-investigation", map[string]any{})
	if err == nil {
		t.Error("expected error for missing required parameter")
	}
}

func TestStartExecutionDefaultParams(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	exec, err := e.StartExecution("dlq-investigation", map[string]any{
		"topic": "test-dlq",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// "limit" should have default value of 100
	if exec.Parameters["limit"] != 100 {
		t.Errorf("expected default limit=100, got %v", exec.Parameters["limit"])
	}
}

func TestStartExecutionNonexistentTemplate(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, err := e.StartExecution("nonexistent", map[string]any{})
	if err == nil {
		t.Error("expected error for nonexistent template")
	}
}

func TestCompleteStep(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	exec, _ := e.StartExecution("dlq-investigation", map[string]any{"topic": "t"})

	err := e.CompleteStep(exec.ID, StepResult{
		StepID:   "analyze",
		StepName: "Analyze DLQ messages",
		Tool:     "kafka_analyze_dlq",
		Status:   "success",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exec.CurrentStep != 1 {
		t.Errorf("expected current step 1, got %d", exec.CurrentStep)
	}
}

func TestCompleteStepNotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	err := e.CompleteStep("nonexistent-exec", StepResult{Status: "success"})
	if err == nil {
		t.Error("expected error for nonexistent execution")
	}
}

func TestCompleteAllSteps(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	exec, _ := e.StartExecution("dlq-investigation", map[string]any{"topic": "t"})

	for i := 0; i < exec.TotalSteps; i++ {
		e.CompleteStep(exec.ID, StepResult{Status: "success"})
	}

	result, _ := e.GetExecution(exec.ID)
	if result.Status != "completed" {
		t.Errorf("expected status 'completed', got %q", result.Status)
	}
	if result.CompletedAt == nil {
		t.Error("expected CompletedAt to be set")
	}
}

func TestCompleteStepFailedWithStopOnError(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterTemplate(Template{
		Name: "fail-test",
		Steps: []Step{
			{ID: "s1", Name: "Step 1", Tool: "tool1", OnError: "stop"},
		},
	})
	exec, _ := e.StartExecution("fail-test", map[string]any{})

	e.CompleteStep(exec.ID, StepResult{Status: "failed", Error: "boom"})

	result, _ := e.GetExecution(exec.ID)
	if result.Status != "failed" {
		t.Errorf("expected status 'failed', got %q", result.Status)
	}
	if result.Error != "boom" {
		t.Errorf("expected error 'boom', got %q", result.Error)
	}
}

func TestGetExecution(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	exec, _ := e.StartExecution("dlq-investigation", map[string]any{"topic": "t"})

	result, err := e.GetExecution(exec.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TemplateName != "dlq-investigation" {
		t.Errorf("expected template 'dlq-investigation', got %q", result.TemplateName)
	}
}

func TestGetExecutionNotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, err := e.GetExecution("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent execution")
	}
}

func TestGetNextStep(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	exec, _ := e.StartExecution("dlq-investigation", map[string]any{"topic": "t"})

	step, err := e.GetNextStep(exec.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if step == nil {
		t.Fatal("expected non-nil step")
	}
	if step.ID != "analyze" {
		t.Errorf("expected first step 'analyze', got %q", step.ID)
	}
}

func TestGetNextStepAfterCompletion(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	exec, _ := e.StartExecution("dlq-investigation", map[string]any{"topic": "t"})

	for i := 0; i < exec.TotalSteps; i++ {
		e.CompleteStep(exec.ID, StepResult{Status: "success"})
	}

	step, err := e.GetNextStep(exec.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if step != nil {
		t.Error("expected nil step after all steps completed")
	}
}

func TestGetNextStepNotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, err := e.GetNextStep("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent execution")
	}
}

func TestResolveStepArgsExtended(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	step := Step{
		ID:   "test",
		Args: map[string]string{"topic": "${topic}", "limit": "${limit}"},
	}
	params := map[string]any{"topic": "orders", "limit": 50}
	resolved := e.ResolveStepArgs(step, params)

	if resolved["topic"] != "orders" {
		t.Errorf("expected topic='orders', got %v", resolved["topic"])
	}
	if resolved["limit"] != "50" {
		t.Errorf("expected limit='50', got %v", resolved["limit"])
	}
}

func TestInterpolateExtended(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		template string
		params   map[string]any
		want     string
	}{
		{"simple", "${name}", map[string]any{"name": "test"}, "test"},
		{"no vars", "no vars", map[string]any{}, "no vars"},
		{"multiple", "${a} and ${b}", map[string]any{"a": "1", "b": "2"}, "1 and 2"},
		{"missing", "${missing}", map[string]any{}, "${missing}"},
		{"numeric", "${num}", map[string]any{"num": 42}, "42"},
		{"empty params", "hello", nil, "hello"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := interpolate(tt.template, tt.params)
			if got != tt.want {
				t.Errorf("interpolate(%q) = %q, want %q", tt.template, got, tt.want)
			}
		})
	}
}

func TestBuiltinTemplateCategories(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	categories := make(map[string]bool)
	for _, tmpl := range e.ListTemplates("") {
		categories[tmpl.Category] = true
	}

	expected := []string{"operations", "planning", "development", "monitoring", "discovery", "quality", "debugging"}
	for _, cat := range expected {
		if !categories[cat] {
			t.Errorf("expected category %q in builtin templates", cat)
		}
	}
}

func TestBuiltinTemplateTags(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	tmpl, _ := e.GetTemplate("dlq-investigation")
	if len(tmpl.Tags) == 0 {
		t.Error("expected tags on dlq-investigation template")
	}
}

func TestGetStepOnErrorDefault(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	exec, _ := e.StartExecution("dlq-investigation", map[string]any{"topic": "t"})

	// Complete first step as failed - default OnError is "continue"
	e.CompleteStep(exec.ID, StepResult{Status: "failed", Error: "test error"})

	result, _ := e.GetExecution(exec.ID)
	if result.Status == "failed" {
		t.Error("expected status not to be 'failed' when OnError defaults to 'continue'")
	}
}
