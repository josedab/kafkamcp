package dagworkflow

import (
	"testing"
)

// --- GetWorkflow ---

func TestGetWorkflowFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	steps := []DAGStep{{ID: "s1", Name: "S1", Tool: "t1"}}
	wf, _ := e.CreateWorkflow("test", "", "agent", nil, steps, nil)

	got, err := e.GetWorkflow(wf.ID)
	if err != nil {
		t.Fatalf("GetWorkflow failed: %v", err)
	}
	if got.Name != "test" {
		t.Errorf("expected test, got %s", got.Name)
	}
}

func TestGetWorkflowNotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, err := e.GetWorkflow("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent workflow")
	}
}

// --- ListWorkflows ---

func TestListWorkflowsLatestVersion(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	steps := []DAGStep{{ID: "s1", Name: "S1", Tool: "t1"}}
	_, _ = e.CreateWorkflow("wf-a", "", "agent", nil, steps, nil)
	_, _ = e.CreateWorkflow("wf-a", "", "agent", nil, steps, nil)
	_, _ = e.CreateWorkflow("wf-b", "", "agent", nil, steps, nil)

	list := e.ListWorkflows()
	if len(list) != 2 {
		t.Errorf("expected 2 unique workflows, got %d", len(list))
	}
	for _, wf := range list {
		if wf.Name == "wf-a" && wf.Version != 2 {
			t.Errorf("expected latest version 2 for wf-a, got %d", wf.Version)
		}
	}
}

func TestListWorkflowsEmpty(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	list := e.ListWorkflows()
	if len(list) != 0 {
		t.Errorf("expected 0, got %d", len(list))
	}
}

// --- GetVersions ---

func TestGetVersions(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	steps := []DAGStep{{ID: "s1", Name: "S1", Tool: "t1"}}
	_, _ = e.CreateWorkflow("wf", "", "agent", nil, steps, nil)
	_, _ = e.CreateWorkflow("wf", "", "agent", nil, steps, nil)
	_, _ = e.CreateWorkflow("wf", "", "agent", nil, steps, nil)

	versions, err := e.GetVersions("wf")
	if err != nil {
		t.Fatalf("GetVersions failed: %v", err)
	}
	if len(versions) != 3 {
		t.Errorf("expected 3 versions, got %d", len(versions))
	}
}

func TestGetVersionsNotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, err := e.GetVersions("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent workflow name")
	}
}

// --- ResolveStepArgs ---

func TestResolveStepArgsInterpolation(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	step := DAGStep{
		Args: map[string]string{
			"topic":   "${topic_name}",
			"group":   "${group_id}",
			"literal": "no_param",
		},
	}
	params := map[string]any{
		"topic_name": "orders",
		"group_id":   "my-group",
	}

	resolved := e.ResolveStepArgs(step, params)
	if resolved["topic"] != "orders" {
		t.Errorf("expected orders, got %v", resolved["topic"])
	}
	if resolved["group"] != "my-group" {
		t.Errorf("expected my-group, got %v", resolved["group"])
	}
	if resolved["literal"] != "no_param" {
		t.Errorf("expected no_param, got %v", resolved["literal"])
	}
}

func TestResolveStepArgsMissingParam(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	step := DAGStep{Args: map[string]string{"topic": "${missing}"}}
	resolved := e.ResolveStepArgs(step, map[string]any{})
	if resolved["topic"] != "${missing}" {
		t.Errorf("expected raw template for missing param, got %v", resolved["topic"])
	}
}

func TestResolveStepArgsEmptyParams(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	step := DAGStep{Args: map[string]string{"key": "value"}}
	resolved := e.ResolveStepArgs(step, nil)
	if resolved["key"] != "value" {
		t.Errorf("expected value, got %v", resolved["key"])
	}
}

// --- interpolate ---

func TestInterpolateSubstitution(t *testing.T) {
	t.Parallel()
	result := interpolate("Hello ${name}!", map[string]any{"name": "World"})
	if result != "Hello World!" {
		t.Errorf("expected 'Hello World!', got '%s'", result)
	}
}

func TestInterpolateMultipleParams(t *testing.T) {
	t.Parallel()
	result := interpolate("${a}-${b}", map[string]any{"a": "x", "b": "y"})
	if result != "x-y" {
		t.Errorf("expected x-y, got %s", result)
	}
}

func TestInterpolateUndefinedParam(t *testing.T) {
	t.Parallel()
	result := interpolate("${undef}", map[string]any{})
	if result != "${undef}" {
		t.Errorf("expected ${undef}, got %s", result)
	}
}

func TestInterpolateEmptyParams(t *testing.T) {
	t.Parallel()
	result := interpolate("no params here", nil)
	if result != "no params here" {
		t.Errorf("expected 'no params here', got '%s'", result)
	}
}

func TestInterpolateNumericParam(t *testing.T) {
	t.Parallel()
	result := interpolate("count=${n}", map[string]any{"n": 42})
	if result != "count=42" {
		t.Errorf("expected count=42, got %s", result)
	}
}

// --- ListRuns ---

func TestListRunsFilterByTemplate(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	steps := []DAGStep{{ID: "s1", Name: "S1", Tool: "t1"}}
	wf1, _ := e.CreateWorkflow("wf1", "", "agent", nil, steps, nil)
	wf2, _ := e.CreateWorkflow("wf2", "", "agent", nil, steps, nil)
	_, _ = e.StartRun(wf1.ID, "agent", nil)
	_, _ = e.StartRun(wf2.ID, "agent", nil)

	runs := e.ListRuns(wf1.ID, 50)
	if len(runs) != 1 {
		t.Errorf("expected 1 run for wf1, got %d", len(runs))
	}
}

func TestListRunsAll(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	steps := []DAGStep{{ID: "s1", Name: "S1", Tool: "t1"}}
	wf, _ := e.CreateWorkflow("wf", "", "agent", nil, steps, nil)
	_, _ = e.StartRun(wf.ID, "agent", nil)
	_, _ = e.StartRun(wf.ID, "agent", nil)

	runs := e.ListRuns("", 50)
	if len(runs) != 2 {
		t.Errorf("expected 2, got %d", len(runs))
	}
}

func TestListRunsLimit(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	steps := []DAGStep{{ID: "s1", Name: "S1", Tool: "t1"}}
	wf, _ := e.CreateWorkflow("wf", "", "agent", nil, steps, nil)
	for i := 0; i < 5; i++ {
		_, _ = e.StartRun(wf.ID, "agent", nil)
	}

	runs := e.ListRuns("", 2)
	if len(runs) != 2 {
		t.Errorf("expected 2, got %d", len(runs))
	}
}

func TestListRunsEmpty(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	runs := e.ListRuns("", 50)
	if len(runs) != 0 {
		t.Errorf("expected 0, got %d", len(runs))
	}
}

// --- CompleteStep ---

func TestCompleteStepTriggersWorkflowCompletion(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	steps := []DAGStep{{ID: "s1", Name: "S1", Tool: "t1"}}
	wf, _ := e.CreateWorkflow("wf", "", "agent", nil, steps, nil)
	run, _ := e.StartRun(wf.ID, "agent", nil)

	_ = e.CompleteStep(run.ID, "s1", "completed", "output", "")

	got, _ := e.GetRun(run.ID)
	if got.Status != "completed" {
		t.Errorf("expected completed, got %s", got.Status)
	}
	if got.CompletedAt == nil {
		t.Error("expected CompletedAt to be set")
	}
}

func TestCompleteStepFailed(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	steps := []DAGStep{{ID: "s1", Name: "S1", Tool: "t1"}}
	wf, _ := e.CreateWorkflow("wf", "", "agent", nil, steps, nil)
	run, _ := e.StartRun(wf.ID, "agent", nil)

	_ = e.CompleteStep(run.ID, "s1", "failed", nil, "error occurred")

	got, _ := e.GetRun(run.ID)
	if got.Status != "failed" {
		t.Errorf("expected failed, got %s", got.Status)
	}
}

func TestCompleteStepNonExistentRun(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	err := e.CompleteStep("nonexistent", "s1", "completed", nil, "")
	if err == nil {
		t.Error("expected error for nonexistent run")
	}
}

func TestCompleteStepNonExistentStep(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	steps := []DAGStep{{ID: "s1", Name: "S1", Tool: "t1"}}
	wf, _ := e.CreateWorkflow("wf", "", "agent", nil, steps, nil)
	run, _ := e.StartRun(wf.ID, "agent", nil)

	err := e.CompleteStep(run.ID, "nonexistent", "completed", nil, "")
	if err == nil {
		t.Error("expected error for nonexistent step")
	}
}

// --- GetReadySteps ---

func TestGetReadyStepsAllDepsComplete(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	steps := []DAGStep{
		{ID: "s1", Name: "S1", Tool: "t1"},
		{ID: "s2", Name: "S2", Tool: "t2", DependsOn: []string{"s1"}},
	}
	wf, _ := e.CreateWorkflow("wf", "", "agent", nil, steps, nil)
	run, _ := e.StartRun(wf.ID, "agent", nil)

	_ = e.CompleteStep(run.ID, "s1", "completed", nil, "")

	ready, _ := e.GetReadySteps(run.ID)
	if len(ready) != 1 || ready[0].ID != "s2" {
		t.Errorf("expected s2 ready, got %v", ready)
	}
}

func TestGetReadyStepsPartialDeps(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	steps := []DAGStep{
		{ID: "s1", Name: "S1", Tool: "t1"},
		{ID: "s2", Name: "S2", Tool: "t2"},
		{ID: "s3", Name: "S3", Tool: "t3", DependsOn: []string{"s1", "s2"}},
	}
	wf, _ := e.CreateWorkflow("wf", "", "agent", nil, steps, nil)
	run, _ := e.StartRun(wf.ID, "agent", nil)

	_ = e.CompleteStep(run.ID, "s1", "completed", nil, "")

	ready, _ := e.GetReadySteps(run.ID)
	for _, s := range ready {
		if s.ID == "s3" {
			t.Error("s3 should not be ready (s2 not completed)")
		}
	}
}

func TestGetReadyStepsSkippedDepsCount(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	steps := []DAGStep{
		{ID: "s1", Name: "S1", Tool: "t1"},
		{ID: "s2", Name: "S2", Tool: "t2", DependsOn: []string{"s1"}},
	}
	wf, _ := e.CreateWorkflow("wf", "", "agent", nil, steps, nil)
	run, _ := e.StartRun(wf.ID, "agent", nil)

	_ = e.CompleteStep(run.ID, "s1", "skipped", nil, "")

	ready, _ := e.GetReadySteps(run.ID)
	if len(ready) != 1 || ready[0].ID != "s2" {
		t.Error("s2 should be ready when s1 is skipped")
	}
}

func TestGetReadyStepsNonExistentRun(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, err := e.GetReadySteps("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent run")
	}
}

// --- GetRun ---

func TestGetRunFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	steps := []DAGStep{{ID: "s1", Name: "S1", Tool: "t1"}}
	wf, _ := e.CreateWorkflow("wf", "", "agent", nil, steps, nil)
	run, _ := e.StartRun(wf.ID, "agent", nil)

	got, err := e.GetRun(run.ID)
	if err != nil {
		t.Fatalf("GetRun failed: %v", err)
	}
	if got.Status != "running" {
		t.Errorf("expected running, got %s", got.Status)
	}
}

func TestGetRunNotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, err := e.GetRun("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent run")
	}
}

// --- Edge cases ---

func TestCreateWorkflowNoSteps(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, err := e.CreateWorkflow("empty", "", "agent", nil, []DAGStep{}, nil)
	if err == nil {
		t.Error("expected error for empty steps")
	}
}

func TestCreateWorkflowEmptyName(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, err := e.CreateWorkflow("", "", "agent", nil, []DAGStep{{ID: "s1", Tool: "t1"}}, nil)
	if err == nil {
		t.Error("expected error for empty name")
	}
}

func TestStartRunWithRequiredParams(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	steps := []DAGStep{{ID: "s1", Name: "S1", Tool: "t1"}}
	params := []Parameter{{Name: "topic", Required: true}}
	wf, _ := e.CreateWorkflow("wf", "", "agent", params, steps, nil)

	_, err := e.StartRun(wf.ID, "agent", nil)
	if err == nil {
		t.Error("expected error for missing required param")
	}
}

func TestStartRunWithDefaultParams(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	steps := []DAGStep{{ID: "s1", Name: "S1", Tool: "t1"}}
	params := []Parameter{{Name: "topic", Required: true, Default: "orders"}}
	wf, _ := e.CreateWorkflow("wf", "", "agent", params, steps, nil)

	run, err := e.StartRun(wf.ID, "agent", nil)
	if err != nil {
		t.Fatalf("StartRun failed: %v", err)
	}
	if run.Parameters["topic"] != "orders" {
		t.Errorf("expected default param orders, got %v", run.Parameters["topic"])
	}
}

func TestCancelAlreadyCancelled(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	steps := []DAGStep{{ID: "s1", Name: "S1", Tool: "t1"}}
	wf, _ := e.CreateWorkflow("wf", "", "agent", nil, steps, nil)
	run, _ := e.StartRun(wf.ID, "agent", nil)
	_ = e.CancelRun(run.ID)

	err := e.CancelRun(run.ID)
	if err == nil {
		t.Error("expected error for already canceled run")
	}
}
