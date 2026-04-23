package pipeline

import (
	"log/slog"
	"testing"
)

func newTestEngine() *Engine {
	return NewEngine(slog.Default())
}

func TestCreatePipeline(t *testing.T) {
	e := newTestEngine()

	steps := []Step{
		{Type: "filter", Name: "filter-errors", Config: map[string]string{"field": "value", "operator": "contains", "value": "error"}},
		{Type: "map", Name: "uppercase", Config: map[string]string{"expression": "uppercase", "target": "value"}},
	}

	pipe, err := e.CreatePipeline("error-pipeline", "logs", "errors", "agent-1", steps)
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}
	if pipe.Name != "error-pipeline" {
		t.Fatalf("expected name error-pipeline, got %s", pipe.Name)
	}
	if len(pipe.Steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(pipe.Steps))
	}
}

func TestExecuteFilter(t *testing.T) {
	e := newTestEngine()

	steps := []Step{
		{Type: "filter", Name: "filter-errors", Config: map[string]string{
			"field": "value", "operator": "contains", "value": "error",
		}},
	}

	pipe, _ := e.CreatePipeline("test", "src", "", "agent", steps)

	messages := []PipelineMessage{
		{Value: "error occurred in auth service"},
		{Value: "user logged in successfully"},
		{Value: "database error timeout"},
	}

	run, output, err := e.Execute(pipe.ID, messages)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if run.InputCount != 3 {
		t.Fatalf("expected 3 input, got %d", run.InputCount)
	}
	if len(output) != 2 {
		t.Fatalf("expected 2 output (filtered), got %d", len(output))
	}
}

func TestExecuteMap(t *testing.T) {
	e := newTestEngine()

	steps := []Step{
		{Type: "map", Name: "uppercase", Config: map[string]string{
			"expression": "uppercase", "target": "value",
		}},
	}

	pipe, _ := e.CreatePipeline("test", "src", "", "agent", steps)

	messages := []PipelineMessage{
		{Value: "hello world"},
	}

	_, output, err := e.Execute(pipe.ID, messages)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if output[0].Value != "HELLO WORLD" {
		t.Fatalf("expected HELLO WORLD, got %s", output[0].Value)
	}
}

func TestExecuteEnrich(t *testing.T) {
	e := newTestEngine()

	steps := []Step{
		{Type: "enrich", Name: "add-headers", Config: map[string]string{
			"header_source": "pipeline",
		}},
	}

	pipe, _ := e.CreatePipeline("test", "src", "", "agent", steps)

	messages := []PipelineMessage{
		{Value: "test message"},
	}

	_, output, err := e.Execute(pipe.ID, messages)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if output[0].Headers["source"] != "pipeline" {
		t.Fatalf("expected header source=pipeline, got %v", output[0].Headers)
	}
}

func TestInvalidStepType(t *testing.T) {
	e := newTestEngine()

	steps := []Step{
		{Type: "invalid_type", Name: "bad"},
	}

	_, err := e.CreatePipeline("test", "src", "", "agent", steps)
	if err == nil {
		t.Fatal("expected error for invalid step type")
	}
}

func TestListAndDelete(t *testing.T) {
	e := newTestEngine()

	steps := []Step{{Type: "filter", Name: "f", Config: map[string]string{"field": "value", "operator": "exists"}}}
	pipe, _ := e.CreatePipeline("test", "src", "", "agent", steps)

	pipelines := e.ListPipelines()
	if len(pipelines) != 1 {
		t.Fatalf("expected 1 pipeline, got %d", len(pipelines))
	}

	err := e.DeletePipeline(pipe.ID)
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	pipelines = e.ListPipelines()
	if len(pipelines) != 0 {
		t.Fatalf("expected 0 pipelines after delete, got %d", len(pipelines))
	}
}
