package llmpipeline

import (
	"testing"
)

func TestNewEngine(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	if e == nil {
		t.Fatal("NewEngine returned nil")
	}
}

func TestCreatePipeline(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	cfg, err := e.CreatePipeline(PipelineConfig{
		Topics: []string{"orders"},
		Format: FormatJSONL,
	})
	if err != nil {
		t.Fatalf("CreatePipeline error: %v", err)
	}
	if cfg.ID == "" {
		t.Error("expected pipeline ID")
	}
	if cfg.MaxMessages != 100000 {
		t.Errorf("MaxMessages = %d, want 100000", cfg.MaxMessages)
	}
}

func TestCreatePipelineValidation(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	_, err := e.CreatePipeline(PipelineConfig{})
	if err == nil {
		t.Error("expected error for no topics")
	}
}

func TestExport(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	cfg, _ := e.CreatePipeline(PipelineConfig{
		Topics:       []string{"orders"},
		Format:       FormatJSONL,
		PIIRedaction: true,
		Deduplicate:  true,
	})

	messages := []map[string]any{
		{"name": "Alice", "email": "alice@test.com", "value": 100},
		{"name": "Bob", "phone": "555-1234", "value": 200},
		{"name": "Alice", "email": "alice@test.com", "value": 100}, // duplicate
	}

	result, err := e.Export(cfg.ID, messages)
	if err != nil {
		t.Fatalf("Export error: %v", err)
	}
	if result.Status != "completed" {
		t.Errorf("Status = %q, want completed", result.Status)
	}
	if result.MessagesDeduplicated != 1 {
		t.Errorf("Deduplicated = %d, want 1", result.MessagesDeduplicated)
	}
	if result.PIIFieldsRedacted == 0 {
		t.Error("expected PII fields to be redacted")
	}
}

func TestExportCSV(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	cfg, _ := e.CreatePipeline(PipelineConfig{
		Topics: []string{"orders"},
		Format: FormatCSV,
	})

	messages := []map[string]any{
		{"name": "Alice", "value": 100},
		{"name": "Bob", "value": 200},
	}

	result, err := e.Export(cfg.ID, messages)
	if err != nil {
		t.Fatalf("Export error: %v", err)
	}
	if result.MessagesExported != 2 {
		t.Errorf("Exported = %d, want 2", result.MessagesExported)
	}
}

func TestExportNotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	_, err := e.Export("nonexistent", nil)
	if err == nil {
		t.Error("expected error for nonexistent pipeline")
	}
}

func TestSamplingRandom(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	cfg, _ := e.CreatePipeline(PipelineConfig{
		Topics: []string{"orders"},
		Sampling: SamplingConfig{
			Strategy: SamplingRandom,
			Rate:     0.5,
		},
	})

	messages := make([]map[string]any, 1000)
	for i := range messages {
		messages[i] = map[string]any{"id": i}
	}

	result, _ := e.Export(cfg.ID, messages)
	// With 50% sampling, expect roughly 400-600 messages
	if result.MessagesExported > 800 || result.MessagesExported < 200 {
		t.Errorf("Expected ~500 messages with 50%% sampling, got %d", result.MessagesExported)
	}
}

func TestFilterFields(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	cfg, _ := e.CreatePipeline(PipelineConfig{
		Topics: []string{"orders"},
		Fields: []string{"name"},
	})

	messages := []map[string]any{
		{"name": "Alice", "email": "a@b.com", "value": 100},
	}

	result, _ := e.Export(cfg.ID, messages)
	if result.MessagesExported != 1 {
		t.Errorf("Exported = %d, want 1", result.MessagesExported)
	}
}

func TestListPipelines(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	_, _ = e.CreatePipeline(PipelineConfig{Topics: []string{"a"}})
	_, _ = e.CreatePipeline(PipelineConfig{Topics: []string{"b"}})

	pipelines := e.ListPipelines()
	if len(pipelines) != 2 {
		t.Errorf("expected 2 pipelines, got %d", len(pipelines))
	}
}

func TestStratifiedSampling(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	cfg, _ := e.CreatePipeline(PipelineConfig{
		Topics: []string{"orders"},
		Sampling: SamplingConfig{
			Strategy:      SamplingStratified,
			StratifyField: "category",
			MaxPerStratum: 2,
		},
	})

	messages := []map[string]any{
		{"category": "A", "id": 1},
		{"category": "A", "id": 2},
		{"category": "A", "id": 3},
		{"category": "B", "id": 4},
		{"category": "B", "id": 5},
	}

	result, _ := e.Export(cfg.ID, messages)
	// 2 per stratum × 2 strata = 4
	if result.MessagesExported != 4 {
		t.Errorf("Expected 4 messages with stratified sampling, got %d", result.MessagesExported)
	}
}
