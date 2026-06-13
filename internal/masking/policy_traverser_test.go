package masking

import (
	"log/slog"
	"testing"

	"github.com/josedab/kafkamcp/internal/config"
)

func newTestMaskingEngine() *Engine {
	return NewEngine(config.MaskingConfig{Enabled: true}, slog.Default())
}

func newDisabledMaskingEngine() *Engine {
	return NewEngine(config.MaskingConfig{Enabled: false}, slog.Default())
}

func TestNewJSONPathTraverser_Creates(t *testing.T) {
	t.Parallel()
	engine := newTestMaskingEngine()
	traverser := NewJSONPathTraverser(engine)
	if traverser == nil {
		t.Fatal("expected non-nil traverser")
	}
	if traverser.engine != engine {
		t.Error("expected traverser engine to match")
	}
}

func TestTraverseAndMask_ValidJSON(t *testing.T) {
	t.Parallel()
	engine := newDisabledMaskingEngine()
	traverser := NewJSONPathTraverser(engine)

	data := []byte(`{"name":"Alice","email":"alice@example.com"}`)
	result, err := traverser.TraverseAndMask("agent-1", data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) == 0 {
		t.Error("expected non-empty result")
	}
}

func TestTraverseAndMask_InvalidJSON(t *testing.T) {
	t.Parallel()
	engine := newDisabledMaskingEngine()
	traverser := NewJSONPathTraverser(engine)

	data := []byte("not valid json at all")
	result, err := traverser.TraverseAndMask("agent-1", data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Invalid JSON is treated as raw string, returned after masking
	if len(result) == 0 {
		t.Error("expected non-empty result for raw string")
	}
}

func TestTraverseAndMask_NestedJSON(t *testing.T) {
	t.Parallel()
	engine := newDisabledMaskingEngine()
	traverser := NewJSONPathTraverser(engine)

	data := []byte(`{"user":{"profile":{"email":"test@example.com"}}}`)
	result, err := traverser.TraverseAndMask("agent-1", data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) == 0 {
		t.Error("expected non-empty result for nested JSON")
	}
}

func TestTraverseAndMask_BinaryData(t *testing.T) {
	t.Parallel()
	engine := newDisabledMaskingEngine()
	traverser := NewJSONPathTraverser(engine)

	data := []byte{0x00, 0xFF, 0xFE, 0x01}
	result, err := traverser.TraverseAndMask("agent-1", data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Binary data is not valid JSON, treated as raw string
	if result == nil {
		t.Error("expected non-nil result for binary data")
	}
}

func TestTraverseAndMask_EmptyInput(t *testing.T) {
	t.Parallel()
	engine := newDisabledMaskingEngine()
	traverser := NewJSONPathTraverser(engine)

	data := []byte("")
	result, err := traverser.TraverseAndMask("agent-1", data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = result // Should not panic
}

func TestDetectPII_EmailPattern(t *testing.T) {
	t.Parallel()
	detections := DetectPII("send to user@example.com please")
	found := false
	for _, d := range detections {
		if d.Type == "email" {
			found = true
		}
	}
	if !found {
		t.Error("expected email detection")
	}
}

func TestDetectPII_SSNPattern(t *testing.T) {
	t.Parallel()
	detections := DetectPII("SSN is 123-45-6789")
	found := false
	for _, d := range detections {
		if d.Type == "ssn" {
			found = true
		}
	}
	if !found {
		t.Error("expected SSN detection")
	}
}

func TestDetectPII_CreditCardPattern(t *testing.T) {
	t.Parallel()
	detections := DetectPII("Card: 4111 1111 1111 1111")
	found := false
	for _, d := range detections {
		if d.Type == "credit_card" {
			found = true
		}
	}
	if !found {
		t.Error("expected credit card detection")
	}
}

func TestDetectPII_PhonePattern(t *testing.T) {
	t.Parallel()
	detections := DetectPII("Call (555) 123-4567")
	found := false
	for _, d := range detections {
		if d.Type == "phone" {
			found = true
		}
	}
	if !found {
		t.Error("expected phone detection")
	}
}

func TestMaskPartial_MasksAllButLastN(t *testing.T) {
	t.Parallel()
	tests := []struct {
		value   string
		visible int
		want    string
	}{
		{"1234567890", 4, "******7890"},
		{"abc", 1, "**c"},
		{"a", 5, "*"},
	}
	for _, tt := range tests {
		got := MaskPartial(tt.value, tt.visible)
		if got != tt.want {
			t.Errorf("MaskPartial(%q, %d) = %q, want %q", tt.value, tt.visible, got, tt.want)
		}
	}
}

func TestHashValue_DeterministicOutput(t *testing.T) {
	t.Setenv(DefaultKeyEnv, "0123456789abcdef0123456789abcdef")
	h1 := HashValue("test-input")
	h2 := HashValue("test-input")
	if h1 != h2 {
		t.Errorf("expected deterministic hash, got %q and %q", h1, h2)
	}
}

func TestHashValue_TokPrefixOutput(t *testing.T) {
	t.Setenv(DefaultKeyEnv, "0123456789abcdef0123456789abcdef")
	h := HashValue("anything")
	if len(h) < 4 || h[:4] != "tok_" {
		t.Errorf("expected tok_ prefix, got %q", h)
	}
}

func TestHashValue_DifferentInputsDifferentHashes(t *testing.T) {
	t.Setenv(DefaultKeyEnv, "0123456789abcdef0123456789abcdef")
	h1 := HashValue("input-a")
	h2 := HashValue("input-b")
	if h1 == h2 {
		t.Error("expected different hashes for different inputs")
	}
}
