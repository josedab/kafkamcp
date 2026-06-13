package masking

import (
	"testing"

	"github.com/josedab/kafkamcp/internal/config"
)

func TestMaskValueChecked_MasksNestedFields(t *testing.T) {
	t.Parallel()
	cfg := config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "email", Type: "email", FieldPath: "**email**"},
		},
	}
	e := NewEngine(cfg, testLogger())

	out, err := e.MaskValueChecked("agent-1", map[string]any{
		"user": map[string]any{"email": "john@example.com", "name": "John"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := out.(map[string]any)
	nested := m["user"].(map[string]any)
	if nested["email"] == "john@example.com" {
		t.Error("expected nested email to be masked")
	}
	if nested["name"] != "John" {
		t.Error("expected non-matching field preserved")
	}
}

func TestMaskValueChecked_DisabledPassthrough(t *testing.T) {
	t.Parallel()
	e := NewEngine(config.MaskingConfig{Enabled: false}, testLogger())
	out, err := e.MaskValueChecked("agent-1", "john@example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "john@example.com" {
		t.Errorf("disabled engine must pass through, got %v", out)
	}
}

func TestMaskValueChecked_NilValue(t *testing.T) {
	t.Parallel()
	cfg := config.MaskingConfig{
		Enabled: true,
		Rules:   []config.MaskingRule{{Name: "email", Type: "email"}},
	}
	e := NewEngine(cfg, testLogger())
	out, err := e.MaskValueChecked("agent-1", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != nil {
		t.Errorf("expected nil passthrough, got %v", out)
	}
}

func TestMaskValueChecked_MasksTopLevelArrays(t *testing.T) {
	t.Parallel()
	e := NewEngine(config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "email", Type: "email", FieldPath: "**email**"},
		},
	}, testLogger())
	out, err := e.MaskValueChecked("agent-1", []any{
		map[string]any{"email": "first@example.com"},
		[]any{map[string]any{"email": "second@example.com"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	values := out.([]any)
	if values[0].(map[string]any)["email"] == "first@example.com" {
		t.Fatal("top-level array object was not masked")
	}
	nested := values[1].([]any)[0].(map[string]any)
	if nested["email"] == "second@example.com" {
		t.Fatal("nested array object was not masked")
	}
}

func TestMaskValueChecked_PreservesScalarTypes(t *testing.T) {
	t.Parallel()
	e := NewEngine(config.MaskingConfig{
		Enabled: true,
		Rules:   []config.MaskingRule{{Name: "email", Type: "email"}},
	}, testLogger())
	for _, input := range []any{42, true, 3.5} {
		out, err := e.MaskValueChecked("agent-1", input)
		if err != nil {
			t.Fatal(err)
		}
		if out != input {
			t.Fatalf("scalar %T changed from %v to %v (%T)", input, input, out, out)
		}
	}
}

func TestMaskValueChecked_FailsClosedOnUnsupportedValue(t *testing.T) {
	t.Parallel()
	e := NewEngine(config.MaskingConfig{
		Enabled: true,
		Rules:   []config.MaskingRule{{Name: "email", Type: "email"}},
	}, testLogger())
	if _, err := e.MaskValueChecked("agent-1", make(chan int)); err == nil {
		t.Fatal("unsupported value should fail closed")
	}
}
