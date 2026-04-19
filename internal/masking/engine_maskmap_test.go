package masking

import (
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/config"
)

// ===================== maskMap: nested map masking =====================

func TestMaskMap_NestedFieldPathTraversal(t *testing.T) {
	cfg := config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "email", Type: "email", FieldPath: "**.email"},
		},
	}
	e := NewEngine(cfg, testLogger())

	input := map[string]any{
		"user": map[string]any{
			"email": "user@example.com",
			"name":  "Alice",
		},
	}
	result := e.maskMap("agent-1", input, "")
	user := result["user"].(map[string]any)
	if user["email"] == "user@example.com" {
		t.Error("expected nested email to be masked")
	}
	if user["name"] != "Alice" {
		t.Error("expected name to remain unchanged")
	}
}

// ===================== maskMap: array field masking =====================

func TestMaskMap_ArrayOfStrings(t *testing.T) {
	cfg := config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "email", Type: "email"},
		},
	}
	e := NewEngine(cfg, testLogger())

	input := map[string]any{
		"contacts": []any{
			"alice@example.com",
			"bob@example.com",
			"not-an-email",
		},
	}
	result := e.maskMap("agent-1", input, "")
	contacts := result["contacts"].([]any)
	if contacts[0] == "alice@example.com" {
		t.Error("expected first email in array to be masked")
	}
	if contacts[1] == "bob@example.com" {
		t.Error("expected second email in array to be masked")
	}
	if contacts[2] != "not-an-email" {
		t.Error("expected non-email string unchanged")
	}
}

func TestMaskMap_ArrayOfMaps(t *testing.T) {
	cfg := config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "email", Type: "email", FieldPath: "**email**"},
		},
	}
	e := NewEngine(cfg, testLogger())

	input := map[string]any{
		"users": []any{
			map[string]any{"email": "a@test.com", "name": "A"},
			map[string]any{"email": "b@test.com", "name": "B"},
		},
	}
	result := e.maskMap("agent-1", input, "")
	users := result["users"].([]any)

	u0 := users[0].(map[string]any)
	if u0["email"] == "a@test.com" {
		t.Error("expected first user email masked")
	}
	if u0["name"] != "A" {
		t.Error("expected name unchanged")
	}

	u1 := users[1].(map[string]any)
	if u1["email"] == "b@test.com" {
		t.Error("expected second user email masked")
	}
}

// ===================== maskMap: mixed types =====================

func TestMaskMap_MixedTypes(t *testing.T) {
	cfg := config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "ssn", Type: "ssn"},
		},
	}
	e := NewEngine(cfg, testLogger())

	input := map[string]any{
		"ssn":     "123-45-6789",
		"age":     float64(30),
		"active":  true,
		"nothing": nil,
	}
	result := e.maskMap("agent-1", input, "")
	if result["ssn"] == "123-45-6789" {
		t.Error("expected SSN to be masked")
	}
	if result["age"] != float64(30) {
		t.Error("expected numeric value unchanged")
	}
	if result["active"] != true {
		t.Error("expected boolean value unchanged")
	}
}

// ===================== maskMap: deeply nested (3+ levels) =====================

func TestMaskMap_DeeplyNested(t *testing.T) {
	cfg := config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "email", Type: "email", FieldPath: "**email**"},
		},
	}
	e := NewEngine(cfg, testLogger())

	input := map[string]any{
		"level1": map[string]any{
			"level2": map[string]any{
				"level3": map[string]any{
					"email": "deep@test.com",
					"data":  "ok",
				},
			},
		},
	}
	result := e.maskMap("agent-1", input, "")
	l1 := result["level1"].(map[string]any)
	l2 := l1["level2"].(map[string]any)
	l3 := l2["level3"].(map[string]any)
	if l3["email"] == "deep@test.com" {
		t.Error("expected deeply nested email to be masked")
	}
	if l3["data"] != "ok" {
		t.Error("expected non-email field unchanged")
	}
}

// ===================== maskMap: wildcard field path =====================

func TestMaskMap_WildcardFieldPath(t *testing.T) {
	cfg := config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "ssn-wildcard", Type: "ssn", FieldPath: "*.ssn"},
		},
	}
	e := NewEngine(cfg, testLogger())

	input := map[string]any{
		"customer": map[string]any{
			"ssn":  "123-45-6789",
			"name": "Test",
		},
	}
	result := e.maskMap("agent-1", input, "")
	customer := result["customer"].(map[string]any)
	if customer["ssn"] == "123-45-6789" {
		t.Error("expected SSN to be masked via wildcard")
	}
}

// ===================== maskMap: NER-based detection =====================

func TestMaskMap_NERDetectionInNestedStrings(t *testing.T) {
	cfg := config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "ner-detect", Type: "ner"},
		},
	}
	e := NewEngine(cfg, testLogger())

	input := map[string]any{
		"profile": map[string]any{
			"bio": "Contact me at john@test.com or 123-45-6789",
		},
	}
	result := e.maskMap("agent-1", input, "")
	profile := result["profile"].(map[string]any)
	bio := profile["bio"].(string)
	if bio == "Contact me at john@test.com or 123-45-6789" {
		t.Error("expected NER to mask PII in nested string")
	}
}

// ===================== maskMap: no matching fields =====================

func TestMaskMap_NoMatchingFields(t *testing.T) {
	cfg := config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "email", Type: "email", FieldPath: "**.email"},
		},
	}
	e := NewEngine(cfg, testLogger())

	input := map[string]any{
		"name":    "Alice",
		"address": "123 Main St",
	}
	result := e.maskMap("agent-1", input, "")
	if result["name"] != "Alice" {
		t.Error("expected name unchanged")
	}
	if result["address"] != "123 Main St" {
		t.Error("expected address unchanged")
	}
}

// ===================== maskMap: nil/empty map =====================

func TestMaskMap_EmptyMap(t *testing.T) {
	cfg := config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "email", Type: "email"},
		},
	}
	e := NewEngine(cfg, testLogger())
	result := e.maskMap("agent-1", map[string]any{}, "")
	if len(result) != 0 {
		t.Errorf("expected empty map, got %d entries", len(result))
	}
}

// ===================== maskMap: array with non-string/non-map items =====================

func TestMaskMap_ArrayWithPrimitives(t *testing.T) {
	cfg := config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "email", Type: "email"},
		},
	}
	e := NewEngine(cfg, testLogger())

	input := map[string]any{
		"numbers": []any{float64(1), float64(2), float64(3)},
		"mixed":   []any{float64(42), true, nil},
	}
	result := e.maskMap("agent-1", input, "")
	nums := result["numbers"].([]any)
	if len(nums) != 3 {
		t.Errorf("expected 3 elements, got %d", len(nums))
	}
	if nums[0] != float64(1) {
		t.Error("expected numeric array elements unchanged")
	}
}
