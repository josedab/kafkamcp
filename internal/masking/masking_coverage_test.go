package masking

import (
	"log/slog"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/config"
)

func TestAddRuleAllTypes(t *testing.T) {
	e := NewEngine(config.MaskingConfig{Enabled: true}, slog.Default())

	tests := []struct {
		name     string
		ruleType string
		pattern  string
		wantErr  bool
	}{
		{"email-rule", "email", "", false},
		{"ssn-rule", "ssn", "", false},
		{"phone-rule", "phone", "", false},
		{"credit_card-rule", "credit_card", "", false},
		{"regex-rule", "regex", `\d{3}-\d{3}`, false},
		{"ner-rule", "ner", "", false},
		{"unknown-rule", "unknown", "", true},
		{"regex-missing-pattern", "regex", "", true},
		{"regex-invalid-pattern", "regex", "[invalid", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := e.AddRule(tt.name, tt.ruleType, "field", tt.pattern, "", nil)
			if tt.wantErr && err == nil {
				t.Error("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestAddRuleWithAgents(t *testing.T) {
	e := NewEngine(config.MaskingConfig{Enabled: true}, slog.Default())
	err := e.AddRule("agent-email", "email", "email", "", "***", []string{"agent-1", "agent-2"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAddRuleDefaultReplace(t *testing.T) {
	e := NewEngine(config.MaskingConfig{Enabled: true}, slog.Default())
	err := e.AddRule("default-replace", "email", "email", "", "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMaskValueNotEnabled(t *testing.T) {
	e := NewEngine(config.MaskingConfig{Enabled: false}, slog.Default())
	result := e.MaskValue("agent", "test@example.com")
	if result != "test@example.com" {
		t.Errorf("expected unchanged value when not enabled, got %v", result)
	}
}

func TestMaskValueNoRules(t *testing.T) {
	e := NewEngine(config.MaskingConfig{Enabled: true}, slog.Default())
	result := e.MaskValue("agent", "test@example.com")
	if result != "test@example.com" {
		t.Errorf("expected unchanged value with no rules, got %v", result)
	}
}

func TestMaskValueWithMapValue(t *testing.T) {
	e := NewEngine(config.MaskingConfig{Enabled: true}, slog.Default())
	e.AddRule("email-mask", "email", "email", "", "***", nil)

	input := map[string]any{
		"email": "user@example.com",
		"name":  "John",
	}
	result := e.MaskValue("agent", input)
	masked, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map result, got %T", result)
	}
	if masked["name"] != "John" {
		t.Errorf("expected name unchanged, got %v", masked["name"])
	}
}

func TestMaskValueWithNonMapNonString(t *testing.T) {
	e := NewEngine(config.MaskingConfig{Enabled: true}, slog.Default())
	e.AddRule("email-mask", "email", "", "", "***", nil)

	// A struct value that can be JSON marshaled
	type testStruct struct {
		Email string `json:"email"`
		Name  string `json:"name"`
	}
	result := e.MaskValue("agent", testStruct{Email: "user@example.com", Name: "John"})
	if result == nil {
		t.Error("expected non-nil result")
	}
}
