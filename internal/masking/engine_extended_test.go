package masking

import (
	"log/slog"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/config"
)

func TestMatchFieldPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		pattern string
		path    string
		want    bool
	}{
		{"email", "email", true},
		{"email", "user_email", false},
		{"*.email", "user.email", true},
		{"user.*", "user.name", true},
		{"user.*", "admin.name", false},
		{"", "", true},
		{"a.b.c", "a.b.c", true},
		{"a.b.c", "a.b.d", false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.path, func(t *testing.T) {
			t.Parallel()
			got := matchFieldPath(tt.pattern, tt.path)
			if got != tt.want {
				t.Errorf("matchFieldPath(%q, %q) = %v, want %v", tt.pattern, tt.path, got, tt.want)
			}
		})
	}
}

func TestMatchParts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		pattern []string
		path    []string
		want    bool
	}{
		{"exact", []string{"a", "b"}, []string{"a", "b"}, true},
		{"mismatch", []string{"a", "b"}, []string{"a", "c"}, false},
		{"wildcard", []string{"a", "*"}, []string{"a", "b"}, true},
		{"globstar", []string{"**"}, []string{"a", "b", "c"}, true},
		{"empty both", []string{}, []string{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := matchParts(tt.pattern, tt.path)
			if got != tt.want {
				t.Errorf("matchParts(%v, %v) = %v, want %v", tt.pattern, tt.path, got, tt.want)
			}
		})
	}
}

func TestLuhnCheckExtended(t *testing.T) {
	t.Parallel()
	tests := []struct {
		number string
		want   bool
	}{
		{"4111111111111111", true},  // Visa test card
		{"5500000000000004", true},  // Mastercard test card
		{"1234567890123456", false}, // Invalid
		{"0000000000000000", true},  // All zeros passes Luhn
		{"", false},
		{"abc", false},
	}

	for _, tt := range tests {
		t.Run(tt.number, func(t *testing.T) {
			t.Parallel()
			got := luhnCheck(tt.number)
			if got != tt.want {
				t.Errorf("luhnCheck(%q) = %v, want %v", tt.number, got, tt.want)
			}
		})
	}
}

func TestMaskLuhnNumbers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{"card in text", "Card: 4111111111111111 is valid", "Card: ****REDACTED**** is valid"},
		{"no card", "Hello world", "Hello world"},
		{"short number", "1234", "1234"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := maskLuhnNumbers(tt.value, "****REDACTED****")
			if got != tt.want {
				t.Errorf("maskLuhnNumbers(%q) = %q, want %q", tt.value, got, tt.want)
			}
		})
	}
}

func TestEngineEnabled(t *testing.T) {
	t.Parallel()
	e := NewEngine(config.MaskingConfig{Enabled: true}, slog.Default())
	if !e.Enabled() {
		t.Error("expected engine to be enabled")
	}
}

func TestEngineRuleCount(t *testing.T) {
	t.Parallel()
	e := NewEngine(config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "r1", Type: "email"},
			{Name: "r2", Type: "ssn"},
		},
	}, slog.Default())
	if e.RuleCount() != 2 {
		t.Errorf("expected 2 rules, got %d", e.RuleCount())
	}
}

func TestAddRule(t *testing.T) {
	t.Parallel()
	e := NewEngine(config.MaskingConfig{Enabled: true}, slog.Default())
	err := e.AddRule("phone", "phone", "*.phone", "", "***-***-****", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.RuleCount() != 1 {
		t.Errorf("expected 1 rule, got %d", e.RuleCount())
	}
}

func TestRuleApplies(t *testing.T) {
	t.Parallel()
	e := NewEngine(config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "restricted", Type: "email", Agents: []string{"agent-1"}},
			{Name: "global", Type: "ssn"},
		},
	}, slog.Default())

	// Global rule applies to any agent
	if !e.ruleApplies(e.rules[1], "any-agent") {
		t.Error("expected global rule to apply to any agent")
	}

	// Agent-specific rule
	if !e.ruleApplies(e.rules[0], "agent-1") {
		t.Error("expected rule to apply to agent-1")
	}
	if e.ruleApplies(e.rules[0], "agent-2") {
		t.Error("expected rule NOT to apply to agent-2")
	}
}

func TestMaskValueNil(t *testing.T) {
	t.Parallel()
	e := NewEngine(config.MaskingConfig{Enabled: true}, slog.Default())
	result := e.MaskValue("agent", nil)
	if result != nil {
		t.Error("expected nil for nil input")
	}
}

func TestMaskValueString(t *testing.T) {
	t.Parallel()
	e := NewEngine(config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "email", Type: "email"},
		},
	}, slog.Default())
	result := e.MaskValue("agent", "Contact: user@example.com")
	str, ok := result.(string)
	if !ok {
		t.Fatalf("expected string result, got %T", result)
	}
	if str == "Contact: user@example.com" {
		t.Error("expected email to be masked")
	}
}

func TestMaskValueDisabled(t *testing.T) {
	t.Parallel()
	e := NewEngine(config.MaskingConfig{Enabled: false}, slog.Default())
	input := "user@example.com"
	result := e.MaskValue("agent", input)
	if result != input {
		t.Error("expected unmasked result when engine is disabled")
	}
}

func TestMaskMap(t *testing.T) {
	t.Parallel()
	e := NewEngine(config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "email", Type: "email", FieldPath: "*.email"},
		},
	}, slog.Default())

	input := map[string]any{
		"name":  "John",
		"email": "john@example.com",
	}
	result := e.MaskValue("agent", input)
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map result, got %T", result)
	}
	if m["name"] != "John" {
		t.Error("expected name to be unchanged")
	}
}

func TestPhoneMasking(t *testing.T) {
	t.Parallel()
	e := NewEngine(config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "phone", Type: "phone"},
		},
	}, slog.Default())
	result := e.MaskValue("agent", "Call me at 555-123-4567")
	str, ok := result.(string)
	if !ok {
		t.Fatalf("expected string result, got %T", result)
	}
	if str == "Call me at 555-123-4567" {
		t.Error("expected phone number to be masked")
	}
}
