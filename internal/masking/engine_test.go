package masking

import (
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/config"
)

func TestEmailMasking(t *testing.T) {
	cfg := config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "email", Type: "email", FieldPath: "**email**"},
		},
	}
	e := NewEngine(cfg, testLogger())

	result := e.MaskValue("agent-1", map[string]any{
		"user_email": "john@example.com",
		"name":       "John Doe",
	})

	m, ok := result.(map[string]any)
	if !ok {
		t.Fatal("expected map result")
	}
	if m["user_email"] == "john@example.com" {
		t.Error("expected email to be masked")
	}
	if m["name"] != "John Doe" {
		t.Error("expected name to remain unchanged")
	}
}

func TestSSNMasking(t *testing.T) {
	cfg := config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "ssn", Type: "ssn"},
		},
	}
	e := NewEngine(cfg, testLogger())

	result := e.MaskValue("agent-1", "SSN: 123-45-6789")
	s, ok := result.(string)
	if !ok {
		t.Fatal("expected string result")
	}
	if s == "SSN: 123-45-6789" {
		t.Error("expected SSN to be masked")
	}
}

func TestCreditCardMasking(t *testing.T) {
	cfg := config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "cc", Type: "credit_card"},
		},
	}
	e := NewEngine(cfg, testLogger())

	result := e.MaskValue("agent-1", "Card: 4111-1111-1111-1111")
	s, ok := result.(string)
	if !ok {
		t.Fatal("expected string result")
	}
	if s == "Card: 4111-1111-1111-1111" {
		t.Error("expected credit card to be masked")
	}
}

func TestNERMasking(t *testing.T) {
	cfg := config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "ner", Type: "ner"},
		},
	}
	e := NewEngine(cfg, testLogger())

	result := e.MaskValue("agent-1", "Contact: john@test.com, SSN: 123-45-6789")
	s, ok := result.(string)
	if !ok {
		t.Fatal("expected string result")
	}
	if s == "Contact: john@test.com, SSN: 123-45-6789" {
		t.Error("expected PII to be masked by NER")
	}
}

func TestAgentSpecificRules(t *testing.T) {
	cfg := config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "email", Type: "email", Agents: []string{"restricted-agent"}},
		},
	}
	e := NewEngine(cfg, testLogger())

	// Should not mask for unrestricted agent
	result1 := e.MaskValue("admin-agent", "email: test@example.com")
	if result1 != "email: test@example.com" {
		t.Error("expected no masking for admin agent")
	}

	// Should mask for restricted agent
	result2 := e.MaskValue("restricted-agent", "email: test@example.com")
	if result2 == "email: test@example.com" {
		t.Error("expected masking for restricted agent")
	}
}

func TestDisabledEngine(t *testing.T) {
	cfg := config.MaskingConfig{Enabled: false}
	e := NewEngine(cfg, testLogger())

	result := e.MaskValue("agent-1", "SSN: 123-45-6789")
	if result != "SSN: 123-45-6789" {
		t.Error("expected no masking when engine is disabled")
	}
}

func TestLuhnCheck(t *testing.T) {
	tests := []struct {
		number string
		valid  bool
	}{
		{"4111111111111111", true},
		{"1234567890123456", false},
		{"79927398713", false}, // too short
	}
	for _, tt := range tests {
		if luhnCheck(tt.number) != tt.valid {
			t.Errorf("luhnCheck(%q) = %v, want %v", tt.number, !tt.valid, tt.valid)
		}
	}
}

func TestFieldPathMatching(t *testing.T) {
	tests := []struct {
		pattern, path string
		expected      bool
	}{
		{"email", "email", true},
		{"**email**", "user_email", true},
		{"**email**", "contact.email_address", true},
		{"user.email", "user.email", true},
		{"user.*", "user.name", true},
		{"**.ssn", "person.ssn", true},
	}
	for _, tt := range tests {
		result := matchFieldPath(tt.pattern, tt.path)
		if result != tt.expected {
			t.Errorf("matchFieldPath(%q, %q) = %v, want %v", tt.pattern, tt.path, result, tt.expected)
		}
	}
}

func TestRegexMasking(t *testing.T) {
	cfg := config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "custom", Type: "regex", Pattern: `API_KEY_\w+`, Replace: "[REDACTED_KEY]"},
		},
	}
	e := NewEngine(cfg, testLogger())

	result := e.MaskValue("agent-1", "key: API_KEY_abc123")
	s, ok := result.(string)
	if !ok {
		t.Fatal("expected string result")
	}
	if s != "key: [REDACTED_KEY]" {
		t.Errorf("expected masked result, got %q", s)
	}
}

func TestNestedMapMasking(t *testing.T) {
	cfg := config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "email", Type: "email", FieldPath: "**email**"},
		},
	}
	e := NewEngine(cfg, testLogger())

	result := e.MaskValue("agent-1", map[string]any{
		"user": map[string]any{
			"email": "deep@nested.com",
			"name":  "Test",
		},
	})

	m := result.(map[string]any)
	user := m["user"].(map[string]any)
	if user["email"] == "deep@nested.com" {
		t.Error("expected nested email to be masked")
	}
	if user["name"] != "Test" {
		t.Error("expected name to remain unchanged")
	}
}
