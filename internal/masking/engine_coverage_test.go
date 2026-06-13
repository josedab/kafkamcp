package masking

import (
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/josedab/kafkamcp/internal/config"
)

// Tests for ApplyAction, DetectPIIInValue, PolicyManager, and Luhn check.

func TestApplyAction_Redact(t *testing.T) {
	t.Parallel()
	e := &Engine{enabled: true}
	result := e.ApplyAction("redact", "secret", "")
	if result != defaultReplacement {
		t.Errorf("expected %q, got %q", defaultReplacement, result)
	}
}

func TestApplyAction_RedactCustom(t *testing.T) {
	t.Parallel()
	e := &Engine{enabled: true}
	result := e.ApplyAction("redact", "secret", "[MASKED]")
	if result != "[MASKED]" {
		t.Errorf("expected [MASKED], got %q", result)
	}
}

func TestApplyAction_Hash(t *testing.T) {
	t.Parallel()
	// No key configured -> fail closed (fully redact) rather than emit a weak token.
	e := &Engine{enabled: true}
	if got := e.ApplyAction("hash", "test@example.com", ""); got != defaultReplacement {
		t.Errorf("expected fail-closed redaction without key, got %q", got)
	}
}

func TestApplyAction_Hash_Keyed(t *testing.T) {
	t.Setenv(DefaultKeyEnv, "0123456789abcdef0123456789abcdef")
	e := NewEngine(config.MaskingConfig{Enabled: true}, testLogger())
	result := e.ApplyAction("hash", "test@example.com", "")
	if !strings.HasPrefix(result, "tok_") {
		t.Errorf("expected tok_ prefix, got %q", result)
	}
	// Should be deterministic
	result2 := e.ApplyAction("hash", "test@example.com", "")
	if result != result2 {
		t.Errorf("hash should be deterministic: %q != %q", result, result2)
	}
	// Domain separation: the same value under "tokenize" differs from "hash".
	if tok := e.ApplyAction("tokenize", "test@example.com", ""); tok == result {
		t.Error("expected domain separation between hash and tokenize actions")
	}
}

func TestApplyAction_MaskPartial(t *testing.T) {
	t.Parallel()
	e := &Engine{enabled: true}
	result := e.ApplyAction("mask_partial", "4111111111111111", "")
	if !strings.HasSuffix(result, "1111") {
		t.Errorf("expected last 4 digits visible, got %q", result)
	}
	if !strings.Contains(result, "*") {
		t.Errorf("expected masked characters, got %q", result)
	}
}

func TestApplyAction_Tokenize(t *testing.T) {
	t.Parallel()
	// No key configured -> fail closed.
	e := &Engine{enabled: true}
	if got := e.ApplyAction("tokenize", "value", ""); got != defaultReplacement {
		t.Errorf("expected fail-closed redaction without key, got %q", got)
	}
}

func TestApplyAction_Tokenize_Keyed(t *testing.T) {
	t.Setenv(DefaultKeyEnv, "0123456789abcdef0123456789abcdef")
	e := NewEngine(config.MaskingConfig{Enabled: true}, testLogger())
	result := e.ApplyAction("tokenize", "value", "")
	if !strings.HasPrefix(result, "tok_") {
		t.Errorf("expected tok_ prefix for tokenize, got %q", result)
	}
}

func TestApplyAction_UnknownDefault(t *testing.T) {
	t.Parallel()
	e := &Engine{enabled: true}
	result := e.ApplyAction("unknown", "value", "[REDACTED]")
	if result != "[REDACTED]" {
		t.Errorf("expected [REDACTED] for unknown action, got %q", result)
	}
}

func TestApplyAction_UnknownNoReplacement(t *testing.T) {
	t.Parallel()
	e := &Engine{enabled: true}
	result := e.ApplyAction("unknown", "value", "")
	if result != defaultReplacement {
		t.Errorf("expected default replacement, got %q", result)
	}
}

func TestDetectPIIInValue_String(t *testing.T) {
	t.Parallel()
	e := &Engine{enabled: true}
	detections := e.DetectPIIInValue("contact me at test@example.com or 123-45-6789")
	if len(detections) == 0 {
		t.Fatal("expected PII detections for email and SSN")
	}

	types := make(map[string]bool)
	for _, d := range detections {
		types[d.Type] = true
	}
	if !types["email"] {
		t.Error("expected email detection")
	}
	if !types["ssn"] {
		t.Error("expected SSN detection")
	}
}

func TestDetectPIIInValue_Map(t *testing.T) {
	t.Parallel()
	e := &Engine{enabled: true}
	data := map[string]any{
		"name":  "John Doe",
		"email": "john@example.com",
		"ssn":   "999-88-7777",
	}
	detections := e.DetectPIIInValue(data)
	if len(detections) == 0 {
		t.Fatal("expected PII detections in map values")
	}

	types := make(map[string]bool)
	for _, d := range detections {
		types[d.Type] = true
	}
	if !types["email"] {
		t.Error("expected email detection")
	}
}

func TestDetectPIIInValue_NonStringNonMap(t *testing.T) {
	t.Parallel()
	e := &Engine{enabled: true}
	detections := e.DetectPIIInValue(42)
	if detections != nil {
		t.Errorf("expected nil detections for int, got %v", detections)
	}
}

func TestDetectPIIInValue_NoMatches(t *testing.T) {
	t.Parallel()
	e := &Engine{enabled: true}
	detections := e.DetectPIIInValue("just a normal string with no PII")
	if len(detections) != 0 {
		t.Errorf("expected 0 detections, got %d", len(detections))
	}
}

func TestPolicyManager_NotNil(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	e := NewEngine(config.MaskingConfig{Enabled: true}, logger)
	pm := e.PolicyManager()
	if pm == nil {
		t.Fatal("expected non-nil policy manager")
	}
}

func TestLuhnCheck_ValidNumbers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		number string
		valid  bool
	}{
		{"Visa test card", "4111111111111111", true},
		{"Mastercard test", "5500000000000004", true},
		{"too short", "123456", false},
		{"too long", "12345678901234567890", false},
		{"non-numeric", "abcdefghijklm", false},
		{"with spaces", "4111 1111 1111 1111", true},
		{"with dashes", "4111-1111-1111-1111", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := luhnCheck(tt.number)
			if got != tt.valid {
				t.Errorf("luhnCheck(%q) = %v, want %v", tt.number, got, tt.valid)
			}
		})
	}
}

func TestDetectPII_EmailViaEngine(t *testing.T) {
	t.Parallel()
	detections := DetectPII("contact: user@example.com")
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

func TestDetectPII_CreditCardViaEngine(t *testing.T) {
	t.Parallel()
	detections := DetectPII("card: 4111111111111111")
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

func TestDetectPII_PhoneViaEngine(t *testing.T) {
	t.Parallel()
	detections := DetectPII("call: (555) 123-4567")
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

func TestDetectPII_SSNViaEngine(t *testing.T) {
	t.Parallel()
	detections := DetectPII("ssn: 123-45-6789")
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

func TestMaskPartial_Values(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		value   string
		visible int
		want    string
	}{
		{"normal", "4111111111111111", 4, "************1111"},
		{"short value", "abc", 4, "***"},
		{"exact length", "abcd", 4, "****"},
		{"empty", "", 4, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MaskPartial(tt.value, tt.visible)
			if got != tt.want {
				t.Errorf("MaskPartial(%q, %d) = %q, want %q", tt.value, tt.visible, got, tt.want)
			}
		})
	}
}

func TestHashValue_Deterministic(t *testing.T) {
	t.Setenv(DefaultKeyEnv, "0123456789abcdef0123456789abcdef")
	v1 := HashValue("test-value")
	v2 := HashValue("test-value")
	if v1 != v2 {
		t.Errorf("hash should be deterministic: %q != %q", v1, v2)
	}

	v3 := HashValue("different-value")
	if v1 == v3 {
		t.Error("different inputs should produce different hashes")
	}
}

func TestHashValue_FailsClosedWithoutKey(t *testing.T) {
	t.Setenv(DefaultKeyEnv, "")
	if got := HashValue("test-value"); got != defaultReplacement {
		t.Errorf("expected fail-closed redaction without key, got %q", got)
	}
}

func TestMaskValue_NestedJSON(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	e := NewEngine(config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "email", Type: "email", Replace: "[EMAIL]"},
		},
	}, logger)

	data := map[string]any{
		"user": map[string]any{
			"email": "john@example.com",
			"name":  "John",
		},
	}

	masked := e.MaskValue("agent-1", data)
	m, ok := masked.(map[string]any)
	if !ok {
		t.Fatal("expected map result")
	}
	user, ok := m["user"].(map[string]any)
	if !ok {
		t.Fatal("expected nested map")
	}
	if user["email"] == "john@example.com" {
		t.Error("email should have been masked")
	}
}

func TestMaskValue_DisabledEngine(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	e := NewEngine(config.MaskingConfig{Enabled: false}, logger)

	result := e.MaskValue("agent-1", "test@example.com")
	if result != "test@example.com" {
		t.Error("disabled engine should not mask values")
	}
}

func TestRuleCount(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	e := NewEngine(config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "email", Type: "email"},
			{Name: "ssn", Type: "ssn"},
		},
	}, logger)

	if e.RuleCount() != 2 {
		t.Errorf("expected 2 rules, got %d", e.RuleCount())
	}
}

func TestEnabled(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	e1 := NewEngine(config.MaskingConfig{Enabled: true}, logger)
	if !e1.Enabled() {
		t.Error("expected enabled")
	}

	e2 := NewEngine(config.MaskingConfig{Enabled: false}, logger)
	if e2.Enabled() {
		t.Error("expected disabled")
	}
}

func TestMaskValue_AgentSpecificRules(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	e := NewEngine(config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "email", Type: "email", Replace: "[EMAIL]", Agents: []string{"restricted-agent"}},
		},
	}, logger)

	// Targeted agent should see masked value
	masked := e.MaskValue("restricted-agent", "email: test@example.com")
	if strings.Contains(masked.(string), "test@example.com") {
		t.Error("restricted agent should not see email")
	}

	// Other agent should see unmasked value
	unmasked := e.MaskValue("other-agent", "email: test@example.com")
	if !strings.Contains(unmasked.(string), "test@example.com") {
		t.Error("other agent should see email unmasked")
	}
}
