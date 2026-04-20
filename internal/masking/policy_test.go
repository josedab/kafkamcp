package masking

import (
	"strings"
	"testing"
)

// --- PII Detection tests ---

func TestDetectPII_Email(t *testing.T) {
	t.Parallel()
	detections := DetectPII("Contact us at user@example.com for info")
	found := false
	for _, d := range detections {
		if d.Type == "email" {
			found = true
			if d.Severity != SensitivityHigh {
				t.Errorf("expected high severity for email, got %s", d.Severity)
			}
		}
	}
	if !found {
		t.Error("expected email detection")
	}
}

func TestDetectPII_SSN(t *testing.T) {
	t.Parallel()
	detections := DetectPII("SSN: 123-45-6789")
	found := false
	for _, d := range detections {
		if d.Type == "ssn" {
			found = true
			if d.Severity != SensitivityCritical {
				t.Errorf("expected critical severity for SSN, got %s", d.Severity)
			}
		}
	}
	if !found {
		t.Error("expected SSN detection")
	}
}

func TestDetectPII_CreditCard(t *testing.T) {
	t.Parallel()
	// Valid Visa number (passes Luhn)
	detections := DetectPII("Card: 4111 1111 1111 1111")
	found := false
	for _, d := range detections {
		if d.Type == "credit_card" {
			found = true
			if d.Severity != SensitivityCritical {
				t.Errorf("expected critical severity for credit card, got %s", d.Severity)
			}
		}
	}
	if !found {
		t.Error("expected credit card detection")
	}
}

func TestDetectPII_CreditCard_InvalidLuhn(t *testing.T) {
	t.Parallel()
	// Number that looks like credit card but fails Luhn
	detections := DetectPII("Card: 1234 5678 9012 3456")
	for _, d := range detections {
		if d.Type == "credit_card" {
			t.Error("expected no credit card detection for invalid Luhn number")
		}
	}
}

func TestDetectPII_Phone(t *testing.T) {
	t.Parallel()
	detections := DetectPII("Call me at (555) 123-4567")
	found := false
	for _, d := range detections {
		if d.Type == "phone" {
			found = true
			if d.Severity != SensitivityMedium {
				t.Errorf("expected medium severity for phone, got %s", d.Severity)
			}
		}
	}
	if !found {
		t.Error("expected phone detection")
	}
}

func TestDetectPII_MultipleTypes(t *testing.T) {
	t.Parallel()
	detections := DetectPII("user@test.com SSN: 123-45-6789")
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

func TestDetectPII_NoPII(t *testing.T) {
	t.Parallel()
	detections := DetectPII("Hello world, no PII here")
	if len(detections) != 0 {
		t.Errorf("expected no detections, got %d", len(detections))
	}
}

// --- MaskPartial tests ---

func TestMaskPartial(t *testing.T) {
	t.Parallel()
	tests := []struct {
		value   string
		visible int
		want    string
	}{
		{"4111111111111111", 4, "************1111"},
		{"hello", 2, "***lo"},
		{"ab", 4, "**"},    // shorter than visible chars
		{"", 4, ""},
		{"secret", 0, "******"},
	}

	for _, tt := range tests {
		got := MaskPartial(tt.value, tt.visible)
		if got != tt.want {
			t.Errorf("MaskPartial(%q, %d) = %q, want %q", tt.value, tt.visible, got, tt.want)
		}
	}
}

// --- HashValue tests ---

func TestHashValue(t *testing.T) {
	t.Parallel()
	h1 := HashValue("test")
	if !strings.HasPrefix(h1, "tok_") {
		t.Errorf("expected tok_ prefix, got %s", h1)
	}

	// Deterministic
	h2 := HashValue("test")
	if h1 != h2 {
		t.Error("expected deterministic hash")
	}

	// Different values produce different hashes
	h3 := HashValue("other")
	if h1 == h3 {
		t.Error("expected different hash for different input")
	}
}

// --- PolicyManager tests ---

func TestPolicyManager_LoadFromYAML(t *testing.T) {
	t.Parallel()
	pm := NewPolicyManager()
	yaml := `
version: "1.0"
rules:
  - name: email_rule
    type: pii
    field_paths: ["user.email"]
    pattern: "[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}"
    sensitivity: high
    action: redact
  - name: ssn_rule
    type: pii
    field_paths: ["user.ssn"]
    sensitivity: critical
    action: hash
agent_policies:
  - agent_id: agent-1
    min_sensitivity: medium
    allowed_fields: ["user.name"]
    denied_fields: ["user.ssn"]
`
	err := pm.LoadFromYAML([]byte(yaml))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	policy := pm.GetPolicy()
	if policy == nil || len(policy.Rules) != 2 {
		t.Fatalf("expected 2 rules, got %v", policy)
	}
}

func TestPolicyManager_LoadFromYAML_InvalidYAML(t *testing.T) {
	t.Parallel()
	pm := NewPolicyManager()
	err := pm.LoadFromYAML([]byte("{{invalid yaml"))
	if err == nil {
		t.Error("expected error for invalid YAML")
	}
}

func TestPolicyManager_LoadFromYAML_InvalidRegex(t *testing.T) {
	t.Parallel()
	pm := NewPolicyManager()
	yaml := `
version: "1.0"
rules:
  - name: bad_rule
    type: pii
    field_paths: ["x"]
    pattern: "[invalid regex"
    sensitivity: low
    action: redact
`
	err := pm.LoadFromYAML([]byte(yaml))
	if err == nil {
		t.Error("expected error for invalid regex pattern")
	}
}

func TestPolicyManager_Apply(t *testing.T) {
	t.Parallel()
	pm := NewPolicyManager()
	policy := &MaskingPolicy{
		Version: "1.0",
		Rules: []PolicyRule{
			{Name: "r1", FieldPaths: []string{"email"}, Sensitivity: SensitivityHigh, Action: "redact"},
		},
		AgentPolicies: []AgentPolicy{
			{AgentID: "a1", MinSensitivity: SensitivityMedium},
		},
	}

	err := pm.Apply(policy)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pm.GetPolicy().Version != "1.0" {
		t.Error("expected policy to be applied")
	}
}

func TestPolicyManager_GetAgentPolicy(t *testing.T) {
	t.Parallel()
	pm := NewPolicyManager()
	pm.Apply(&MaskingPolicy{
		AgentPolicies: []AgentPolicy{
			{AgentID: "agent-1", MinSensitivity: SensitivityHigh},
		},
	})

	ap := pm.GetAgentPolicy("agent-1")
	if ap == nil || ap.MinSensitivity != SensitivityHigh {
		t.Error("expected agent policy for agent-1")
	}

	if pm.GetAgentPolicy("unknown") != nil {
		t.Error("expected nil for unknown agent")
	}
}

func TestPolicyManager_ShouldMaskForAgent_UnknownAgent(t *testing.T) {
	t.Parallel()
	pm := NewPolicyManager()
	pm.Apply(&MaskingPolicy{})

	// Unknown agent → always mask
	if !pm.ShouldMaskForAgent("unknown", "any.field", SensitivityLow) {
		t.Error("expected masking for unknown agent")
	}
}

func TestPolicyManager_ShouldMaskForAgent_DeniedField(t *testing.T) {
	t.Parallel()
	pm := NewPolicyManager()
	pm.Apply(&MaskingPolicy{
		AgentPolicies: []AgentPolicy{
			{AgentID: "a1", MinSensitivity: SensitivityLow, DeniedFields: []string{"user.ssn"}},
		},
	})

	if !pm.ShouldMaskForAgent("a1", "user.ssn", SensitivityLow) {
		t.Error("expected masking for denied field")
	}
}

func TestPolicyManager_ShouldMaskForAgent_AllowedField(t *testing.T) {
	t.Parallel()
	pm := NewPolicyManager()
	pm.Apply(&MaskingPolicy{
		AgentPolicies: []AgentPolicy{
			{AgentID: "a1", MinSensitivity: SensitivityCritical, AllowedFields: []string{"user.name"}},
		},
	})

	if pm.ShouldMaskForAgent("a1", "user.name", SensitivityHigh) {
		t.Error("expected no masking for allowed field")
	}
}

func TestPolicyManager_ShouldMaskForAgent_SensitivityThreshold(t *testing.T) {
	t.Parallel()
	pm := NewPolicyManager()
	pm.Apply(&MaskingPolicy{
		AgentPolicies: []AgentPolicy{
			{AgentID: "a1", MinSensitivity: SensitivityHigh},
		},
	})

	// Medium < High threshold → should NOT mask
	if pm.ShouldMaskForAgent("a1", "field", SensitivityMedium) {
		t.Error("expected no masking below threshold")
	}

	// High >= High threshold → should mask
	if !pm.ShouldMaskForAgent("a1", "field", SensitivityHigh) {
		t.Error("expected masking at threshold")
	}

	// Critical > High threshold → should mask
	if !pm.ShouldMaskForAgent("a1", "field", SensitivityCritical) {
		t.Error("expected masking above threshold")
	}
}

// --- sensitivityOrd tests ---

func TestSensitivityOrd(t *testing.T) {
	t.Parallel()
	tests := []struct {
		level SensitivityLevel
		want  int
	}{
		{SensitivityLow, 1},
		{SensitivityMedium, 2},
		{SensitivityHigh, 3},
		{SensitivityCritical, 4},
		{"unknown", 2}, // default
	}

	for _, tt := range tests {
		got := sensitivityOrd(tt.level)
		if got != tt.want {
			t.Errorf("sensitivityOrd(%q) = %d, want %d", tt.level, got, tt.want)
		}
	}
}
