package contracts

import (
	"testing"
)

// helper to create an engine with a registered contract.
func setupEnforcementEngine(t *testing.T) *Engine {
	t.Helper()
	e := NewEngine()
	_ = e.RegisterContract(Contract{
		Name:    "test-contract",
		Version: "1.0",
		Topic:   "test.topic",
		Owner:   "team",
		Quality: []QualityRule{
			{Field: "id", Rule: "not_null", Severity: "error"},
			{Field: "status", Rule: "enum", Value: "active,inactive", Severity: "warning"},
		},
	})
	return e
}

func TestSetEnforcementMode(t *testing.T) {
	t.Run("set mode on existing contract", func(t *testing.T) {
		e := setupEnforcementEngine(t)
		err := e.SetEnforcementMode("test.topic", ModeReject)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		c, _ := e.GetContract("test.topic")
		_ = c // contract still valid
		mode := e.getEnforcementMode("test.topic")
		if mode != ModeReject {
			t.Errorf("expected mode reject, got %q", mode)
		}
	})

	t.Run("overwrite existing mode", func(t *testing.T) {
		e := setupEnforcementEngine(t)
		_ = e.SetEnforcementMode("test.topic", ModeWarn)
		_ = e.SetEnforcementMode("test.topic", ModeQuarantine)
		mode := e.getEnforcementMode("test.topic")
		if mode != ModeQuarantine {
			t.Errorf("expected mode quarantine, got %q", mode)
		}
	})

	t.Run("contract not found", func(t *testing.T) {
		e := NewEngine()
		err := e.SetEnforcementMode("nonexistent", ModeWarn)
		if err == nil {
			t.Error("expected error for missing topic")
		}
	})
}

func TestEnforceContract(t *testing.T) {
	t.Run("warn mode allows invalid message", func(t *testing.T) {
		e := setupEnforcementEngine(t)
		_ = e.SetEnforcementMode("test.topic", ModeWarn)

		r := e.EnforceContract("test.topic", map[string]any{"status": "unknown"})
		if r == nil {
			t.Fatal("expected non-nil result")
		}
		if !r.Allowed {
			t.Error("expected Allowed=true in warn mode")
		}
		if r.Action != "warned" {
			t.Errorf("expected action 'warned', got %q", r.Action)
		}
		if r.Mode != ModeWarn {
			t.Errorf("expected mode warn, got %q", r.Mode)
		}
		if len(r.Violations) == 0 {
			t.Error("expected violations for invalid message")
		}
	})

	t.Run("reject mode rejects invalid message", func(t *testing.T) {
		e := setupEnforcementEngine(t)
		_ = e.SetEnforcementMode("test.topic", ModeReject)

		r := e.EnforceContract("test.topic", map[string]any{"status": "unknown"})
		if r == nil {
			t.Fatal("expected non-nil result")
		}
		if r.Allowed {
			t.Error("expected Allowed=false in reject mode")
		}
		if r.Action != "rejected" {
			t.Errorf("expected action 'rejected', got %q", r.Action)
		}
	})

	t.Run("quarantine mode quarantines invalid message", func(t *testing.T) {
		e := setupEnforcementEngine(t)
		_ = e.SetEnforcementMode("test.topic", ModeQuarantine)

		r := e.EnforceContract("test.topic", map[string]any{"status": "unknown"})
		if r == nil {
			t.Fatal("expected non-nil result")
		}
		if r.Allowed {
			t.Error("expected Allowed=false in quarantine mode")
		}
		if r.Action != "quarantined" {
			t.Errorf("expected action 'quarantined', got %q", r.Action)
		}
	})

	t.Run("valid message passes", func(t *testing.T) {
		e := setupEnforcementEngine(t)
		_ = e.SetEnforcementMode("test.topic", ModeReject)

		r := e.EnforceContract("test.topic", map[string]any{"id": "123", "status": "active"})
		if r == nil {
			t.Fatal("expected non-nil result")
		}
		if !r.Allowed {
			t.Error("expected Allowed=true for valid message")
		}
		if r.Action != "passed" {
			t.Errorf("expected action 'passed', got %q", r.Action)
		}
		if len(r.Violations) != 0 {
			t.Errorf("expected 0 violations, got %d", len(r.Violations))
		}
	})

	t.Run("no contract returns nil", func(t *testing.T) {
		e := NewEngine()
		r := e.EnforceContract("nonexistent", map[string]any{"key": "val"})
		if r != nil {
			t.Error("expected nil result for unknown topic")
		}
	})
}

func TestGetViolationSummary(t *testing.T) {
	t.Run("with violations", func(t *testing.T) {
		e := setupEnforcementEngine(t)
		// Generate violations
		e.ValidateMessage("test.topic", map[string]any{"status": "unknown"})  // id missing + bad enum
		e.ValidateMessage("test.topic", map[string]any{"id": "1", "status": "active"}) // valid

		summary, err := e.GetViolationSummary("test.topic")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if summary.Topic != "test.topic" {
			t.Errorf("expected topic 'test.topic', got %q", summary.Topic)
		}
		if summary.ContractName != "test-contract" {
			t.Errorf("expected contract name 'test-contract', got %q", summary.ContractName)
		}
		if summary.TotalViolations != 1 {
			t.Errorf("expected 1 failed check, got %d", summary.TotalViolations)
		}
		if summary.ComplianceRate == 0 {
			t.Error("expected non-zero compliance rate")
		}
		if len(summary.ViolationsByRule) == 0 {
			t.Error("expected non-empty ViolationsByRule")
		}
	})

	t.Run("no contract", func(t *testing.T) {
		e := NewEngine()
		_, err := e.GetViolationSummary("nonexistent")
		if err == nil {
			t.Error("expected error for missing topic")
		}
	})

	t.Run("no violations", func(t *testing.T) {
		e := setupEnforcementEngine(t)
		// No validations run yet — no stats entry
		summary, err := e.GetViolationSummary("test.topic")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if summary.TotalViolations != 0 {
			t.Errorf("expected 0 violations, got %d", summary.TotalViolations)
		}
		if summary.ViolationsByRule == nil {
			t.Error("expected non-nil ViolationsByRule map")
		}
	})
}

func TestInferContract(t *testing.T) {
	t.Run("infer from samples with not_null and enum rules", func(t *testing.T) {
		samples := make([]map[string]any, 20)
		for i := 0; i < 20; i++ {
			status := "active"
			if i%3 == 0 {
				status = "inactive"
			}
			samples[i] = map[string]any{
				"id":     "id-" + string(rune('a'+i%26)),
				"status": status,
				"name":   "user-" + string(rune('a'+i%26)),
			}
		}

		e := NewEngine()
		result := e.InferContract("my.topic", samples)
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result.Topic != "my.topic" {
			t.Errorf("expected topic 'my.topic', got %q", result.Topic)
		}
		if result.Name != "my.topic-contract" {
			t.Errorf("expected name 'my.topic-contract', got %q", result.Name)
		}
		if result.Version != "1.0.0-inferred" {
			t.Errorf("expected version '1.0.0-inferred', got %q", result.Version)
		}
		if result.SampleSize != 20 {
			t.Errorf("expected sample size 20, got %d", result.SampleSize)
		}
		if result.Confidence != 0.7 {
			t.Errorf("expected confidence 0.7 for 20 samples, got %f", result.Confidence)
		}
		if len(result.Quality) == 0 {
			t.Error("expected inferred quality rules")
		}

		// All fields present in all samples → not_null rules expected
		hasNotNull := false
		hasEnum := false
		for _, rule := range result.Quality {
			if rule.Rule == "not_null" {
				hasNotNull = true
			}
			if rule.Rule == "enum" {
				hasEnum = true
			}
		}
		if !hasNotNull {
			t.Error("expected at least one not_null rule")
		}
		if !hasEnum {
			t.Error("expected at least one enum rule for status field")
		}
	})

	t.Run("empty samples", func(t *testing.T) {
		e := NewEngine()
		result := e.InferContract("my.topic", nil)
		if result != nil {
			t.Error("expected nil for empty samples")
		}

		result = e.InferContract("my.topic", []map[string]any{})
		if result != nil {
			t.Error("expected nil for empty slice")
		}
	})

	t.Run("confidence levels", func(t *testing.T) {
		e := NewEngine()

		// < 10 samples → 0.5
		small := make([]map[string]any, 5)
		for i := range small {
			small[i] = map[string]any{"a": "v"}
		}
		r := e.InferContract("t1", small)
		if r.Confidence != 0.5 {
			t.Errorf("expected confidence 0.5 for 5 samples, got %f", r.Confidence)
		}

		// 10-99 samples → 0.7
		medium := make([]map[string]any, 50)
		for i := range medium {
			medium[i] = map[string]any{"a": "v"}
		}
		r = e.InferContract("t2", medium)
		if r.Confidence != 0.7 {
			t.Errorf("expected confidence 0.7 for 50 samples, got %f", r.Confidence)
		}

		// >= 100 samples → 0.9
		large := make([]map[string]any, 100)
		for i := range large {
			large[i] = map[string]any{"a": "v"}
		}
		r = e.InferContract("t3", large)
		if r.Confidence != 0.9 {
			t.Errorf("expected confidence 0.9 for 100 samples, got %f", r.Confidence)
		}
	})
}
