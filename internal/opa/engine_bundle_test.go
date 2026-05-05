package opa

import (
	"encoding/json"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// ValidatePolicy
// ---------------------------------------------------------------------------

func TestValidatePolicy_ValidFullPolicy(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	v := pe.ValidatePolicy(`package kafkamcp.authz
allow {
    input.action == "read"
}
deny {
    input.agent == "bad"
}`)

	if !v.Valid {
		t.Fatal("expected valid")
	}
	if !v.HasAllow {
		t.Fatal("expected HasAllow")
	}
	if !v.HasDeny {
		t.Fatal("expected HasDeny")
	}
	if !v.HasPackage {
		t.Fatal("expected HasPackage")
	}
	if v.RuleCount < 2 {
		t.Fatalf("expected at least 2 rules, got %d", v.RuleCount)
	}
}

func TestValidatePolicy_Empty(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	v := pe.ValidatePolicy("")
	if v.Valid {
		t.Fatal("expected invalid for empty policy")
	}
	if len(v.Errors) == 0 || v.Errors[0] != "empty policy" {
		t.Fatalf("expected 'empty policy' error, got %v", v.Errors)
	}
}

func TestValidatePolicy_NoRules(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	v := pe.ValidatePolicy(`package kafkamcp.authz
# just a comment
`)
	if v.Valid {
		t.Fatal("expected invalid when no allow/deny rules")
	}
	found := false
	for _, e := range v.Errors {
		if strings.Contains(e, "no allow or deny rules") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected 'no allow or deny rules' error, got %v", v.Errors)
	}
}

func TestValidatePolicy_UnbalancedBraces(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	v := pe.ValidatePolicy(`package kafkamcp.authz
allow {
    input.action == "read"
`)
	if v.Valid {
		t.Fatal("expected invalid for unbalanced braces")
	}
	found := false
	for _, e := range v.Errors {
		if strings.Contains(e, "unbalanced braces") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected 'unbalanced braces' error, got %v", v.Errors)
	}
}

func TestValidatePolicy_MissingPackage(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	v := pe.ValidatePolicy(`allow {
    input.action == "read"
}`)
	if !v.Valid {
		t.Fatal("expected valid even without package declaration")
	}
	found := false
	for _, w := range v.Warnings {
		if strings.Contains(w, "missing package") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected 'missing package' warning, got %v", v.Warnings)
	}
}

func TestValidatePolicy_NoInputReference(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	v := pe.ValidatePolicy(`package kafkamcp.authz
allow {
    1 == 1
}`)
	found := false
	for _, w := range v.Warnings {
		if strings.Contains(w, "does not reference input") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected 'does not reference input' warning, got %v", v.Warnings)
	}
}

func TestValidatePolicy_DefaultAllowTrue(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	v := pe.ValidatePolicy(`package kafkamcp.authz
default allow = true
`)
	found := false
	for _, w := range v.Warnings {
		if strings.Contains(w, "extremely permissive") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected permissive warning, got %v", v.Warnings)
	}
}

func TestValidatePolicy_CountsRules(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	v := pe.ValidatePolicy(`package kafkamcp.authz
allow {
    input.action == "read"
}
allow {
    input.action == "describe"
}
deny {
    input.agent == "anonymous"
}`)
	// "allow" appears 2 times, "deny" appears 1 time → at least 3
	if v.RuleCount < 3 {
		t.Fatalf("expected at least 3 rule references, got %d", v.RuleCount)
	}
}

// ---------------------------------------------------------------------------
// PredictImpact
// ---------------------------------------------------------------------------

func TestPredictImpact_AllowAll(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	// An allow block with no conditions matches everything in the simplified evaluator.
	impact := pe.PredictImpact("test-allow-all", `package kafkamcp.authz
allow {
}`)
	// A blanket allow should produce many WouldAllow and low risk
	if impact.WouldAllow == 0 {
		t.Fatal("expected non-zero WouldAllow for allow-all policy")
	}
	if impact.RiskLevel == "critical" || impact.RiskLevel == "high" {
		t.Fatalf("expected low risk for allow-all policy, got %s", impact.RiskLevel)
	}
}

func TestPredictImpact_DenyAll(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	// A deny block with no conditions matches everything in the simplified evaluator.
	impact := pe.PredictImpact("test-deny-all", `package kafkamcp.authz
deny {
}`)
	if impact.WouldDeny == 0 {
		t.Fatal("expected non-zero WouldDeny for deny-all policy")
	}
	if impact.RiskLevel != "critical" {
		t.Fatalf("expected critical risk for deny-all, got %s", impact.RiskLevel)
	}
}

func TestPredictImpact_Targeted(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	impact := pe.PredictImpact("test-targeted", `package kafkamcp.authz
deny {
    input.action == "delete"
    not input.agent == "admin"
}`)
	if impact.WouldDeny == 0 {
		t.Fatal("expected some denies for targeted policy")
	}
	if impact.RiskLevel == "low" {
		t.Fatal("expected higher than low risk for deny policy")
	}
}

func TestPredictImpact_NoMatch(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	impact := pe.PredictImpact("no-match", `package kafkamcp.authz
allow {
    input.action == "nonexistent_action"
}`)
	if impact.WouldAllow != 0 {
		t.Fatalf("expected zero WouldAllow, got %d", impact.WouldAllow)
	}
	if impact.WouldDeny != 0 {
		t.Fatalf("expected zero WouldDeny, got %d", impact.WouldDeny)
	}
	if impact.RiskLevel != "low" {
		t.Fatalf("expected low risk, got %s", impact.RiskLevel)
	}
}

// ---------------------------------------------------------------------------
// RegisterBundle / ListBundles
// ---------------------------------------------------------------------------

func TestRegisterBundle_Success(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	bundle, err := pe.RegisterBundle("b1", "Read Bundle", "1.0", []string{"default-allow-read"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if bundle.ID != "b1" || bundle.Name != "Read Bundle" || bundle.Version != "1.0" {
		t.Fatal("bundle fields mismatch")
	}
	if len(bundle.Policies) != 1 || bundle.Policies[0] != "default-allow-read" {
		t.Fatal("policy IDs mismatch")
	}
}

func TestRegisterBundle_MissingIDOrName(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	_, err := pe.RegisterBundle("", "name", "1.0", nil)
	if err == nil {
		t.Fatal("expected error for empty ID")
	}

	_, err = pe.RegisterBundle("b1", "", "1.0", nil)
	if err == nil {
		t.Fatal("expected error for empty name")
	}
}

func TestRegisterBundle_PolicyNotFound(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	_, err := pe.RegisterBundle("b1", "test", "1.0", []string{"does-not-exist"})
	if err == nil {
		t.Fatal("expected error for non-existent policy")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected 'not found' in error, got: %v", err)
	}
}

func TestRegisterBundle_Overwrite(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	_, err := pe.RegisterBundle("b1", "v1", "1.0", []string{"default-allow-read"})
	if err != nil {
		t.Fatal(err)
	}

	_, err = pe.RegisterBundle("b1", "v2", "2.0", []string{"default-deny-delete"})
	if err != nil {
		t.Fatal(err)
	}

	bundles := pe.ListBundles()
	if len(bundles) != 1 {
		t.Fatalf("expected 1 bundle after overwrite, got %d", len(bundles))
	}
	if bundles[0].Name != "v2" {
		t.Fatalf("expected overwritten name 'v2', got %q", bundles[0].Name)
	}
}

// ---------------------------------------------------------------------------
// ListBundles
// ---------------------------------------------------------------------------

func TestListBundles_Empty(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	bundles := pe.ListBundles()
	if len(bundles) != 0 {
		t.Fatalf("expected 0 bundles, got %d", len(bundles))
	}
}

func TestListBundles_AfterRegistering(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	_, _ = pe.RegisterBundle("b1", "B1", "1.0", []string{"default-allow-read"})
	_, _ = pe.RegisterBundle("b2", "B2", "1.0", []string{"default-deny-delete"})

	bundles := pe.ListBundles()
	if len(bundles) != 2 {
		t.Fatalf("expected 2 bundles, got %d", len(bundles))
	}
}

// ---------------------------------------------------------------------------
// EnableBundle / DisableBundle
// ---------------------------------------------------------------------------

func TestEnableBundle_Success(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	// Disable a policy manually, then enable via bundle.
	pe.policies["default-allow-read"].Enabled = false

	_, err := pe.RegisterBundle("b1", "B", "1.0", []string{"default-allow-read"})
	if err != nil {
		t.Fatal(err)
	}

	if err := pe.EnableBundle("b1"); err != nil {
		t.Fatal(err)
	}

	if !pe.policies["default-allow-read"].Enabled {
		t.Fatal("expected policy to be enabled after EnableBundle")
	}
}

func TestEnableBundle_NotFound(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	err := pe.EnableBundle("nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent bundle")
	}
}

func TestDisableBundle_Success(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	_, err := pe.RegisterBundle("b1", "B", "1.0", []string{"default-allow-read", "default-deny-delete"})
	if err != nil {
		t.Fatal(err)
	}

	if err := pe.DisableBundle("b1"); err != nil {
		t.Fatal(err)
	}

	if pe.policies["default-allow-read"].Enabled {
		t.Fatal("expected default-allow-read to be disabled")
	}
	if pe.policies["default-deny-delete"].Enabled {
		t.Fatal("expected default-deny-delete to be disabled")
	}
}

func TestDisableBundle_NotFound(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	err := pe.DisableBundle("nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent bundle")
	}
}

func TestDisableThenEnableBundle(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	_, err := pe.RegisterBundle("b1", "B", "1.0", []string{"default-allow-read"})
	if err != nil {
		t.Fatal(err)
	}

	if err := pe.DisableBundle("b1"); err != nil {
		t.Fatal(err)
	}
	if pe.policies["default-allow-read"].Enabled {
		t.Fatal("expected disabled after DisableBundle")
	}

	if err := pe.EnableBundle("b1"); err != nil {
		t.Fatal(err)
	}
	if !pe.policies["default-allow-read"].Enabled {
		t.Fatal("expected enabled after EnableBundle")
	}
}

// ---------------------------------------------------------------------------
// RegisterPolicy edge cases
// ---------------------------------------------------------------------------

func TestRegisterPolicy_EmptyID(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	err := pe.RegisterPolicy(Policy{ID: "", Rego: "allow { true }"})
	if err == nil {
		t.Fatal("expected error for empty ID")
	}
}

func TestRegisterPolicy_EmptyRego(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	err := pe.RegisterPolicy(Policy{ID: "p1", Rego: ""})
	if err == nil {
		t.Fatal("expected error for empty Rego")
	}
}

func TestRegisterPolicy_UpdateExisting(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	err := pe.RegisterPolicy(Policy{
		ID:   "custom-pol",
		Name: "Original",
		Rego: `allow { input.action == "read" }`,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = pe.RegisterPolicy(Policy{
		ID:   "custom-pol",
		Name: "Updated",
		Rego: `deny { input.action == "delete" }`,
	})
	if err != nil {
		t.Fatal(err)
	}

	p := pe.policies["custom-pol"]
	if p.Name != "Updated" {
		t.Fatalf("expected updated name, got %q", p.Name)
	}
	if !strings.Contains(p.Rego, "delete") {
		t.Fatal("expected updated Rego")
	}
}

// ---------------------------------------------------------------------------
// ExportPolicies
// ---------------------------------------------------------------------------

func TestExportPolicies(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	exported, err := pe.ExportPolicies()
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}
	if exported == "" {
		t.Fatal("expected non-empty export")
	}

	// Should be valid JSON.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(exported), &raw); err != nil {
		t.Fatalf("exported JSON invalid: %v", err)
	}
	if len(raw) < 3 {
		t.Fatalf("expected at least 3 policies in export, got %d", len(raw))
	}
}

// ---------------------------------------------------------------------------
// RecentDecisions
// ---------------------------------------------------------------------------

func TestRecentDecisions_Empty(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	decisions := pe.RecentDecisions(5)
	if len(decisions) != 0 {
		t.Fatalf("expected 0 decisions, got %d", len(decisions))
	}
}

func TestRecentDecisions_ReturnsRecent(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	// Record several decisions.
	for i := 0; i < 5; i++ {
		d := pe.Evaluate(PolicyInput{Agent: "a", Action: "read", Resource: "topic"})
		pe.RecordDecision(d)
	}

	decisions := pe.RecentDecisions(3)
	if len(decisions) != 3 {
		t.Fatalf("expected 3 decisions, got %d", len(decisions))
	}
}

func TestRecentDecisions_LimitExceedsTotal(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	d := pe.Evaluate(PolicyInput{Agent: "a", Action: "read", Resource: "topic"})
	pe.RecordDecision(d)

	decisions := pe.RecentDecisions(100)
	if len(decisions) != 1 {
		t.Fatalf("expected 1 decision, got %d", len(decisions))
	}
}

func TestRecentDecisions_ZeroLimit(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	d := pe.Evaluate(PolicyInput{Agent: "a", Action: "read", Resource: "topic"})
	pe.RecordDecision(d)

	decisions := pe.RecentDecisions(0)
	if len(decisions) != 1 {
		t.Fatalf("expected all decisions (1) for zero limit, got %d", len(decisions))
	}
}
