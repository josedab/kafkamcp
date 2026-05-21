package server

import (
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
)

func TestToolTier_DefaultsToAdvanced(t *testing.T) {
	tool := mcp.NewTool("kafka_dummy")
	if got := toolTier(&tool); got != TierAdvanced {
		t.Fatalf("expected default tier %q, got %q", TierAdvanced, got)
	}
}

func TestWithTier_SetsMetadata(t *testing.T) {
	tool := mcp.NewTool("kafka_dummy",
		withCategory("ops"),
		withTier(TierEssential),
	)
	if got := toolTier(&tool); got != TierEssential {
		t.Fatalf("expected tier %q, got %q", TierEssential, got)
	}
	if got := toolCategoryOf(&tool); got != "ops" {
		t.Fatalf("expected category %q, got %q", "ops", got)
	}
}

func TestNewToolPackFilter_DefaultsToEssential(t *testing.T) {
	f := NewToolPackFilter(nil)
	if f.AllAllowed() {
		t.Fatalf("expected filter with default packs to filter tools, got AllAllowed=true")
	}
	packs := f.Packs()
	if len(packs) != 1 || packs[0] != PackEssential {
		t.Fatalf("expected [essential], got %v", packs)
	}
	if f.AllowedCount() <= 0 {
		t.Fatalf("expected non-zero allowed count")
	}
}

func TestToolPackFilter_AllPack(t *testing.T) {
	f := NewToolPackFilter([]string{"all"})
	if !f.AllAllowed() {
		t.Fatalf("expected all-pack to disable filtering")
	}
	if !f.ShouldRegister("kafka_chaos_inject", TierExperimental) {
		t.Fatalf("all-pack should register experimental tools")
	}
	if !f.ShouldRegister("kafka_arbitrary", TierAdvanced) {
		t.Fatalf("all-pack should register advanced tools")
	}
	if f.AllowedCount() != -1 {
		t.Fatalf("expected AllowedCount() == -1 when filtering disabled, got %d", f.AllowedCount())
	}
}

func TestToolPackFilter_EssentialAlwaysWins(t *testing.T) {
	f := NewToolPackFilter([]string{"essential"})
	// Essential-tier tools register regardless of pack inclusion list.
	if !f.ShouldRegister("kafka_brand_new_essential", TierEssential) {
		t.Fatalf("essential tier should always register")
	}
	// Listed tools register.
	if !f.ShouldRegister("kafka_list_topics", TierAdvanced) {
		t.Fatalf("kafka_list_topics is in the essential pack and should register")
	}
	// Unlisted advanced tools are filtered.
	if f.ShouldRegister("kafka_obscure_advanced", TierAdvanced) {
		t.Fatalf("expected unlisted advanced tool to be filtered out")
	}
}

func TestToolPackFilter_ExperimentalRequiresOptIn(t *testing.T) {
	essentialOnly := NewToolPackFilter([]string{"essential"})
	if essentialOnly.ShouldRegister("kafka_chaos_inject", TierExperimental) {
		t.Fatalf("experimental tool must NOT be registered in essential-only pack")
	}

	withExp := NewToolPackFilter([]string{"essential", "experimental"})
	if !withExp.ShouldRegister("kafka_chaos_inject", TierExperimental) {
		t.Fatalf("experimental tool must register when experimental pack active")
	}
	// Even when experimental pack is active, unlisted advanced tools stay out.
	if withExp.ShouldRegister("kafka_obscure_advanced", TierAdvanced) {
		t.Fatalf("unlisted advanced tools must not appear when only experimental is added")
	}
}

func TestToolPackFilter_OpsPackIncludesEssentials(t *testing.T) {
	f := NewToolPackFilter([]string{"ops"})
	for _, name := range []string{"kafka_list_topics", "kafka_consume", "kafka_produce"} {
		if !f.ShouldRegister(name, TierAdvanced) {
			t.Fatalf("ops pack must include essential tool %q", name)
		}
	}
	// Ops-specific additions
	if !f.ShouldRegister("kafka_create_connector", TierAdvanced) {
		t.Fatalf("ops pack should include kafka_create_connector")
	}
}

func TestToolPackFilter_GovernancePackHasGovTools(t *testing.T) {
	f := NewToolPackFilter([]string{"governance"})
	for _, name := range []string{"kafka_compliance_scan", "kafka_audit_query", "kafka_encryption_key"} {
		if !f.ShouldRegister(name, TierAdvanced) {
			t.Fatalf("governance pack should include %q", name)
		}
	}
}

func TestToolPackFilter_NormalisationAndDeduplication(t *testing.T) {
	f := NewToolPackFilter([]string{" Essential ", "essential", "OPS"})
	packs := f.Packs()
	if len(packs) != 2 {
		t.Fatalf("expected 2 packs after normalization, got %v", packs)
	}
	if packs[0] != "essential" || packs[1] != "ops" {
		t.Fatalf("expected normalised order [essential, ops], got %v", packs)
	}
}

func TestToolPackFilter_EssentialCountUnderCeiling(t *testing.T) {
	f := NewToolPackFilter([]string{"essential"})
	// LLM tool routing degrades past 40–60 tools; aim for ≤50 in the default
	// pack. Hard-enforce 50 here so future contributions stay disciplined.
	if got := f.AllowedCount(); got > 50 {
		t.Fatalf("essential pack must stay ≤50 tools, got %d", got)
	}
}

func TestToolPackFilter_NilSafety(t *testing.T) {
	var f *ToolPackFilter
	if !f.AllAllowed() {
		t.Fatalf("nil filter should allow everything")
	}
	if !f.ShouldRegister("anything", TierAdvanced) {
		t.Fatalf("nil filter must register everything")
	}
	if f.AllowedCount() != -1 {
		t.Fatalf("nil filter AllowedCount should be -1")
	}
	if f.AllowedNames() != nil {
		t.Fatalf("nil filter AllowedNames should be nil")
	}
}

func TestToolPackFilter_AllowedNamesSortedAndStable(t *testing.T) {
	f := NewToolPackFilter([]string{"essential"})
	names := f.AllowedNames()
	for i := 1; i < len(names); i++ {
		if names[i-1] >= names[i] {
			t.Fatalf("expected sorted names, got %v", names)
		}
	}
}
