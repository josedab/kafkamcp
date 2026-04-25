package onboarding

import (
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestDiscoverClusters(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	result := e.DiscoverClusters(context.Background())
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Duration == "" {
		t.Error("expected duration to be set")
	}
	if len(result.ScanMethods) == 0 {
		t.Error("expected scan methods")
	}
}

func TestDiscoverClusters_CancelledContext(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := e.DiscoverClusters(ctx)
	if result == nil {
		t.Fatal("expected non-nil result even with cancelled context")
	}
}

func TestSuggestRBAC_WithTopics(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []string{"orders", "payments", "orders-dlq"}
	rbac := e.SuggestRBAC(topics)
	if rbac == nil {
		t.Fatal("expected non-nil RBAC suggestion")
	}
	// Should have at least 3 base policies (sre, dev, readonly) + dlq handler
	if len(rbac.AgentPolicies) < 4 {
		t.Errorf("expected at least 4 policies (3 base + 1 dlq), got %d", len(rbac.AgentPolicies))
	}
	// Check DLQ-specific policy
	foundDLQ := false
	for _, p := range rbac.AgentPolicies {
		if p.AgentID == "dlq-handler" {
			foundDLQ = true
			if len(p.Permissions) == 0 {
				t.Error("dlq-handler should have permissions")
			}
		}
	}
	if !foundDLQ {
		t.Error("expected dlq-handler policy for orders-dlq topic")
	}
	if rbac.YAMLConfig == "" {
		t.Error("expected non-empty YAML config")
	}
}

func TestSuggestRBAC_EmptyTopics(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	rbac := e.SuggestRBAC(nil)
	if rbac == nil {
		t.Fatal("expected non-nil RBAC suggestion")
	}
	// Should have 3 base policies even with no topics
	if len(rbac.AgentPolicies) != 3 {
		t.Errorf("expected 3 base policies, got %d", len(rbac.AgentPolicies))
	}
}

func TestSuggestRBAC_DeadLetterNaming(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []string{"dead-letter-orders"}
	rbac := e.SuggestRBAC(topics)
	foundDLQ := false
	for _, p := range rbac.AgentPolicies {
		if p.AgentID == "dlq-handler" {
			foundDLQ = true
		}
	}
	if !foundDLQ {
		t.Error("expected dlq-handler for topic containing 'dead'")
	}
}

func TestGenerateQuickstart_ClusterFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	guide := e.GenerateQuickstart(true, 5)
	if guide == nil {
		t.Fatal("expected non-nil guide")
	}
	if guide.Total != 6 {
		t.Errorf("expected 6 steps, got %d", guide.Total)
	}
	if guide.Completed < 2 {
		t.Errorf("expected at least 2 completed steps (install + config), got %d", guide.Completed)
	}
	// Step 2 should be complete when cluster found
	if guide.Steps[1].Status != "complete" {
		t.Errorf("expected step 2 complete when cluster found, got %s", guide.Steps[1].Status)
	}
	if !strings.Contains(guide.ConfigYAML, "localhost:9092") {
		t.Error("expected localhost:9092 in config when cluster found")
	}
}

func TestGenerateQuickstart_NoCluster(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	guide := e.GenerateQuickstart(false, 0)
	if guide.Completed < 1 {
		t.Error("expected at least step 1 (install) to be complete")
	}
	if guide.Steps[1].Status != "pending" {
		t.Errorf("expected step 2 pending when no cluster, got %s", guide.Steps[1].Status)
	}
	if !strings.Contains(guide.ConfigYAML, "YOUR_KAFKA_BROKER") {
		t.Error("expected placeholder broker in config")
	}
}

func TestGenerateQuickstart_StepStructure(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	guide := e.GenerateQuickstart(true, 10)
	for i, step := range guide.Steps {
		if step.Number != i+1 {
			t.Errorf("step %d: expected number %d, got %d", i, i+1, step.Number)
		}
		if step.Title == "" {
			t.Errorf("step %d: expected non-empty title", i)
		}
		if step.Description == "" {
			t.Errorf("step %d: expected non-empty description", i)
		}
	}
}

func TestGenerateRBACYAML(t *testing.T) {
	t.Parallel()
	policies := []SuggestedPolicy{
		{AgentID: "test-agent", Role: "viewer", Topics: []string{"*"}, Permissions: []string{"read"}, RateLimit: 30},
	}
	yaml := generateRBACYAML(policies)
	if !strings.Contains(yaml, "test-agent") {
		t.Error("expected agent ID in YAML")
	}
	if !strings.Contains(yaml, "viewer") {
		t.Error("expected role in YAML")
	}
	if !strings.Contains(yaml, "requests_per_minute: 30") {
		t.Error("expected rate limit in YAML")
	}
}

func TestGenerateQuickstartConfig(t *testing.T) {
	t.Parallel()
	cfg := generateQuickstartConfig(true)
	if !strings.Contains(cfg, "localhost:9092") {
		t.Error("expected localhost in config when cluster found")
	}
	cfg = generateQuickstartConfig(false)
	if !strings.Contains(cfg, "YOUR_KAFKA_BROKER") {
		t.Error("expected placeholder when no cluster")
	}
}
