package intentverify

import (
	"log/slog"
	"testing"
)

func TestClassifyTool(t *testing.T) {
	v := NewVerifier(DefaultVerifierConfig(), slog.Default())

	tests := []struct {
		tool     string
		wantRisk RiskLevel
	}{
		{"kafka_list_topics", RiskRead},
		{"kafka_produce", RiskWrite},
		{"kafka_delete_topic", RiskDestructive},
		{"kafka_acl_manage", RiskAdmin},
		{"unknown_tool", RiskRead}, // default
	}
	for _, tt := range tests {
		c := v.ClassifyTool(tt.tool)
		if c.Risk != tt.wantRisk {
			t.Errorf("ClassifyTool(%q).Risk = %q, want %q", tt.tool, c.Risk, tt.wantRisk)
		}
	}
}

func TestVerifyIntent_Aligned(t *testing.T) {
	v := NewVerifier(DefaultVerifierConfig(), slog.Default())

	// Read intent for read tool
	m := v.VerifyIntent("agent-1", "kafka_list_topics", "I want to list all topics")
	if !m.Aligned {
		t.Error("expected aligned intent for read/list")
	}
	if m.RequiresConfirm {
		t.Error("read operations should not require confirmation")
	}
}

func TestVerifyIntent_Misaligned(t *testing.T) {
	v := NewVerifier(DefaultVerifierConfig(), slog.Default())

	// Read intent for destructive tool
	m := v.VerifyIntent("agent-1", "kafka_delete_topic", "I want to read the topic")
	if m.Aligned {
		t.Error("expected misaligned intent for read intent on delete tool")
	}
	if !m.RequiresConfirm {
		t.Error("misaligned destructive should require confirmation")
	}
}

func TestVerifyIntent_DestructiveRequiresConfirm(t *testing.T) {
	v := NewVerifier(DefaultVerifierConfig(), slog.Default())

	m := v.VerifyIntent("agent-1", "kafka_delete_topic", "I want to delete old topics")
	if !m.RequiresConfirm {
		t.Error("destructive operations should require confirmation")
	}
}

func TestConfirmationFlow(t *testing.T) {
	v := NewVerifier(DefaultVerifierConfig(), slog.Default())

	// Request confirmation
	req := v.RequestConfirmation("agent-1", "kafka_delete_topic", "clean up old data")
	if req.Status != "pending" {
		t.Errorf("Status = %q, want pending", req.Status)
	}

	// Approve
	if err := v.ResolveConfirmation(req.ID, "admin", true); err != nil {
		t.Fatalf("ResolveConfirmation: %v", err)
	}

	req2, _ := v.GetConfirmation(req.ID)
	if req2.Status != "confirmed" {
		t.Errorf("Status = %q, want confirmed", req2.Status)
	}
}

func TestConfirmationDeny(t *testing.T) {
	v := NewVerifier(DefaultVerifierConfig(), slog.Default())

	req := v.RequestConfirmation("agent-1", "kafka_delete_topic", "clean up")
	_ = v.ResolveConfirmation(req.ID, "admin", false)

	req2, _ := v.GetConfirmation(req.ID)
	if req2.Status != "denied" {
		t.Errorf("Status = %q, want denied", req2.Status)
	}
}

func TestAgentBlockDestructive(t *testing.T) {
	v := NewVerifier(DefaultVerifierConfig(), slog.Default())
	v.SetAgentConfig(AgentRiskConfig{
		AgentID:          "restricted-agent",
		BlockDestructive: true,
		RequireConfirmAt: RiskWrite,
	})

	m := v.VerifyIntent("restricted-agent", "kafka_delete_topic", "delete topic")
	if !m.RequiresConfirm {
		t.Error("blocked agent should require confirmation for destructive")
	}
}

func TestListClassifications(t *testing.T) {
	v := NewVerifier(DefaultVerifierConfig(), slog.Default())

	all := v.ListClassifications("")
	if len(all) == 0 {
		t.Error("expected classifications")
	}

	destructive := v.ListClassifications(RiskDestructive)
	if len(destructive) == 0 {
		t.Error("expected destructive classifications")
	}
	for _, c := range destructive {
		if c.Risk != RiskDestructive {
			t.Errorf("expected destructive risk, got %q", c.Risk)
		}
	}
}

func TestListPendingConfirmations(t *testing.T) {
	v := NewVerifier(DefaultVerifierConfig(), slog.Default())

	v.RequestConfirmation("agent-1", "kafka_delete_topic", "clean up")
	v.RequestConfirmation("agent-2", "kafka_reset_offsets", "reset")

	pending := v.ListPendingConfirmations("")
	if len(pending) != 2 {
		t.Errorf("pending = %d, want 2", len(pending))
	}

	pending1 := v.ListPendingConfirmations("agent-1")
	if len(pending1) != 1 {
		t.Errorf("pending for agent-1 = %d, want 1", len(pending1))
	}
}

func TestRiskWeight(t *testing.T) {
	if RiskRead.Weight() >= RiskWrite.Weight() {
		t.Error("read should be less than write")
	}
	if RiskWrite.Weight() >= RiskDestructive.Weight() {
		t.Error("write should be less than destructive")
	}
	if RiskDestructive.Weight() >= RiskAdmin.Weight() {
		t.Error("destructive should be less than admin")
	}
}

func TestStats(t *testing.T) {
	v := NewVerifier(DefaultVerifierConfig(), slog.Default())
	stats := v.Stats()
	if stats["classified_tools"].(int) == 0 {
		t.Error("expected classified tools")
	}
}
