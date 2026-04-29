package cloud

import (
	"log/slog"
	"strings"
	"testing"
)

func TestUpgradePlan_ValidUpgrade(t *testing.T) {
	cs := NewCloudService(slog.Default())
	ws, _, _ := cs.CreateWorkspace("Test", "test@test.com", "free")

	err := cs.UpgradePlan(ws.ID, "pro")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	updated, _ := cs.GetWorkspace(ws.ID)
	if updated.Plan != "pro" {
		t.Errorf("expected plan 'pro', got %s", updated.Plan)
	}
}

func TestUpgradePlan_AllValidPlans(t *testing.T) {
	plans := []string{"free", "pro", "enterprise"}
	for _, plan := range plans {
		cs := NewCloudService(slog.Default())
		ws, _, _ := cs.CreateWorkspace("Test", "test@test.com", "free")

		err := cs.UpgradePlan(ws.ID, plan)
		if err != nil {
			t.Fatalf("UpgradePlan(%q) failed: %v", plan, err)
		}
		updated, _ := cs.GetWorkspace(ws.ID)
		if updated.Plan != plan {
			t.Errorf("expected plan %q, got %s", plan, updated.Plan)
		}
	}
}

func TestUpgradePlan_NonExistentWorkspace(t *testing.T) {
	cs := NewCloudService(slog.Default())
	err := cs.UpgradePlan("nonexistent", "pro")
	if err == nil {
		t.Fatal("expected error for nonexistent workspace")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' error, got: %v", err)
	}
}

func TestUpgradePlan_InvalidPlan(t *testing.T) {
	cs := NewCloudService(slog.Default())
	ws, _, _ := cs.CreateWorkspace("Test", "test@test.com", "free")

	err := cs.UpgradePlan(ws.ID, "platinum")
	if err == nil {
		t.Fatal("expected error for invalid plan")
	}
	if !strings.Contains(err.Error(), "invalid plan") {
		t.Errorf("expected 'invalid plan' error, got: %v", err)
	}
}

func TestUpgradePlan_EmptyPlan(t *testing.T) {
	cs := NewCloudService(slog.Default())
	ws, _, _ := cs.CreateWorkspace("Test", "test@test.com", "free")

	err := cs.UpgradePlan(ws.ID, "")
	if err == nil {
		t.Fatal("expected error for empty plan")
	}
}

func TestUpgradePlan_DowngradePath(t *testing.T) {
	cs := NewCloudService(slog.Default())
	ws, _, _ := cs.CreateWorkspace("Test", "test@test.com", "enterprise")

	// Downgrade enterprise -> free
	err := cs.UpgradePlan(ws.ID, "free")
	if err != nil {
		t.Fatalf("downgrade should succeed: %v", err)
	}

	updated, _ := cs.GetWorkspace(ws.ID)
	if updated.Plan != "free" {
		t.Errorf("expected plan 'free', got %s", updated.Plan)
	}
}

func TestUpgradePlan_SamePlan(t *testing.T) {
	cs := NewCloudService(slog.Default())
	ws, _, _ := cs.CreateWorkspace("Test", "test@test.com", "pro")

	err := cs.UpgradePlan(ws.ID, "pro")
	if err != nil {
		t.Fatalf("same plan should succeed: %v", err)
	}
}
