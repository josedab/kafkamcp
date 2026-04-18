package cloud

import (
	"fmt"
	"testing"
)

func TestCreateWorkspace(t *testing.T) {
	cs := NewCloudService(nil)

	ws, apiKey, err := cs.CreateWorkspace("My Team", "user@example.com", "free")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ws.Name != "My Team" {
		t.Errorf("expected 'My Team', got %s", ws.Name)
	}
	if apiKey == "" {
		t.Error("expected API key")
	}
	if ws.Plan != "free" {
		t.Errorf("expected free plan, got %s", ws.Plan)
	}
	if len(ws.Members) != 1 {
		t.Errorf("expected 1 member, got %d", len(ws.Members))
	}
}

func TestAuthenticateAPIKey(t *testing.T) {
	cs := NewCloudService(nil)

	ws, apiKey, _ := cs.CreateWorkspace("Test", "test@test.com", "free")

	wsID, err := cs.AuthenticateAPIKey(apiKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if wsID != ws.ID {
		t.Error("workspace ID mismatch")
	}

	_, err = cs.AuthenticateAPIKey("invalid")
	if err == nil {
		t.Error("expected error for invalid key")
	}
}

func TestClusterLimits(t *testing.T) {
	cs := NewCloudService(nil)

	ws, _, _ := cs.CreateWorkspace("Test", "test@test.com", "free")
	limits := GetPlanLimits("free")

	for i := 0; i < limits.MaxClusters; i++ {
		err := cs.AddCluster(ws.ID, ClusterConnection{Name: fmt.Sprintf("cluster-%d", i)})
		if err != nil {
			t.Fatalf("unexpected error adding cluster %d: %v", i, err)
		}
	}

	err := cs.AddCluster(ws.ID, ClusterConnection{Name: "one-too-many"})
	if err == nil {
		t.Error("expected error when exceeding cluster limit")
	}
}

func TestRecordUsage(t *testing.T) {
	cs := NewCloudService(nil)
	ws, _, _ := cs.CreateWorkspace("Test", "test@test.com", "free")

	err := cs.RecordUsage(ws.ID, 100, 50, 25)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	updated, _ := cs.GetWorkspace(ws.ID)
	if updated.Usage.ToolCalls != 100 {
		t.Errorf("expected 100 tool calls, got %d", updated.Usage.ToolCalls)
	}
}

func TestGetDashboardData(t *testing.T) {
	cs := NewCloudService(nil)
	ws, _, _ := cs.CreateWorkspace("Test", "test@test.com", "pro")

	data, err := cs.GetDashboardData(ws.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data["workspace"] == nil {
		t.Error("expected workspace in dashboard data")
	}
}

func TestPlanLimits(t *testing.T) {
	free := GetPlanLimits("free")
	if free.MaxClusters != 3 {
		t.Errorf("free plan should allow 3 clusters, got %d", free.MaxClusters)
	}

	enterprise := GetPlanLimits("enterprise")
	if enterprise.MaxClusters != -1 {
		t.Error("enterprise should have unlimited clusters")
	}
	if !enterprise.SSO {
		t.Error("enterprise should have SSO")
	}
}
