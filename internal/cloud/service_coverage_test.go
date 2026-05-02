package cloud

import (
	"log/slog"
	"testing"
)

func TestListWorkspaces(t *testing.T) {
	t.Parallel()
	cs := NewCloudService(slog.Default())

	// Empty initially
	workspaces := cs.ListWorkspaces()
	if len(workspaces) != 0 {
		t.Errorf("expected 0 workspaces, got %d", len(workspaces))
	}

	// Create a workspace
	ws, _, err := cs.CreateWorkspace("test-org", "admin@test.com", "free")
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	workspaces = cs.ListWorkspaces()
	if len(workspaces) != 1 {
		t.Fatalf("expected 1 workspace, got %d", len(workspaces))
	}
	if workspaces[0].ID != ws.ID {
		t.Errorf("expected workspace ID %s, got %s", ws.ID, workspaces[0].ID)
	}
}

func TestListWorkspacesMultiple(t *testing.T) {
	t.Parallel()
	cs := NewCloudService(slog.Default())

	cs.CreateWorkspace("org1", "a@test.com", "free")
	cs.CreateWorkspace("org2", "b@test.com", "pro")

	workspaces := cs.ListWorkspaces()
	if len(workspaces) != 2 {
		t.Errorf("expected 2 workspaces, got %d", len(workspaces))
	}
}

func TestUsagePct(t *testing.T) {
	t.Parallel()
	tests := []struct {
		used, limit int64
		want        float64
	}{
		{50, 100, 50.0},
		{0, 100, 0.0},
		{100, 100, 100.0},
		{0, 0, 0.0},
		{10, -1, 0.0},
	}
	for _, tt := range tests {
		got := usagePct(tt.used, tt.limit)
		if got != tt.want {
			t.Errorf("usagePct(%d, %d) = %f, want %f", tt.used, tt.limit, got, tt.want)
		}
	}
}

func TestUsagePct_Exceeds100(t *testing.T) {
	t.Parallel()
	got := usagePct(200, 100)
	if got != 200.0 {
		t.Errorf("expected 200.0 when usage exceeds limit, got %f", got)
	}
}

func TestGenerateID_Uniqueness(t *testing.T) {
	t.Parallel()
	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id, err := generateID("test")
		if err != nil {
			t.Fatalf("generateID failed: %v", err)
		}
		if seen[id] {
			t.Fatalf("duplicate ID generated: %s", id)
		}
		seen[id] = true
	}
}

func TestGenerateID_HasPrefix(t *testing.T) {
	t.Parallel()
	id, err := generateID("ws")
	if err != nil {
		t.Fatalf("generateID failed: %v", err)
	}
	if len(id) < 4 || id[:3] != "ws_" {
		t.Errorf("expected ID starting with 'ws_', got %q", id)
	}
}

func TestHashKey_Consistency(t *testing.T) {
	t.Parallel()
	key := "test-api-key-12345"
	h1 := hashKey(key)
	h2 := hashKey(key)
	if h1 != h2 {
		t.Error("hashKey should be deterministic")
	}
}

func TestHashKey_NonReversible(t *testing.T) {
	t.Parallel()
	key := "secret-key"
	h := hashKey(key)
	if h == key {
		t.Error("hash should differ from input")
	}
	if len(h) != 64 { // SHA-256 hex = 64 chars
		t.Errorf("expected 64 char hex hash, got %d chars", len(h))
	}
}

func TestHashKey_DifferentInputs(t *testing.T) {
	t.Parallel()
	h1 := hashKey("key-a")
	h2 := hashKey("key-b")
	if h1 == h2 {
		t.Error("different keys should produce different hashes")
	}
}

func TestGetDashboardData_NoWorkspace(t *testing.T) {
	t.Parallel()
	cs := NewCloudService(slog.Default())
	_, err := cs.GetDashboardData("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent workspace")
	}
}

func TestGetDashboardData_WithClustersAndMembers(t *testing.T) {
	t.Parallel()
	cs := NewCloudService(slog.Default())
	ws, _, _ := cs.CreateWorkspace("Team", "admin@test.com", "pro")
	cs.AddCluster(ws.ID, ClusterConnection{Name: "prod"})
	cs.AddCluster(ws.ID, ClusterConnection{Name: "staging"})
	cs.AddMember(ws.ID, "dev@test.com", "member")

	data, err := cs.GetDashboardData(ws.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data["clusters_count"] != 2 {
		t.Errorf("expected 2 clusters, got %v", data["clusters_count"])
	}
	if data["members_count"] != 2 { // owner + 1 member
		t.Errorf("expected 2 members, got %v", data["members_count"])
	}
	usage := data["usage"].(map[string]any)
	if usage["tool_calls"] != int64(0) {
		t.Errorf("expected 0 tool calls, got %v", usage["tool_calls"])
	}
}

func TestGetDashboardData_DeletedWorkspace(t *testing.T) {
	t.Parallel()
	cs := NewCloudService(slog.Default())
	ws, _, _ := cs.CreateWorkspace("Team", "admin@test.com", "free")
	cs.DeleteWorkspace(ws.ID)

	// Dashboard should still work for deleted workspaces (they're still in map)
	data, err := cs.GetDashboardData(ws.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wsData := data["workspace"].(*Workspace)
	if wsData.Status != "deleted" {
		t.Errorf("expected deleted status, got %s", wsData.Status)
	}
}

func TestGetDashboardData_UsagePercentage(t *testing.T) {
	t.Parallel()
	cs := NewCloudService(slog.Default())
	ws, _, _ := cs.CreateWorkspace("Team", "admin@test.com", "free")
	cs.RecordUsage(ws.ID, 25000, 0, 0)

	data, err := cs.GetDashboardData(ws.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	usage := data["usage"].(map[string]any)
	pct := usage["tool_calls_pct"].(float64)
	if pct != 50.0 { // 25000/50000 * 100
		t.Errorf("expected 50%% usage, got %f", pct)
	}
}

