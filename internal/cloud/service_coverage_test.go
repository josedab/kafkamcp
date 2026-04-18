package cloud

import (
	"log/slog"
	"testing"
)

func TestListWorkspaces(t *testing.T) {
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
	cs := NewCloudService(slog.Default())

	cs.CreateWorkspace("org1", "a@test.com", "free")
	cs.CreateWorkspace("org2", "b@test.com", "pro")

	workspaces := cs.ListWorkspaces()
	if len(workspaces) != 2 {
		t.Errorf("expected 2 workspaces, got %d", len(workspaces))
	}
}

func TestUsagePct(t *testing.T) {
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

