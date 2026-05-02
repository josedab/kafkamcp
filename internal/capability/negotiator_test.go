package capability

import (
	"log/slog"
	"testing"
)

func TestNegotiateViewer(t *testing.T) {
	t.Parallel()
	n := NewNegotiator(slog.Default())

	result := n.Negotiate("agent-1", RoleViewer, nil)
	if result.FilteredTools >= result.TotalTools {
		t.Error("viewer should have a subset of tools")
	}
	// Viewer should only get discovery + chat
	for _, tool := range result.AvailableTools {
		if tool.Category != CatDiscovery && tool.Category != CatChat {
			t.Errorf("viewer should not have %s tool %s", tool.Category, tool.Name)
		}
	}
}

func TestNegotiateAdmin(t *testing.T) {
	t.Parallel()
	n := NewNegotiator(slog.Default())

	result := n.Negotiate("agent-1", RoleAdmin, nil)
	if result.FilteredTools == 0 {
		t.Error("admin should have tools")
	}
	// Admin should have most/all categories
	if len(result.Categories) < 5 {
		t.Errorf("admin should have many categories, got %d", len(result.Categories))
	}
}

func TestContextAwarePruning(t *testing.T) {
	t.Parallel()
	n := NewNegotiator(slog.Default())

	// Viewer with lag context should get monitoring too
	result := n.Negotiate("agent-1", RoleViewer, []string{"consumer lag issues"})

	hasMonitoring := false
	for _, tool := range result.AvailableTools {
		if tool.Category == CatMonitoring {
			hasMonitoring = true
			break
		}
	}
	if !hasMonitoring {
		t.Error("expected monitoring tools added by context")
	}
}

func TestDiscoverTools(t *testing.T) {
	t.Parallel()
	n := NewNegotiator(slog.Default())

	// Negotiate as viewer first
	n.Negotiate("agent-1", RoleViewer, nil)

	// Discover analytics tools
	result := n.DiscoverTools("agent-1", "analytics")
	if result.Total == 0 {
		t.Error("expected analytics tools")
	}
	if result.NewTools == 0 {
		t.Error("expected new tools discovered")
	}

	// Now those tools should be allowed
	if !n.IsToolAllowed("agent-1", "kafka_anomaly_detect") {
		t.Error("discovered tool should be allowed")
	}
}

func TestIsToolAllowed(t *testing.T) {
	t.Parallel()
	n := NewNegotiator(slog.Default())

	// No profile = allowed
	if !n.IsToolAllowed("unknown", "kafka_list_topics") {
		t.Error("no profile should allow all")
	}

	// After negotiation, only allowed tools
	n.Negotiate("agent-1", RoleViewer, nil)
	if !n.IsToolAllowed("agent-1", "kafka_list_topics") {
		t.Error("viewer should access discovery tools")
	}
}

func TestListCategories(t *testing.T) {
	t.Parallel()
	n := NewNegotiator(slog.Default())
	cats := n.ListCategories()
	if len(cats) == 0 {
		t.Error("expected categories")
	}
}
