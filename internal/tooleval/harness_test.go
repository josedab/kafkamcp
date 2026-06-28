package tooleval

import (
	"testing"

	"github.com/josedab/kafkamcp/internal/server"
)

// TestFixtureCount ensures we always evaluate a representative set (>=25).
func TestFixtureCount(t *testing.T) {
	if n := len(Tasks()); n < 25 {
		t.Fatalf("expected at least 25 evaluation tasks, got %d", n)
	}
}

// TestRoutingSelectsExpectedTool validates deterministic routing for every task.
func TestRoutingSelectsExpectedTool(t *testing.T) {
	for _, task := range Tasks() {
		got := Route(task.Prompt)
		if got != task.ExpectedTool {
			t.Errorf("task %q: routed to %q, expected %q\n  prompt: %s", task.Name, got, task.ExpectedTool, task.Prompt)
		}
	}
}

// TestExpectedToolsExistAndUnique validates catalog integrity (no duplicate
// names, expected tools present).
func TestExpectedToolsExistAndUnique(t *testing.T) {
	seen := map[string]struct{}{}
	for _, name := range CatalogTools() {
		if _, dup := seen[name]; dup {
			t.Errorf("duplicate tool name in catalog: %q", name)
		}
		seen[name] = struct{}{}
	}
	for _, task := range Tasks() {
		if _, ok := seen[task.ExpectedTool]; !ok {
			t.Errorf("task %q references unknown tool %q", task.Name, task.ExpectedTool)
		}
	}
}

// TestWriteTasksNeverRouteIntoCoreRead is the key safety invariant: any task
// requiring a mutation must route to a tool that is NOT in the default
// read-only pack and IS a real write tool per the server's authoritative set.
func TestWriteTasksNeverRouteIntoCoreRead(t *testing.T) {
	coreRead := server.NewToolPackFilter([]string{"core-read"})
	for _, task := range Tasks() {
		if !task.Write {
			continue
		}
		// Must be classified as a write tool by the server's source of truth.
		if !server.IsWriteTool(task.ExpectedTool) {
			t.Errorf("task %q expects %q which is not in the server writeToolSet", task.Name, task.ExpectedTool)
		}
		// Must NOT be registrable in the read-only core.
		if coreRead.ShouldRegister(task.ExpectedTool, "advanced") {
			t.Errorf("write task %q routes to %q which is exposed by core-read (unsafe!)", task.Name, task.ExpectedTool)
		}
	}
}

// TestReadTasksNeverRouteToWriteTools ensures read-only tasks stay read-only.
func TestReadTasksNeverRouteToWriteTools(t *testing.T) {
	for _, task := range Tasks() {
		if task.Write {
			continue
		}
		if server.IsWriteTool(task.ExpectedTool) {
			t.Errorf("read task %q routes to write tool %q", task.Name, task.ExpectedTool)
		}
	}
}

// TestAdversarialPromptsStillRequireApproval verifies that benign-sounding
// adversarial prompts still resolve to write tools excluded from core-read (so
// they are subject to approval enforcement, not silently available).
func TestAdversarialPromptsStillRequireApproval(t *testing.T) {
	coreRead := server.NewToolPackFilter([]string{"core-read"})
	var adversarial int
	for _, task := range Tasks() {
		if !task.Adversarial {
			continue
		}
		adversarial++
		if !server.IsWriteTool(task.ExpectedTool) {
			t.Errorf("adversarial task %q must resolve to a write tool, got %q", task.Name, task.ExpectedTool)
		}
		if coreRead.ShouldRegister(task.ExpectedTool, "advanced") {
			t.Errorf("adversarial task %q smuggled unsafe tool %q into core-read", task.Name, task.ExpectedTool)
		}
	}
	if adversarial == 0 {
		t.Fatal("expected at least one adversarial task")
	}
}

// TestCoreReadToolsAreRoutable sanity-checks that the read-only tasks cover the
// core-read pack tools (each core-read tool the catalog knows about is the
// expected tool of at least one read task).
func TestReadTasksCoverCoreReadCatalog(t *testing.T) {
	expected := map[string]struct{}{}
	for _, task := range Tasks() {
		if !task.Write {
			expected[task.ExpectedTool] = struct{}{}
		}
	}
	// Every catalog read tool that is also in core-read should be exercised.
	coreRead := server.NewToolPackFilter([]string{"core-read"})
	for _, tool := range CatalogTools() {
		if IsCatalogWriteTool(tool) {
			continue
		}
		if !coreRead.ShouldRegister(tool, "advanced") {
			continue // governance read tool, not in core-read
		}
		if _, ok := expected[tool]; !ok {
			t.Errorf("core-read tool %q is not covered by any read task", tool)
		}
	}
}
