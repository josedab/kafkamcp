package federation

import (
	"log/slog"
	"os"
	"sync"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestNewCoordinator(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	if c == nil {
		t.Fatal("expected non-nil coordinator")
	}
	if c.localID != "local-1" {
		t.Errorf("expected localID local-1, got %s", c.localID)
	}
	if len(c.instances) != 0 {
		t.Errorf("expected empty instances map, got %d", len(c.instances))
	}
}

func TestJoinSuccess(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())

	inst, err := c.Join(Instance{
		ID:       "inst-1",
		Name:     "node-1",
		Endpoint: "http://localhost:8080",
	})
	if err != nil {
		t.Fatalf("Join failed: %v", err)
	}
	if inst.Status != StatusActive {
		t.Errorf("expected active status, got %s", inst.Status)
	}
	if inst.JoinedAt == "" {
		t.Error("expected JoinedAt to be set")
	}
	if inst.LastHeartbeat == "" {
		t.Error("expected LastHeartbeat to be set")
	}
}

func TestJoinEmptyID(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	_, err := c.Join(Instance{Endpoint: "http://localhost:8080"})
	if err == nil {
		t.Fatal("expected error for empty ID")
	}
}

func TestJoinEmptyEndpoint(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	_, err := c.Join(Instance{ID: "inst-1"})
	if err == nil {
		t.Fatal("expected error for empty endpoint")
	}
}

func TestJoinDuplicate(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	c.Join(Instance{ID: "inst-1", Name: "n1", Endpoint: "http://a:8080"})
	// Joining with same ID overwrites (no error per implementation)
	inst, err := c.Join(Instance{ID: "inst-1", Name: "n2", Endpoint: "http://b:8080"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if inst.Name != "n2" {
		t.Errorf("expected overwritten name n2, got %s", inst.Name)
	}
}

func TestLeaveSuccess(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	c.Join(Instance{ID: "inst-1", Endpoint: "http://a:8080"})

	err := c.Leave("inst-1")
	if err != nil {
		t.Fatalf("Leave failed: %v", err)
	}

	instances := c.ListInstances()
	if len(instances) != 0 {
		t.Errorf("expected 0 instances after leave, got %d", len(instances))
	}
}

func TestLeaveNotFound(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	err := c.Leave("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent instance")
	}
}

func TestHeartbeatSuccess(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	c.Join(Instance{ID: "inst-1", Endpoint: "http://a:8080"})

	err := c.Heartbeat("inst-1")
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}
}

func TestHeartbeatNotFound(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	err := c.Heartbeat("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent instance")
	}
}

func TestHeartbeatAfterLeave(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	c.Join(Instance{ID: "inst-1", Endpoint: "http://a:8080"})
	c.Leave("inst-1")

	err := c.Heartbeat("inst-1")
	if err == nil {
		t.Fatal("expected error for heartbeat after leave")
	}
}

func TestQueryAllActive(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	c.Join(Instance{ID: "inst-1", Name: "n1", Endpoint: "http://a:8080"})
	c.Join(Instance{ID: "inst-2", Name: "n2", Endpoint: "http://b:8080"})

	q := c.Query("list_topics", map[string]any{"cluster": "default"}, nil)
	if q.Status != "completed" {
		t.Errorf("expected completed, got %s", q.Status)
	}
	if len(q.Results) != 2 {
		t.Errorf("expected 2 results, got %d", len(q.Results))
	}
	if q.Tool != "list_topics" {
		t.Errorf("expected tool list_topics, got %s", q.Tool)
	}
}

func TestQueryTargeted(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	c.Join(Instance{ID: "inst-1", Name: "n1", Endpoint: "http://a:8080"})
	c.Join(Instance{ID: "inst-2", Name: "n2", Endpoint: "http://b:8080"})

	q := c.Query("list_topics", nil, []string{"inst-1"})
	if len(q.Results) != 1 {
		t.Errorf("expected 1 targeted result, got %d", len(q.Results))
	}
	if q.Results[0].InstanceID != "inst-1" {
		t.Errorf("expected inst-1, got %s", q.Results[0].InstanceID)
	}
}

func TestQueryNoInstances(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	q := c.Query("list_topics", nil, nil)
	if len(q.Results) != 0 {
		t.Errorf("expected 0 results, got %d", len(q.Results))
	}
}

func TestQuerySkipsInactive(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	c.Join(Instance{ID: "inst-1", Endpoint: "http://a:8080"})
	c.Join(Instance{ID: "inst-2", Endpoint: "http://b:8080"})

	// Manually set one to degraded
	c.mu.Lock()
	c.instances["inst-2"].Status = StatusDegraded
	c.mu.Unlock()

	q := c.Query("list_topics", nil, nil)
	if len(q.Results) != 1 {
		t.Errorf("expected 1 active result, got %d", len(q.Results))
	}
}

func TestGetStatus(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	c.Join(Instance{ID: "inst-1", Endpoint: "http://a:8080"})
	c.Join(Instance{ID: "inst-2", Endpoint: "http://b:8080"})

	status := c.GetStatus()
	if status["local_id"] != "local-1" {
		t.Errorf("expected local_id local-1, got %v", status["local_id"])
	}
	if status["total_instances"].(int) != 2 {
		t.Errorf("expected 2 total instances, got %v", status["total_instances"])
	}
	if status["active"].(int) != 2 {
		t.Errorf("expected 2 active, got %v", status["active"])
	}
}

func TestGetStatusEmpty(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	status := c.GetStatus()
	if status["total_instances"].(int) != 0 {
		t.Errorf("expected 0 instances, got %v", status["total_instances"])
	}
}

func TestListInstances(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	c.Join(Instance{ID: "inst-1", Endpoint: "http://a:8080"})

	instances := c.ListInstances()
	if len(instances) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(instances))
	}
	if instances[0].ID != "inst-1" {
		t.Errorf("expected inst-1, got %s", instances[0].ID)
	}
}

func TestListInstancesEmpty(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	instances := c.ListInstances()
	if instances != nil && len(instances) != 0 {
		t.Errorf("expected nil or empty, got %d", len(instances))
	}
}

func TestAddPolicy(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())

	p := c.AddPolicy(Policy{
		ID:   "pol-1",
		Name: "rate-limit",
		Type: "rate_limit",
	})
	if p.CreatedAt == "" {
		t.Error("expected CreatedAt to be set")
	}
	if p.Version != 1 {
		t.Errorf("expected version 1, got %d", p.Version)
	}
}

func TestAddPolicyWithVersion(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	p := c.AddPolicy(Policy{ID: "pol-1", Version: 5})
	if p.Version != 5 {
		t.Errorf("expected version 5, got %d", p.Version)
	}
}

func TestListPolicies(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	c.AddPolicy(Policy{ID: "pol-1", Name: "p1"})
	c.AddPolicy(Policy{ID: "pol-2", Name: "p2"})

	policies := c.ListPolicies()
	if len(policies) != 2 {
		t.Errorf("expected 2 policies, got %d", len(policies))
	}
}

func TestListPoliciesEmpty(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	policies := c.ListPolicies()
	if policies != nil && len(policies) != 0 {
		t.Errorf("expected nil or empty, got %d", len(policies))
	}
}

func TestGetAuditLog(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	c.Join(Instance{ID: "inst-1", Endpoint: "http://a:8080"})
	c.Leave("inst-1")

	log := c.GetAuditLog(10)
	if len(log) != 2 {
		t.Errorf("expected 2 audit entries, got %d", len(log))
	}
	if log[0].Action != "instance_joined" {
		t.Errorf("expected instance_joined, got %s", log[0].Action)
	}
	if log[1].Action != "instance_left" {
		t.Errorf("expected instance_left, got %s", log[1].Action)
	}
}

func TestGetAuditLogLimit(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	for i := 0; i < 5; i++ {
		c.Join(Instance{ID: "inst", Endpoint: "http://a:8080"})
	}

	log := c.GetAuditLog(2)
	if len(log) != 2 {
		t.Errorf("expected 2 audit entries, got %d", len(log))
	}
}

func TestGetAuditLogZeroLimit(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	c.Join(Instance{ID: "inst-1", Endpoint: "http://a:8080"})

	log := c.GetAuditLog(0)
	if len(log) != 1 {
		t.Errorf("expected all entries with limit 0, got %d", len(log))
	}
}

func TestGetAuditLogNegativeLimit(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	c.Join(Instance{ID: "inst-1", Endpoint: "http://a:8080"})

	log := c.GetAuditLog(-1)
	if len(log) != 1 {
		t.Errorf("expected all entries with negative limit, got %d", len(log))
	}
}

func TestContains(t *testing.T) {
	t.Parallel()
	slice := []string{"a", "b", "c"}
	if !contains(slice, "b") {
		t.Error("expected contains to return true for 'b'")
	}
	if contains(slice, "d") {
		t.Error("expected contains to return false for 'd'")
	}
	if contains(nil, "a") {
		t.Error("expected contains to return false for nil slice")
	}
}

func TestConcurrentJoinLeave(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			instID := "inst-" + string(rune('A'+id%26))
			c.Join(Instance{ID: instID, Endpoint: "http://a:8080"})
			c.Heartbeat(instID)
			c.Leave(instID)
		}(i)
	}
	wg.Wait()

	// After all leave, should be empty
	instances := c.ListInstances()
	if len(instances) != 0 {
		t.Errorf("expected 0 instances after concurrent join/leave, got %d", len(instances))
	}
}

func TestConcurrentQuery(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())
	c.Join(Instance{ID: "inst-1", Name: "n1", Endpoint: "http://a:8080"})
	c.Join(Instance{ID: "inst-2", Name: "n2", Endpoint: "http://b:8080"})

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			q := c.Query("list_topics", nil, nil)
			if q.Status != "completed" {
				t.Errorf("expected completed status")
			}
		}()
	}
	wg.Wait()
}
