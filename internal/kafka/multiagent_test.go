package kafka

import (
	"testing"
)

func TestNewAgentRegistry(t *testing.T) {
	reg := NewAgentRegistry()
	if reg == nil {
		t.Fatal("expected non-nil registry")
	}
	agents := reg.ListAgents()
	if len(agents) != 0 {
		t.Errorf("expected 0 agents, got %d", len(agents))
	}
}

func TestRegisterAgent(t *testing.T) {
	reg := NewAgentRegistry()
	agent := reg.RegisterAgent("agent-1", "Test Agent")
	if agent.ID != "agent-1" {
		t.Errorf("expected ID 'agent-1', got %q", agent.ID)
	}
	if agent.Status != "active" {
		t.Errorf("expected status 'active', got %q", agent.Status)
	}
	if agent.Partitions == nil {
		t.Error("expected non-nil Partitions map")
	}
}

func TestListAgents(t *testing.T) {
	reg := NewAgentRegistry()
	reg.RegisterAgent("a1", "Agent 1")
	reg.RegisterAgent("a2", "Agent 2")

	agents := reg.ListAgents()
	if len(agents) != 2 {
		t.Errorf("expected 2 agents, got %d", len(agents))
	}
}

func TestHeartbeat(t *testing.T) {
	reg := NewAgentRegistry()
	reg.RegisterAgent("a1", "Agent 1")

	err := reg.Heartbeat("a1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHeartbeatUnregistered(t *testing.T) {
	reg := NewAgentRegistry()
	err := reg.Heartbeat("nonexistent")
	if err == nil {
		t.Error("expected error for unregistered agent")
	}
}

func TestClaimPartition(t *testing.T) {
	reg := NewAgentRegistry()
	reg.RegisterAgent("a1", "Agent 1")

	err := reg.ClaimPartition("a1", "orders", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	agents := reg.ListAgents()
	for _, a := range agents {
		if a.ID == "a1" {
			parts := a.Partitions["orders"]
			if len(parts) != 1 || parts[0] != 0 {
				t.Errorf("expected partition [0], got %v", parts)
			}
		}
	}
}

func TestClaimPartitionUnregistered(t *testing.T) {
	reg := NewAgentRegistry()
	err := reg.ClaimPartition("nonexistent", "orders", 0)
	if err == nil {
		t.Error("expected error for unregistered agent")
	}
}

func TestClaimPartitionConflict(t *testing.T) {
	reg := NewAgentRegistry()
	reg.RegisterAgent("a1", "Agent 1")
	reg.RegisterAgent("a2", "Agent 2")

	reg.ClaimPartition("a1", "orders", 0)
	err := reg.ClaimPartition("a2", "orders", 0)
	if err == nil {
		t.Error("expected error for conflicting claim")
	}
}

func TestClaimPartitionSameAgent(t *testing.T) {
	reg := NewAgentRegistry()
	reg.RegisterAgent("a1", "Agent 1")

	reg.ClaimPartition("a1", "orders", 0)
	err := reg.ClaimPartition("a1", "orders", 0)
	if err != nil {
		t.Fatalf("expected no error for same agent re-claim, got: %v", err)
	}
}

func TestReleasePartition(t *testing.T) {
	reg := NewAgentRegistry()
	reg.RegisterAgent("a1", "Agent 1")
	reg.ClaimPartition("a1", "orders", 0)

	err := reg.ReleasePartition("a1", "orders", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReleasePartitionWrongOwner(t *testing.T) {
	reg := NewAgentRegistry()
	reg.RegisterAgent("a1", "Agent 1")
	reg.RegisterAgent("a2", "Agent 2")
	reg.ClaimPartition("a1", "orders", 0)

	err := reg.ReleasePartition("a2", "orders", 0)
	if err == nil {
		t.Error("expected error when releasing partition owned by another agent")
	}
}

func TestShareCursor(t *testing.T) {
	reg := NewAgentRegistry()
	cursor := reg.ShareCursor("a1", "orders", 0, 42)
	if cursor == nil {
		t.Fatal("expected non-nil cursor")
	}
	if cursor.Offset != 42 {
		t.Errorf("expected offset 42, got %d", cursor.Offset)
	}
}

func TestGetCursors(t *testing.T) {
	reg := NewAgentRegistry()
	reg.ShareCursor("a1", "orders", 0, 10)
	reg.ShareCursor("a1", "orders", 1, 20)

	cursors := reg.GetCursors()
	if len(cursors) != 2 {
		t.Errorf("expected 2 cursors, got %d", len(cursors))
	}
}

func TestGetCursorsEmpty(t *testing.T) {
	reg := NewAgentRegistry()
	cursors := reg.GetCursors()
	if len(cursors) != 0 {
		t.Errorf("expected 0 cursors, got %d", len(cursors))
	}
}

func TestQueueHandoff(t *testing.T) {
	reg := NewAgentRegistry()
	reg.RegisterAgent("a1", "Agent 1")
	reg.RegisterAgent("a2", "Agent 2")

	msg, err := reg.QueueHandoff("a1", "a2", "process this data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.Status != "pending" {
		t.Errorf("expected status 'pending', got %q", msg.Status)
	}
	if msg.FromAgent != "a1" {
		t.Errorf("expected FromAgent 'a1', got %q", msg.FromAgent)
	}
}

func TestQueueHandoffUnregisteredTarget(t *testing.T) {
	reg := NewAgentRegistry()
	reg.RegisterAgent("a1", "Agent 1")

	_, err := reg.QueueHandoff("a1", "nonexistent", "data")
	if err == nil {
		t.Error("expected error for unregistered target agent")
	}
}

func TestPendingHandoffs(t *testing.T) {
	reg := NewAgentRegistry()
	reg.RegisterAgent("a1", "Agent 1")
	reg.RegisterAgent("a2", "Agent 2")

	reg.QueueHandoff("a1", "a2", "data1")
	reg.QueueHandoff("a1", "a2", "data2")

	pending := reg.PendingHandoffs("a2")
	if len(pending) != 2 {
		t.Errorf("expected 2 pending handoffs, got %d", len(pending))
	}

	// Should be delivered now
	pending2 := reg.PendingHandoffs("a2")
	if len(pending2) != 0 {
		t.Errorf("expected 0 pending after delivery, got %d", len(pending2))
	}
}

func TestPendingHandoffsNoMessages(t *testing.T) {
	reg := NewAgentRegistry()
	reg.RegisterAgent("a1", "Agent 1")

	pending := reg.PendingHandoffs("a1")
	if len(pending) != 0 {
		t.Errorf("expected 0 pending, got %d", len(pending))
	}
}

func TestAppendUnique(t *testing.T) {
	tests := []struct {
		name     string
		slice    []int32
		val      int32
		wantLen  int
	}{
		{"empty slice", nil, 1, 1},
		{"add new", []int32{1, 2}, 3, 3},
		{"duplicate", []int32{1, 2, 3}, 2, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := appendUnique(tt.slice, tt.val)
			if len(result) != tt.wantLen {
				t.Errorf("expected len %d, got %d", tt.wantLen, len(result))
			}
		})
	}
}

func TestRemovePartition(t *testing.T) {
	tests := []struct {
		name    string
		slice   []int32
		val     int32
		wantLen int
	}{
		{"remove existing", []int32{1, 2, 3}, 2, 2},
		{"remove nonexistent", []int32{1, 2, 3}, 4, 3},
		{"empty slice", nil, 1, 0},
		{"remove only element", []int32{1}, 1, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := removePartition(tt.slice, tt.val)
			if len(result) != tt.wantLen {
				t.Errorf("expected len %d, got %d", tt.wantLen, len(result))
			}
		})
	}
}
