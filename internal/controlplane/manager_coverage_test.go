package controlplane

import (
	"log/slog"
	"testing"
	"time"
)

// --- ListInstances ---

func TestListInstances_FilterByOrgID(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	org1, _ := m.CreateOrganization("Org1", "pro")
	org2, _ := m.CreateOrganization("Org2", "starter")

	_, _ = m.ProvisionInstance(org1.ID, "inst-1", "standard", "us-east-1", InstanceConfig{BootstrapServers: "kafka:9092"})
	_, _ = m.ProvisionInstance(org2.ID, "inst-2", "standard", "eu-west-1", InstanceConfig{BootstrapServers: "kafka:9092"})

	instances := m.ListInstances(org1.ID)
	if len(instances) != 1 {
		t.Fatalf("expected 1 instance for org1, got %d", len(instances))
	}
	if instances[0].OrgID != org1.ID {
		t.Errorf("expected org %s, got %s", org1.ID, instances[0].OrgID)
	}
}

func TestListInstances_AllWithEmptyOrgID(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	org1, _ := m.CreateOrganization("Org1", "pro")
	org2, _ := m.CreateOrganization("Org2", "starter")

	_, _ = m.ProvisionInstance(org1.ID, "inst-1", "", "", InstanceConfig{BootstrapServers: "kafka:9092"})
	_, _ = m.ProvisionInstance(org2.ID, "inst-2", "", "", InstanceConfig{BootstrapServers: "kafka:9092"})

	instances := m.ListInstances("")
	if len(instances) != 2 {
		t.Errorf("expected 2 instances with empty orgID, got %d", len(instances))
	}
}

func TestListInstances_EmptyResults(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	instances := m.ListInstances("nonexistent-org")
	if len(instances) != 0 {
		t.Errorf("expected 0 instances, got %d", len(instances))
	}
}

func TestListInstances_MultipleOrgs(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	org1, _ := m.CreateOrganization("Org1", "pro")
	org2, _ := m.CreateOrganization("Org2", "enterprise")

	_, _ = m.ProvisionInstance(org1.ID, "inst-1", "", "", InstanceConfig{BootstrapServers: "k:9092"})
	_, _ = m.ProvisionInstance(org1.ID, "inst-2", "", "", InstanceConfig{BootstrapServers: "k:9092"})
	_, _ = m.ProvisionInstance(org2.ID, "inst-3", "", "", InstanceConfig{BootstrapServers: "k:9092"})

	org1Instances := m.ListInstances(org1.ID)
	if len(org1Instances) != 2 {
		t.Errorf("expected 2 for org1, got %d", len(org1Instances))
	}

	org2Instances := m.ListInstances(org2.ID)
	if len(org2Instances) != 1 {
		t.Errorf("expected 1 for org2, got %d", len(org2Instances))
	}
}

// --- AddMember ---

func TestAddMember_Valid(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	org, _ := m.CreateOrganization("Test Corp", "pro")

	err := m.AddMember(org.ID, "user-1", "alice@test.com", "admin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, _ := m.GetOrganization(org.ID)
	if len(got.Members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(got.Members))
	}
	if got.Members[0].Email != "alice@test.com" {
		t.Errorf("expected alice@test.com, got %s", got.Members[0].Email)
	}
	if got.Members[0].Role != "admin" {
		t.Errorf("expected admin role, got %s", got.Members[0].Role)
	}
}

func TestAddMember_Duplicate(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	org, _ := m.CreateOrganization("Test Corp", "pro")

	_ = m.AddMember(org.ID, "user-1", "alice@test.com", "admin")
	_ = m.AddMember(org.ID, "user-2", "alice@test.com", "viewer")

	got, _ := m.GetOrganization(org.ID)
	// Current implementation allows duplicates
	if len(got.Members) != 2 {
		t.Errorf("expected 2 members (duplicates allowed), got %d", len(got.Members))
	}
}

func TestAddMember_NonExistentOrg(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())

	err := m.AddMember("nonexistent", "user-1", "alice@test.com", "admin")
	if err == nil {
		t.Error("expected error for non-existent org")
	}
}

func TestAddMember_UpdatesTimestamp(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	org, _ := m.CreateOrganization("Test Corp", "pro")

	beforeAdd := org.UpdatedAt
	time.Sleep(1 * time.Millisecond)
	_ = m.AddMember(org.ID, "user-1", "bob@test.com", "viewer")

	got, _ := m.GetOrganization(org.ID)
	if !got.UpdatedAt.After(beforeAdd) {
		t.Error("expected UpdatedAt to be updated after AddMember")
	}
}

func TestAddMember_AllFields(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	org, _ := m.CreateOrganization("Corp", "pro")

	_ = m.AddMember(org.ID, "uid-123", "user@example.com", "operator")

	got, _ := m.GetOrganization(org.ID)
	member := got.Members[0]
	if member.UserID != "uid-123" {
		t.Errorf("UserID = %s, want uid-123", member.UserID)
	}
	if member.Email != "user@example.com" {
		t.Errorf("Email = %s, want user@example.com", member.Email)
	}
	if member.Role != "operator" {
		t.Errorf("Role = %s, want operator", member.Role)
	}
}

// --- GenerateAPIKey improvements ---

func TestGenerateAPIKey_Format(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	org, _ := m.CreateOrganization("Test", "pro")

	key, err := m.CreateAPIKey(org.ID, "test-key", []string{"read"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key.Key == "" {
		t.Error("expected non-empty key")
	}
	if !key.Active {
		t.Error("expected active key")
	}
}

func TestGenerateAPIKey_UniqueKeys(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	org, _ := m.CreateOrganization("Test", "pro")

	keys := make(map[string]bool)
	for i := 0; i < 10; i++ {
		key, err := m.CreateAPIKey(org.ID, "key", []string{"read"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if keys[key.Key] {
			t.Fatalf("duplicate key generated: %s", key.Key)
		}
		keys[key.Key] = true
	}
}

func TestGenerateAPIKey_NonExistentOrg(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	_, err := m.CreateAPIKey("nonexistent", "key", nil)
	if err == nil {
		t.Error("expected error for non-existent org")
	}
}
