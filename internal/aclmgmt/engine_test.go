package aclmgmt

import (
	"log/slog"
	"testing"
)

func TestSetAndGetDesiredState(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	rules := []ACLRule{
		{ResourceType: "topic", ResourceName: "orders", Operation: "read"},
		{ResourceType: "topic", ResourceName: "orders", Operation: "write"},
	}
	e.SetDesiredState("User:app1", rules)

	got := e.GetDesiredState("User:app1")
	if len(got) != 2 {
		t.Errorf("expected 2 rules, got %d", len(got))
	}
	if got[0].Principal != "User:app1" {
		t.Errorf("expected principal set, got %s", got[0].Principal)
	}
	if got[0].Host != "*" {
		t.Errorf("expected default host *, got %s", got[0].Host)
	}
}

func TestComputeDiff(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	desired := []ACLRule{
		{Principal: "User:app1", Host: "*", ResourceType: "topic", ResourceName: "orders", PatternType: "literal", Operation: "read", Permission: "allow"},
		{Principal: "User:app1", Host: "*", ResourceType: "topic", ResourceName: "orders", PatternType: "literal", Operation: "write", Permission: "allow"},
	}
	actual := []ACLRule{
		{Principal: "User:app1", Host: "*", ResourceType: "topic", ResourceName: "orders", PatternType: "literal", Operation: "read", Permission: "allow"},
		{Principal: "User:app1", Host: "*", ResourceType: "topic", ResourceName: "events", PatternType: "literal", Operation: "read", Permission: "allow"},
	}

	diff := e.ComputeDiff(desired, actual)
	if len(diff.ToAdd) != 1 {
		t.Errorf("expected 1 to add, got %d", len(diff.ToAdd))
	}
	if len(diff.ToRemove) != 1 {
		t.Errorf("expected 1 to remove, got %d", len(diff.ToRemove))
	}
	if len(diff.InSync) != 1 {
		t.Errorf("expected 1 in sync, got %d", len(diff.InSync))
	}
}

func TestAudit(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	acls := []ACLRule{
		{Principal: "User:admin", ResourceType: "cluster", ResourceName: "*", Operation: "all", Permission: "allow"},
		{Principal: "User:app1", ResourceType: "topic", ResourceName: "orders", Operation: "read", Permission: "allow"},
	}

	result := e.Audit(acls)
	if result.TotalACLs != 2 {
		t.Errorf("expected 2 total ACLs, got %d", result.TotalACLs)
	}
	if len(result.WildcardRules) != 1 {
		t.Errorf("expected 1 wildcard rule, got %d", len(result.WildcardRules))
	}
	if len(result.OverPermissioned) != 1 {
		t.Errorf("expected 1 over-permissioned, got %d", len(result.OverPermissioned))
	}
	if len(result.Recommendations) == 0 {
		t.Error("expected recommendations")
	}
}

func TestDryRun(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	desired := []ACLRule{
		{Principal: "User:app1", Host: "*", ResourceType: "topic", ResourceName: "orders", PatternType: "literal", Operation: "read", Permission: "allow"},
	}
	actual := []ACLRule{
		{Principal: "User:app1", Host: "*", ResourceType: "topic", ResourceName: "events", PatternType: "literal", Operation: "read", Permission: "allow"},
	}

	result := e.DryRun(desired, actual)
	if len(result.WouldAdd) != 1 {
		t.Errorf("expected 1 to add, got %d", len(result.WouldAdd))
	}
	if len(result.WouldRemove) != 1 {
		t.Errorf("expected 1 to remove, got %d", len(result.WouldRemove))
	}
}
