package kafka

import (
	"context"
	"testing"
)

func TestValidateACLRule(t *testing.T) {
	t.Parallel()
	valid := ACLEntry{
		Principal: "User:app1", Host: "*", ResourceType: "topic",
		ResourceName: "orders", PatternType: "literal", Operation: "read", Permission: "allow",
	}

	if err := ValidateACLRule(valid); err != nil {
		t.Fatalf("expected valid rule, got %v", err)
	}

	cases := map[string]ACLEntry{
		"missing principal": {ResourceType: "topic", ResourceName: "o", Operation: "read", Permission: "allow", Host: "*"},
		"bad principal":     {Principal: "app1", ResourceType: "topic", ResourceName: "o", Operation: "read", Permission: "allow", Host: "*"},
		"missing host":      {Principal: "User:a", ResourceType: "topic", ResourceName: "o", Operation: "read", Permission: "allow"},
		"bad resource type": {Principal: "User:a", Host: "*", ResourceType: "banana", ResourceName: "o", Operation: "read", Permission: "allow"},
		"missing resource":  {Principal: "User:a", Host: "*", ResourceType: "topic", Operation: "read", Permission: "allow"},
		"bad pattern":       {Principal: "User:a", Host: "*", ResourceType: "topic", ResourceName: "o", PatternType: "regex", Operation: "read", Permission: "allow"},
		"bad operation":     {Principal: "User:a", Host: "*", ResourceType: "topic", ResourceName: "o", Operation: "frobnicate", Permission: "allow"},
		"any operation":     {Principal: "User:a", Host: "*", ResourceType: "topic", ResourceName: "o", Operation: "any", Permission: "allow"},
		"bad permission":    {Principal: "User:a", Host: "*", ResourceType: "topic", ResourceName: "o", Operation: "read", Permission: "maybe"},
		"cluster with name": {Principal: "User:a", Host: "*", ResourceType: "cluster", ResourceName: "my-cluster", Operation: "describe", Permission: "allow"},
	}
	for name, rule := range cases {
		if err := ValidateACLRule(rule); err == nil {
			t.Errorf("%s: expected validation error, got nil", name)
		}
	}

}

func TestValidateACLRuleRejectsNonConcretePermission(t *testing.T) {
	t.Parallel()
	rule := ACLEntry{
		Principal: "User:app1", Host: "*", ResourceType: "topic",
		ResourceName: "orders", PatternType: "literal",
		Operation: "read", Permission: "allow",
	}
	for _, permission := range []string{"any", "unknown"} {
		rule.Permission = permission
		if err := ValidateACLRule(rule); err == nil {
			t.Errorf("permission %q should be rejected", permission)
		}
	}
}

func TestACLMatchesFiltersUsesExactPrincipal(t *testing.T) {
	t.Parallel()
	entry := ACLEntry{Principal: "User:app10", ResourceType: "TOPIC"}
	if aclMatchesFilters(entry, "User:app1", "") {
		t.Fatal("substring principal must not match")
	}
	if !aclMatchesFilters(entry, "User:app10", "topic") {
		t.Fatal("exact principal and case-insensitive resource type should match")
	}
}

func TestValidateACLRule_ClusterNoName(t *testing.T) {
	t.Parallel()
	rule := ACLEntry{Principal: "User:a", Host: "*", ResourceType: "cluster", Operation: "describe", Permission: "allow"}
	if err := ValidateACLRule(rule); err != nil {
		t.Fatalf("cluster rule without name should be valid, got %v", err)
	}
}

func TestNormalizeACLRule_Defaults(t *testing.T) {
	t.Parallel()
	out := NormalizeACLRule(ACLEntry{Principal: "User:a", ResourceType: "Topic", ResourceName: "orders", Operation: "Read"})
	if out.Host != "*" {
		t.Errorf("expected default host *, got %q", out.Host)
	}
	if out.PatternType != "literal" {
		t.Errorf("expected default pattern literal, got %q", out.PatternType)
	}
	if out.Permission != "allow" {
		t.Errorf("expected default permission allow, got %q", out.Permission)
	}
	if out.ResourceType != "topic" {
		t.Errorf("expected canonical resource type topic, got %q", out.ResourceType)
	}
	if out.Operation != "read" {
		t.Errorf("expected lowercase operation read, got %q", out.Operation)
	}
}

func TestAclBuilderForRule(t *testing.T) {
	t.Parallel()
	for _, rt := range []string{"topic", "group", "cluster", "transactional_id"} {
		rule := ACLEntry{Principal: "User:a", Host: "*", ResourceType: rt, ResourceName: "x", Operation: "describe", Permission: "allow", PatternType: "literal"}
		if rt == "cluster" {
			rule.ResourceName = ""
		}
		if _, err := aclBuilderForRule(rule); err != nil {
			t.Errorf("resource_type %s: unexpected error %v", rt, err)
		}
	}
	// deny path
	deny := ACLEntry{Principal: "User:a", Host: "1.2.3.4", ResourceType: "topic", ResourceName: "x", Operation: "write", Permission: "deny", PatternType: "prefixed"}
	if _, err := aclBuilderForRule(deny); err != nil {
		t.Errorf("deny rule: unexpected error %v", err)
	}
}

func TestMockManager_CreateDeleteACL(t *testing.T) {
	m := &MockKafkaManager{}
	if err := m.CreateACL(context.Background(), "c1", ACLEntry{Principal: "User:a"}); err != nil {
		t.Fatalf("default CreateACL: %v", err)
	}
	if n, err := m.DeleteACL(context.Background(), "c1", ACLEntry{Principal: "User:a"}); err != nil || n != 0 {
		t.Fatalf("default DeleteACL: n=%d err=%v", n, err)
	}
	if m.CallCount("CreateACL") != 1 || m.CallCount("DeleteACL") != 1 {
		t.Errorf("expected recorded calls, got create=%d delete=%d", m.CallCount("CreateACL"), m.CallCount("DeleteACL"))
	}
}
