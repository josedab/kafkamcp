package kafka

import "testing"

func TestAnalyzeACLSecurity(t *testing.T) {
	t.Parallel()
	acls := []ACLEntry{
		{Principal: "User:admin", ResourceType: "TOPIC", ResourceName: "*", Operation: "ALL"},
	}
	for i := 0; i < 10; i++ {
		acls = append(acls, ACLEntry{
			Principal: "User:admin", ResourceType: "TOPIC",
			ResourceName: "topic", Operation: "READ",
		})
	}
	result := analyzeACLSecurity(acls)
	if len(result.WildcardACLs) != 1 {
		t.Fatalf("wildcard findings = %d, want 1", len(result.WildcardACLs))
	}
	if len(result.OverPermissioned) != 1 {
		t.Fatalf("over-permissioned findings = %d, want 1", len(result.OverPermissioned))
	}
	if result.Findings != 2 {
		t.Fatalf("total findings = %d, want 2", result.Findings)
	}
}

func TestACLMatchesFilters(t *testing.T) {
	t.Parallel()
	entry := ACLEntry{Principal: "User:app10", ResourceType: "TOPIC"}
	if aclMatchesFilters(entry, "User:app1", "") {
		t.Fatal("substring principal must not match")
	}
	if !aclMatchesFilters(entry, "User:app10", "topic") {
		t.Fatal("exact principal and case-insensitive resource type should match")
	}
	if aclMatchesFilters(entry, "User:app10", "group") {
		t.Fatal("different resource type must not match")
	}
}
