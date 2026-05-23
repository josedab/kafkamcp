package federation

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// AddRoutingRule
// ---------------------------------------------------------------------------

func TestAddRoutingRuleValidToolMatch(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())

	rule, err := c.AddRoutingRule(RoutingRule{ToolMatch: "list_topics", Targets: []string{"inst-1"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rule.ID == "" {
		t.Error("expected auto-generated ID")
	}
	if !rule.Enabled {
		t.Error("expected Enabled=true")
	}
	if rule.ToolMatch != "list_topics" {
		t.Errorf("expected ToolMatch list_topics, got %s", rule.ToolMatch)
	}
}

func TestAddRoutingRuleExplicitID(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())

	rule, err := c.AddRoutingRule(RoutingRule{ID: "my-rule", ToolMatch: "describe_topic"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rule.ID != "my-rule" {
		t.Errorf("expected ID my-rule, got %s", rule.ID)
	}
}

func TestAddRoutingRuleEmptyToolMatchAndCluster(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())

	_, err := c.AddRoutingRule(RoutingRule{Targets: []string{"inst-1"}})
	if err == nil {
		t.Fatal("expected error for empty tool_match and cluster")
	}
	if !strings.Contains(err.Error(), "tool_match or cluster is required") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestAddRoutingRuleClusterOnly(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())

	rule, err := c.AddRoutingRule(RoutingRule{Cluster: "prod", Targets: []string{"inst-1"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rule.Cluster != "prod" {
		t.Errorf("expected cluster prod, got %s", rule.Cluster)
	}
	if !rule.Enabled {
		t.Error("expected Enabled=true")
	}
}

func TestAddRoutingRuleAuditLog(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())

	_, err := c.AddRoutingRule(RoutingRule{ToolMatch: "list_topics"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	log := c.GetAuditLog(10)
	if len(log) == 0 {
		t.Fatal("expected audit log entry")
	}
	found := false
	for _, entry := range log {
		if entry.Action == "routing_rule_added" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected routing_rule_added audit entry")
	}
}

// ---------------------------------------------------------------------------
// ListRoutingRules
// ---------------------------------------------------------------------------

func TestListRoutingRulesEmpty(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())

	rules := c.ListRoutingRules()
	if len(rules) != 0 {
		t.Errorf("expected no rules, got %d", len(rules))
	}
}

func TestListRoutingRulesAfterAdding(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())

	_, _ = c.AddRoutingRule(RoutingRule{ID: "r1", ToolMatch: "list_topics"})
	_, _ = c.AddRoutingRule(RoutingRule{ID: "r2", Cluster: "staging"})

	rules := c.ListRoutingRules()
	if len(rules) != 2 {
		t.Fatalf("expected 2 rules, got %d", len(rules))
	}

	ids := map[string]bool{}
	for _, r := range rules {
		ids[r.ID] = true
	}
	if !ids["r1"] || !ids["r2"] {
		t.Errorf("expected rules r1 and r2, got %v", ids)
	}
}

// ---------------------------------------------------------------------------
// ResolveRoute
// ---------------------------------------------------------------------------

func TestResolveRouteWithMatchingRule(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())

	_, _ = c.Join(Instance{ID: "inst-1", Name: "n1", Endpoint: "http://a:8080", Clusters: []string{"prod"}})
	_, _ = c.Join(Instance{ID: "inst-2", Name: "n2", Endpoint: "http://b:8080", Clusters: []string{"staging"}})
	_, _ = c.AddRoutingRule(RoutingRule{ToolMatch: "list_topics", Targets: []string{"inst-1"}})

	result := c.ResolveRoute("list_topics", "")
	if len(result) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(result))
	}
	if result[0].ID != "inst-1" {
		t.Errorf("expected inst-1, got %s", result[0].ID)
	}
}

func TestResolveRouteWildcardTool(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())

	_, _ = c.Join(Instance{ID: "inst-1", Name: "n1", Endpoint: "http://a:8080", Clusters: []string{"prod"}})
	_, _ = c.Join(Instance{ID: "inst-2", Name: "n2", Endpoint: "http://b:8080", Clusters: []string{"staging"}})
	_, _ = c.AddRoutingRule(RoutingRule{ToolMatch: "*", Targets: []string{"inst-2"}})

	result := c.ResolveRoute("any_tool", "")
	if len(result) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(result))
	}
	if result[0].ID != "inst-2" {
		t.Errorf("expected inst-2, got %s", result[0].ID)
	}
}

func TestResolveRouteNoRuleClusterMatch(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())

	_, _ = c.Join(Instance{ID: "inst-1", Name: "n1", Endpoint: "http://a:8080", Clusters: []string{"prod"}})
	_, _ = c.Join(Instance{ID: "inst-2", Name: "n2", Endpoint: "http://b:8080", Clusters: []string{"staging"}})

	result := c.ResolveRoute("list_topics", "prod")
	if len(result) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(result))
	}
	if result[0].ID != "inst-1" {
		t.Errorf("expected inst-1, got %s", result[0].ID)
	}
}

func TestResolveRouteNoRuleNoClusterReturnsAll(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())

	_, _ = c.Join(Instance{ID: "inst-1", Name: "n1", Endpoint: "http://a:8080", Clusters: []string{"prod"}})
	_, _ = c.Join(Instance{ID: "inst-2", Name: "n2", Endpoint: "http://b:8080", Clusters: []string{"staging"}})

	result := c.ResolveRoute("list_topics", "")
	if len(result) != 2 {
		t.Fatalf("expected 2 instances, got %d", len(result))
	}
}

func TestResolveRouteInactiveTargetFallsThrough(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())

	_, _ = c.Join(Instance{ID: "inst-1", Name: "n1", Endpoint: "http://a:8080", Clusters: []string{"prod"}})
	_, _ = c.Join(Instance{ID: "inst-2", Name: "n2", Endpoint: "http://b:8080", Clusters: []string{"prod"}})
	// Deregister inst-1 so the rule's target is inactive
	_ = c.Leave("inst-1")

	_, _ = c.AddRoutingRule(RoutingRule{ToolMatch: "list_topics", Targets: []string{"inst-1"}})

	// Should fall through to cluster routing since inst-1 is inactive
	result := c.ResolveRoute("list_topics", "prod")
	if len(result) != 1 {
		t.Fatalf("expected 1 instance via cluster fallback, got %d", len(result))
	}
	if result[0].ID != "inst-2" {
		t.Errorf("expected inst-2, got %s", result[0].ID)
	}
}

func TestResolveRouteEmptyFederation(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())

	result := c.ResolveRoute("list_topics", "prod")
	if len(result) != 0 {
		t.Errorf("expected no instances, got %d", len(result))
	}
}

// ---------------------------------------------------------------------------
// DiscoverServices
// ---------------------------------------------------------------------------

func TestDiscoverServicesEmpty(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())

	services := c.DiscoverServices()
	if len(services) != 0 {
		t.Errorf("expected no services, got %d", len(services))
	}
}

func TestDiscoverServicesMultipleInstances(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())

	_, _ = c.Join(Instance{ID: "inst-1", Name: "n1", Endpoint: "http://a:8080", Clusters: []string{"prod"}})
	_, _ = c.Join(Instance{ID: "inst-2", Name: "n2", Endpoint: "http://b:8080", Clusters: []string{"staging"}})

	services := c.DiscoverServices()
	if len(services) != 2 {
		t.Fatalf("expected 2 services, got %d", len(services))
	}

	byID := map[string]ServiceDiscovery{}
	for _, svc := range services {
		byID[svc.InstanceID] = svc
	}
	s1 := byID["inst-1"]
	if s1.InstanceName != "n1" || s1.Endpoint != "http://a:8080" || s1.Status != string(StatusActive) {
		t.Errorf("unexpected service data for inst-1: %+v", s1)
	}
	s2 := byID["inst-2"]
	if s2.InstanceName != "n2" || s2.Endpoint != "http://b:8080" || s2.Status != string(StatusActive) {
		t.Errorf("unexpected service data for inst-2: %+v", s2)
	}
}

func TestDiscoverServicesWithCapabilities(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())

	_, _ = c.Join(Instance{
		ID: "inst-1", Name: "n1", Endpoint: "http://a:8080",
		Clusters:     []string{"prod"},
		Capabilities: []string{"list_topics", "describe_topic"},
	})

	services := c.DiscoverServices()
	if len(services) != 1 {
		t.Fatalf("expected 1 service, got %d", len(services))
	}
	if len(services[0].Tools) != 2 {
		t.Errorf("expected 2 tools, got %d", len(services[0].Tools))
	}
}

// ---------------------------------------------------------------------------
// GetTopology
// ---------------------------------------------------------------------------

func TestGetTopologyEmpty(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())

	topo := c.GetTopology()
	if topo.LocalID != "local-1" {
		t.Errorf("expected localID local-1, got %s", topo.LocalID)
	}
	if len(topo.Instances) != 0 {
		t.Errorf("expected no instances, got %d", len(topo.Instances))
	}
	if len(topo.ClusterMap) != 0 {
		t.Errorf("expected empty ClusterMap, got %v", topo.ClusterMap)
	}
	if topo.TotalTools != 0 {
		t.Errorf("expected 0 tools, got %d", topo.TotalTools)
	}
}

func TestGetTopologySingleCluster(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())

	_, _ = c.Join(Instance{ID: "inst-1", Name: "n1", Endpoint: "http://a:8080", Clusters: []string{"prod"}})
	_, _ = c.Join(Instance{ID: "inst-2", Name: "n2", Endpoint: "http://b:8080", Clusters: []string{"prod"}})

	topo := c.GetTopology()
	if len(topo.ClusterMap) != 1 {
		t.Fatalf("expected 1 cluster, got %d", len(topo.ClusterMap))
	}
	prodInstances := topo.ClusterMap["prod"]
	if len(prodInstances) != 2 {
		t.Errorf("expected 2 instances in prod cluster, got %d", len(prodInstances))
	}
}

func TestGetTopologyMultiCluster(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())

	_, _ = c.Join(Instance{ID: "inst-1", Name: "n1", Endpoint: "http://a:8080", Clusters: []string{"prod"}})
	_, _ = c.Join(Instance{ID: "inst-2", Name: "n2", Endpoint: "http://b:8080", Clusters: []string{"staging"}})
	_, _ = c.Join(Instance{ID: "inst-3", Name: "n3", Endpoint: "http://c:8080", Clusters: []string{"prod", "staging"}})

	topo := c.GetTopology()
	if len(topo.ClusterMap) != 2 {
		t.Fatalf("expected 2 clusters, got %d", len(topo.ClusterMap))
	}
	if len(topo.ClusterMap["prod"]) != 2 {
		t.Errorf("expected 2 instances in prod, got %d", len(topo.ClusterMap["prod"]))
	}
	if len(topo.ClusterMap["staging"]) != 2 {
		t.Errorf("expected 2 instances in staging, got %d", len(topo.ClusterMap["staging"]))
	}
}

func TestGetTopologyTotalToolsUnique(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())

	_, _ = c.Join(Instance{
		ID: "inst-1", Name: "n1", Endpoint: "http://a:8080",
		Clusters:     []string{"prod"},
		Capabilities: []string{"list_topics", "describe_topic"},
	})
	_, _ = c.Join(Instance{
		ID: "inst-2", Name: "n2", Endpoint: "http://b:8080",
		Clusters:     []string{"staging"},
		Capabilities: []string{"list_topics", "produce_message"},
	})

	topo := c.GetTopology()
	// list_topics, describe_topic, produce_message = 3 unique
	if topo.TotalTools != 3 {
		t.Errorf("expected 3 unique tools, got %d", topo.TotalTools)
	}
}

func TestGetTopologyIncludesRoutingRules(t *testing.T) {
	t.Parallel()
	c := NewCoordinator("local-1", testLogger())

	_, _ = c.Join(Instance{ID: "inst-1", Name: "n1", Endpoint: "http://a:8080", Clusters: []string{"prod"}})
	_, _ = c.AddRoutingRule(RoutingRule{ID: "r1", ToolMatch: "list_topics", Targets: []string{"inst-1"}})

	topo := c.GetTopology()
	if len(topo.Routes) != 1 {
		t.Fatalf("expected 1 routing rule, got %d", len(topo.Routes))
	}
	if topo.Routes[0].ID != "r1" {
		t.Errorf("expected rule ID r1, got %s", topo.Routes[0].ID)
	}
}
