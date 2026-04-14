package auth

import (
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/config"
	"github.com/kafkamcp/kafkamcp/internal/match"
)

func TestMatchPattern(t *testing.T) {
	tests := []struct {
		pattern string
		name    string
		want    bool
	}{
		{"*", "anything", true},
		{"orders.*", "orders.created", true},
		{"orders.*", "orders.deleted", true},
		{"orders.*", "payments.created", false},
		{"*.dlq", "orders.dlq", true},
		{"*.dlq", "orders.created", false},
		{"orders.created", "orders.created", true},
		{"orders.created", "orders.deleted", false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.name, func(t *testing.T) {
			got := match.Glob(tt.pattern, tt.name)
			if got != tt.want {
				t.Errorf("match.Glob(%q, %q) = %v, want %v", tt.pattern, tt.name, got, tt.want)
			}
		})
	}
}

func TestPolicyEngine(t *testing.T) {
	cfg := config.PolicyConfig{
		DefaultDeny: true,
		Agents: []config.AgentPolicy{
			{
				ID:   "reader-agent",
				Role: "reader",
				Topics: []config.TopicPermission{
					{Pattern: "orders.*", Permissions: []string{"read"}},
					{Pattern: "*.dlq", Permissions: []string{"read"}},
				},
				ConsumerGroups: config.GroupPermission{Permissions: []string{"describe"}},
				Schemas:        config.SchemaPermission{Permissions: []string{"read"}},
			},
			{
				ID:   "admin-agent",
				Role: "admin",
				Topics: []config.TopicPermission{
					{Pattern: "*", Permissions: []string{"read", "write", "create", "delete"}},
				},
				ConsumerGroups: config.GroupPermission{Permissions: []string{"describe", "reset"}},
				Schemas:        config.SchemaPermission{Permissions: []string{"read", "write"}},
			},
		},
	}

	pe := NewPolicyEngine(cfg)

	tests := []struct {
		name     string
		agentID  string
		resource ResourceType
		resName  string
		action   Action
		want     bool
	}{
		{"reader can read orders", "reader-agent", ResourceTopic, "orders.created", ActionRead, true},
		{"reader cannot write orders", "reader-agent", ResourceTopic, "orders.created", ActionWrite, false},
		{"reader can read DLQ", "reader-agent", ResourceTopic, "payments.dlq", ActionRead, true},
		{"reader cannot read payments", "reader-agent", ResourceTopic, "payments.created", ActionRead, false},
		{"admin can read anything", "admin-agent", ResourceTopic, "any.topic", ActionRead, true},
		{"admin can write anything", "admin-agent", ResourceTopic, "any.topic", ActionWrite, true},
		{"unknown agent blocked", "unknown", ResourceTopic, "orders.created", ActionRead, false},
		{"reader can describe groups", "reader-agent", ResourceConsumerGroup, "", ActionDescribe, true},
		{"reader cannot reset groups", "reader-agent", ResourceConsumerGroup, "", ActionReset, false},
		{"reader can read schemas", "reader-agent", ResourceSchema, "", ActionRead, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pe.Authorize(tt.agentID, tt.resource, tt.resName, tt.action)
			if got != tt.want {
				t.Errorf("Authorize(%q, %v, %q, %v) = %v, want %v",
					tt.agentID, tt.resource, tt.resName, tt.action, got, tt.want)
			}
		})
	}
}

func TestFilterTopics(t *testing.T) {
	cfg := config.PolicyConfig{
		DefaultDeny: true,
		Agents: []config.AgentPolicy{
			{
				ID: "reader",
				Topics: []config.TopicPermission{
					{Pattern: "orders.*", Permissions: []string{"read"}},
				},
			},
		},
	}

	pe := NewPolicyEngine(cfg)
	topics := []string{"orders.created", "orders.deleted", "payments.completed", "users.updated"}
	filtered := pe.FilterTopics("reader", topics)

	if len(filtered) != 2 {
		t.Fatalf("expected 2 topics, got %d: %v", len(filtered), filtered)
	}
	for _, f := range filtered {
		if f != "orders.created" && f != "orders.deleted" {
			t.Errorf("unexpected filtered topic: %s", f)
		}
	}
}

func TestNoPolicies(t *testing.T) {
	pe := NewPolicyEngine(config.PolicyConfig{DefaultDeny: false})
	if !pe.Authorize("anyone", ResourceTopic, "any.topic", ActionRead) {
		t.Error("expected allow when no policies and default-deny=false")
	}
}
