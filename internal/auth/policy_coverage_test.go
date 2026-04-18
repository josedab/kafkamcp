package auth

import (
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/config"
)

func TestDefaultDeny(t *testing.T) {
	pe := NewPolicyEngine(config.PolicyConfig{DefaultDeny: true})
	if !pe.DefaultDeny() {
		t.Error("expected DefaultDeny=true")
	}

	pe2 := NewPolicyEngine(config.PolicyConfig{DefaultDeny: false})
	if pe2.DefaultDeny() {
		t.Error("expected DefaultDeny=false")
	}
}

func TestAgentExists(t *testing.T) {
	pe := NewPolicyEngine(config.PolicyConfig{
		DefaultDeny: true,
		Agents: []config.AgentPolicy{
			{ID: "test-agent", Role: "admin"},
		},
	})

	if !pe.AgentExists("test-agent") {
		t.Error("expected test-agent to exist")
	}
	if pe.AgentExists("nonexistent") {
		t.Error("expected nonexistent agent to not exist")
	}
}

func TestAuthorizeClusterAction(t *testing.T) {
	pe := NewPolicyEngine(config.PolicyConfig{
		DefaultDeny: true,
		Agents: []config.AgentPolicy{
			{
				ID:   "admin-agent",
				Role: "admin",
				Topics: []config.TopicPermission{
					{Pattern: "*", Permissions: []string{"read", "write", "create", "delete"}},
				},
			},
			{
				ID:   "reader-agent",
				Role: "reader",
				Topics: []config.TopicPermission{
					{Pattern: "*", Permissions: []string{"read"}},
				},
			},
		},
	})

	// Admin agent can do cluster admin actions
	if !pe.Authorize("admin-agent", ResourceCluster, "", ActionAdmin) {
		t.Error("expected admin agent to be authorized for cluster admin")
	}

	// Reader agent can read cluster
	if !pe.Authorize("reader-agent", ResourceCluster, "", ActionRead) {
		t.Error("expected reader agent to be authorized for cluster read")
	}

	// Reader agent cannot do cluster admin
	if pe.Authorize("reader-agent", ResourceCluster, "", ActionAdmin) {
		t.Error("expected reader agent to be denied cluster admin")
	}
}

func TestAuthorizeSchemaAction(t *testing.T) {
	pe := NewPolicyEngine(config.PolicyConfig{
		DefaultDeny: true,
		Agents: []config.AgentPolicy{
			{
				ID:      "schema-agent",
				Role:    "reader",
				Schemas: config.SchemaPermission{Permissions: []string{"read"}},
			},
			{
				ID:      "schema-writer",
				Role:    "writer",
				Schemas: config.SchemaPermission{Permissions: []string{"read", "write"}},
			},
		},
	})

	if !pe.Authorize("schema-agent", ResourceSchema, "", ActionRead) {
		t.Error("expected schema agent to read schemas")
	}
	if pe.Authorize("schema-agent", ResourceSchema, "", ActionWrite) {
		t.Error("expected schema agent to be denied write")
	}
	if !pe.Authorize("schema-writer", ResourceSchema, "", ActionWrite) {
		t.Error("expected schema writer to write schemas")
	}
}
