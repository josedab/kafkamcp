package auth

import (
	"github.com/kafkamcp/kafkamcp/internal/config"
	"github.com/kafkamcp/kafkamcp/internal/match"
)

// Action represents a permission action.
type Action string

// ActionRead and related values define constants for this package.
const (
	ActionRead     Action = "read"
	ActionWrite    Action = "write"
	ActionCreate   Action = "create"
	ActionDelete   Action = "delete"
	ActionAdmin    Action = "admin"
	ActionDescribe Action = "describe"
	ActionReset    Action = "reset"
)

// ResourceType identifies the type of resource being accessed.
type ResourceType string

// ResourceTopic and related values define constants for this package.
const (
	ResourceTopic         ResourceType = "topic"
	ResourceConsumerGroup ResourceType = "consumer_group"
	ResourceSchema        ResourceType = "schema"
	ResourceCluster       ResourceType = "cluster"
)

// PolicyEngine evaluates per-request authorization against the policy file.
type PolicyEngine struct {
	defaultDeny bool
	agents      map[string]*config.AgentPolicy
}

// NewPolicyEngine creates a new policy engine from configuration.
func NewPolicyEngine(cfg config.PolicyConfig) *PolicyEngine {
	pe := &PolicyEngine{
		defaultDeny: cfg.DefaultDeny,
		agents:      make(map[string]*config.AgentPolicy),
	}
	for i := range cfg.Agents {
		pe.agents[cfg.Agents[i].ID] = &cfg.Agents[i]
	}
	return pe
}

// Authorize checks whether an agent is allowed to perform an action on a resource.
func (pe *PolicyEngine) Authorize(agentID string, resource ResourceType, resourceName string, action Action) bool {
	// If no policies are configured and default-deny is false, allow everything
	if !pe.defaultDeny && len(pe.agents) == 0 {
		return true
	}

	policy, ok := pe.agents[agentID]
	if !ok {
		// Unknown agent: if default-deny, block; otherwise allow
		return !pe.defaultDeny
	}

	switch resource {
	case ResourceTopic:
		return pe.authorizeTopicAction(policy, resourceName, action)
	case ResourceConsumerGroup:
		return pe.authorizeGroupAction(policy, action)
	case ResourceSchema:
		return pe.authorizeSchemaAction(policy, action)
	case ResourceCluster:
		return pe.authorizeClusterAction(policy, action)
	default:
		return !pe.defaultDeny
	}
}

func (pe *PolicyEngine) authorizeTopicAction(policy *config.AgentPolicy, topicName string, action Action) bool {
	for _, tp := range policy.Topics {
		if match.Glob(tp.Pattern, topicName) {
			for _, perm := range tp.Permissions {
				if perm == string(action) || perm == "admin" {
					return true
				}
			}
		}
	}
	return false
}

func (pe *PolicyEngine) authorizeGroupAction(policy *config.AgentPolicy, action Action) bool {
	for _, perm := range policy.ConsumerGroups.Permissions {
		if perm == string(action) {
			return true
		}
	}
	return false
}

func (pe *PolicyEngine) authorizeSchemaAction(policy *config.AgentPolicy, action Action) bool {
	for _, perm := range policy.Schemas.Permissions {
		if perm == string(action) {
			return true
		}
	}
	return false
}

func (pe *PolicyEngine) authorizeClusterAction(policy *config.AgentPolicy, action Action) bool {
	// Read/describe access is allowed for all configured agents
	if action == ActionRead || action == ActionDescribe {
		return true
	}
	// Admin actions require admin role
	return policy.Role == "admin"
}

// DefaultDeny returns whether the policy engine is in default-deny mode.
func (pe *PolicyEngine) DefaultDeny() bool {
	return pe.defaultDeny
}

// AgentExists returns true if the agent ID is configured in the policy.
func (pe *PolicyEngine) AgentExists(agentID string) bool {
	_, ok := pe.agents[agentID]
	return ok
}

// FilterTopics filters a list of topic names by the agent's read permissions.
func (pe *PolicyEngine) FilterTopics(agentID string, topics []string) []string {
	if !pe.defaultDeny && len(pe.agents) == 0 {
		return topics
	}

	var filtered []string
	for _, t := range topics {
		if pe.Authorize(agentID, ResourceTopic, t, ActionRead) {
			filtered = append(filtered, t)
		}
	}
	return filtered
}
