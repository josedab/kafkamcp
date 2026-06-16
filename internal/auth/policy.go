package auth

import (
	"github.com/josedab/kafkamcp/internal/config"
	"github.com/josedab/kafkamcp/internal/match"
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

// Decision explains a policy evaluation without mutating policy state.
type Decision struct {
	Allowed        bool         `json:"allowed"`
	AgentID        string       `json:"agent_id"`
	Role           string       `json:"role,omitempty"`
	ResourceType   ResourceType `json:"resource_type"`
	ResourceName   string       `json:"resource_name"`
	Action         Action       `json:"action"`
	MatchedPattern string       `json:"matched_pattern,omitempty"`
	Reason         string       `json:"reason"`
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
	return pe.Evaluate(agentID, resource, resourceName, action).Allowed
}

// Evaluate performs the same authorization check as Authorize and returns a
// deterministic explanation suitable for policy simulation and approval
// previews.
func (pe *PolicyEngine) Evaluate(agentID string, resource ResourceType, resourceName string, action Action) Decision {
	decision := Decision{
		AgentID:      agentID,
		ResourceType: resource,
		ResourceName: resourceName,
		Action:       action,
	}
	// If no policies are configured and default-deny is false, allow everything
	if !pe.defaultDeny && len(pe.agents) == 0 {
		decision.Allowed = true
		decision.Reason = "no policies configured and default_deny=false"
		return decision
	}

	policy, ok := pe.agents[agentID]
	if !ok {
		// Unknown agent: if default-deny, block; otherwise allow
		decision.Allowed = !pe.defaultDeny
		if decision.Allowed {
			decision.Reason = "unknown agent allowed because default_deny=false"
		} else {
			decision.Reason = "agent is not configured and default_deny=true"
		}
		return decision
	}
	decision.Role = policy.Role

	switch resource {
	case ResourceTopic:
		for _, topicPolicy := range policy.Topics {
			if !match.Glob(topicPolicy.Pattern, resourceName) {
				continue
			}
			decision.MatchedPattern = topicPolicy.Pattern
			for _, permission := range topicPolicy.Permissions {
				if permission == string(action) || permission == "admin" {
					decision.Allowed = true
					decision.Reason = "matching topic policy grants the requested action"
					return decision
				}
			}
			decision.Reason = "matching topic policy does not grant the requested action"
			return decision
		}
		decision.Reason = "no topic policy matches the requested resource"
	case ResourceConsumerGroup:
		for _, permission := range policy.ConsumerGroups.Permissions {
			if permission == string(action) || permission == "admin" {
				decision.Allowed = true
				decision.Reason = "consumer-group permissions grant the requested action"
				return decision
			}
		}
		decision.Reason = "consumer-group permissions do not grant the requested action"
	case ResourceSchema:
		for _, permission := range policy.Schemas.Permissions {
			if permission == string(action) || permission == "admin" {
				decision.Allowed = true
				decision.Reason = "schema permissions grant the requested action"
				return decision
			}
		}
		decision.Reason = "schema permissions do not grant the requested action"
	case ResourceCluster:
		if action == ActionRead || action == ActionDescribe {
			decision.Allowed = true
			decision.Reason = "configured agents may read cluster metadata"
			return decision
		}
		decision.Allowed = policy.Role == "admin"
		if decision.Allowed {
			decision.Reason = "agent has the admin role"
		} else {
			decision.Reason = "cluster mutation requires the admin role"
		}
	default:
		decision.Allowed = !pe.defaultDeny
		if decision.Allowed {
			decision.Reason = "unknown resource type allowed because default_deny=false"
		} else {
			decision.Reason = "unknown resource type denied because default_deny=true"
		}
	}
	return decision
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

// AgentRole returns an agent's configured role.
func (pe *PolicyEngine) AgentRole(agentID string) (string, bool) {
	policy, ok := pe.agents[agentID]
	if !ok {
		return "", false
	}
	return policy.Role, true
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
