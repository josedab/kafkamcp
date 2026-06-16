package server

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	opaEngine "github.com/josedab/kafkamcp/internal/opa"
)

func (s *Server) registerOPATools() {
	if s.opaPolicyEngine == nil {
		return
	}

	s.addTool(mcp.NewTool("kafka_policy_evaluate",
		withCategory("governance"),
		mcp.WithDescription("Evaluate an OPA Rego policy for an authorization decision. Returns whether the action is allowed with reasoning."),
		mcp.WithString("agent", mcp.Required(), mcp.Description("Agent ID to evaluate")),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: read, write, create, delete, admin")),
		mcp.WithString("resource", mcp.Required(), mcp.Description("Resource type: topic, consumer_group, schema, cluster")),
		mcp.WithString("resource_name", mcp.Description("Specific resource name. Default: *")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_policy_evaluate",
		resourceType: "cluster",
		action:       "admin",
	}, s.handlePolicyEvaluate))

	s.addTool(mcp.NewTool("kafka_policy_simulate",
		withCategory("governance"),
		mcp.WithDescription("Simulate a policy evaluation showing how each policy rule matches, without recording the decision."),
		mcp.WithString("agent", mcp.Required(), mcp.Description("Agent ID to simulate")),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action to simulate")),
		mcp.WithString("resource", mcp.Required(), mcp.Description("Resource type")),
		mcp.WithString("resource_name", mcp.Description("Specific resource name")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_policy_simulate",
		resourceType: "cluster",
		action:       "admin",
	}, s.handlePolicySimulate))
}

func (s *Server) handlePolicyEvaluate(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	agent, _ := args["agent"].(string)
	action, _ := args["action"].(string)
	resource, _ := args["resource"].(string)
	resourceName, _ := args["resource_name"].(string)

	if agent == "" || action == "" || resource == "" {
		return mcp.NewToolResultError("agent, action, and resource are required"), nil
	}
	if resourceName == "" {
		resourceName = "*"
	}

	input := opaEngine.PolicyInput{
		Agent:        agent,
		Action:       action,
		Resource:     resource,
		ResourceName: resourceName,
	}

	decision := s.opaPolicyEngine.Evaluate(input)
	s.opaPolicyEngine.RecordDecision(decision)
	s.logAudit(ctx, agentID, "kafka_policy_evaluate", args, "", "success", 1, nil, start)
	return marshalResult(decision)
}

func (s *Server) handlePolicySimulate(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	agent, _ := args["agent"].(string)
	action, _ := args["action"].(string)
	resource, _ := args["resource"].(string)
	resourceName, _ := args["resource_name"].(string)

	if agent == "" || action == "" || resource == "" {
		return mcp.NewToolResultError("agent, action, and resource are required"), nil
	}
	if resourceName == "" {
		resourceName = "*"
	}

	input := opaEngine.PolicyInput{
		Agent:        agent,
		Action:       action,
		Resource:     resource,
		ResourceName: resourceName,
	}

	result := s.opaPolicyEngine.Simulate(input)
	s.logAudit(ctx, agentID, "kafka_policy_simulate", args, "", "success", 1, nil, start)
	return marshalResult(result)
}
