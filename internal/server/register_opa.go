package server

import (
	"context"
	"time"

	opaEngine "github.com/kafkamcp/kafkamcp/internal/opa"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerOPATools() {
	if s.opaPolicyEngine == nil {
		return
	}

	s.mcpServer.AddTool(mcp.NewTool("kafka_policy_evaluate",
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

	s.mcpServer.AddTool(mcp.NewTool("kafka_policy_simulate",
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

	s.mcpServer.AddTool(mcp.NewTool("kafka_policy_manage",
		mcp.WithDescription("Manage OPA Rego policies. Actions: list (show policies), register (add/update), delete (remove), export (JSON dump), decisions (recent log)."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: list, register, delete, export, decisions")),
		mcp.WithString("policy_id", mcp.Description("Policy ID (for register/delete)")),
		mcp.WithString("name", mcp.Description("Policy name (for register)")),
		mcp.WithString("rego", mcp.Description("Rego policy rules (for register)")),
		mcp.WithNumber("limit", mcp.Description("Max decisions to return. Default: 50")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_policy_manage",
		resourceType: "cluster",
		action:       "admin",
	}, s.handlePolicyManage))
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
	s.logAudit(agentID, "kafka_policy_evaluate", args, "", "success", 1, nil, start)
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
	s.logAudit(agentID, "kafka_policy_simulate", args, "", "success", 1, nil, start)
	return marshalResult(result)
}

func (s *Server) handlePolicyManage(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	action, _ := args["action"].(string)

	switch action {
	case "list":
		policies := s.opaPolicyEngine.ListPolicies()
		s.logAudit(agentID, "kafka_policy_manage", args, "", "success", len(policies), nil, start)
		return marshalResult(map[string]any{"policies": policies, "total": len(policies)})

	case "register":
		policyID, _ := args["policy_id"].(string)
		name, _ := args["name"].(string)
		rego, _ := args["rego"].(string)

		if policyID == "" || rego == "" {
			return mcp.NewToolResultError("policy_id and rego are required"), nil
		}

		err := s.opaPolicyEngine.RegisterPolicy(opaEngine.Policy{
			ID:      policyID,
			Name:    name,
			Rego:    rego,
			Enabled: true,
		})
		if err != nil {
			return s.toolError("failed to register policy", err), nil
		}
		s.logAudit(agentID, "kafka_policy_manage", args, "", "success", 1, nil, start)
		return marshalResult(map[string]string{"status": "registered", "policy_id": policyID})

	case "delete":
		policyID, _ := args["policy_id"].(string)
		if policyID == "" {
			return mcp.NewToolResultError("policy_id is required"), nil
		}
		err := s.opaPolicyEngine.DeletePolicy(policyID)
		if err != nil {
			return s.toolError("failed to delete policy", err), nil
		}
		s.logAudit(agentID, "kafka_policy_manage", args, "", "success", 1, nil, start)
		return marshalResult(map[string]string{"status": "deleted", "policy_id": policyID})

	case "export":
		exported, err := s.opaPolicyEngine.ExportPolicies()
		if err != nil {
			return s.toolError("failed to export policies", err), nil
		}
		s.logAudit(agentID, "kafka_policy_manage", args, "", "success", 1, nil, start)
		return mcp.NewToolResultText(exported), nil

	case "decisions":
		limit := 50
		if l, ok := args["limit"].(float64); ok && l > 0 {
			limit = int(l)
		}
		if limit > 1000 {
			limit = 1000
		}
		decisions := s.opaPolicyEngine.RecentDecisions(limit)
		s.logAudit(agentID, "kafka_policy_manage", args, "", "success", len(decisions), nil, start)
		return marshalResult(map[string]any{"decisions": decisions, "total": len(decisions)})

	default:
		return mcp.NewToolResultError("action must be: list, register, delete, export, or decisions"), nil
	}
}
