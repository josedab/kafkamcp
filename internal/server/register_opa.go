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

	s.addTool(mcp.NewTool("kafka_policy_manage",
		withCategory("governance"),
		mcp.WithDescription("Manage OPA Rego policies. Actions: list (show policies), register (add/update), delete (remove), export (JSON dump), decisions (recent log), validate (check syntax), impact (predict impact), bundles (list bundles), create_bundle (group policies), enable_bundle, disable_bundle."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: list, register, delete, export, decisions, validate, impact, bundles, create_bundle, enable_bundle, disable_bundle")),
		mcp.WithString("policy_id", mcp.Description("Policy ID (for register/delete/impact)")),
		mcp.WithString("name", mcp.Description("Policy or bundle name (for register/create_bundle)")),
		mcp.WithString("rego", mcp.Description("Rego policy rules (for register/validate/impact)")),
		mcp.WithNumber("limit", mcp.Description("Max decisions to return. Default: 50")),
		mcp.WithString("bundle_id", mcp.Description("Bundle ID (for create_bundle/enable_bundle/disable_bundle)")),
		mcp.WithString("version", mcp.Description("Bundle version (for create_bundle)")),
		mcp.WithObject("policy_ids", mcp.Description("List of policy IDs (for create_bundle)")),
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

	case "validate":
		rego, _ := args["rego"].(string)
		if rego == "" {
			return mcp.NewToolResultError("rego is required for validate"), nil
		}
		validation := s.opaPolicyEngine.ValidatePolicy(rego)
		s.logAudit(agentID, "kafka_policy_manage", args, "", "success", 1, nil, start)
		return marshalResult(map[string]any{"validation": validation})

	case "impact":
		policyID, _ := args["policy_id"].(string)
		rego, _ := args["rego"].(string)
		if rego == "" {
			return mcp.NewToolResultError("rego is required for impact prediction"), nil
		}
		impact := s.opaPolicyEngine.PredictImpact(policyID, rego)
		s.logAudit(agentID, "kafka_policy_manage", args, "", "success", 1, nil, start)
		return marshalResult(map[string]any{"impact": impact})

	case "bundles":
		bundles := s.opaPolicyEngine.ListBundles()
		s.logAudit(agentID, "kafka_policy_manage", args, "", "success", len(bundles), nil, start)
		return marshalResult(map[string]any{"bundles": bundles, "total": len(bundles)})

	case "create_bundle":
		bundleID, _ := args["bundle_id"].(string)
		name, _ := args["name"].(string)
		version, _ := args["version"].(string)
		if bundleID == "" || name == "" {
			return mcp.NewToolResultError("bundle_id and name are required"), nil
		}
		var policyIDs []string
		if ids, ok := args["policy_ids"].([]any); ok {
			for _, id := range ids {
				if s, ok := id.(string); ok {
					policyIDs = append(policyIDs, s)
				}
			}
		}
		bundle, err := s.opaPolicyEngine.RegisterBundle(bundleID, name, version, policyIDs)
		if err != nil {
			return s.toolError("failed to create bundle", err), nil
		}
		s.logAudit(agentID, "kafka_policy_manage", args, "", "success", 1, nil, start)
		return marshalResult(map[string]any{"status": "created", "bundle": bundle})

	case "enable_bundle":
		bundleID, _ := args["bundle_id"].(string)
		if bundleID == "" {
			return mcp.NewToolResultError("bundle_id is required"), nil
		}
		if err := s.opaPolicyEngine.EnableBundle(bundleID); err != nil {
			return s.toolError("failed to enable bundle", err), nil
		}
		s.logAudit(agentID, "kafka_policy_manage", args, "", "success", 1, nil, start)
		return marshalResult(map[string]string{"status": "enabled", "bundle_id": bundleID})

	case "disable_bundle":
		bundleID, _ := args["bundle_id"].(string)
		if bundleID == "" {
			return mcp.NewToolResultError("bundle_id is required"), nil
		}
		if err := s.opaPolicyEngine.DisableBundle(bundleID); err != nil {
			return s.toolError("failed to disable bundle", err), nil
		}
		s.logAudit(agentID, "kafka_policy_manage", args, "", "success", 1, nil, start)
		return marshalResult(map[string]string{"status": "disabled", "bundle_id": bundleID})

	default:
		return mcp.NewToolResultError("action must be: list, register, delete, export, decisions, validate, impact, bundles, create_bundle, enable_bundle, or disable_bundle"), nil
	}
}
