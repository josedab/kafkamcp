package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/controlplane"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleControlPlaneOrg(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_controlplane_org",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "list")
		orgID := request.GetString("org_id", "")

		if deny := validateStringLength("org_id", orgID); deny != nil {
			return deny, nil
		}

		switch action {
		case "create":
			name := request.GetString("name", "")
			plan := request.GetString("plan", "starter")
			if name == "" {
				return mcp.NewToolResultError("name is required"), nil
			}
			if deny := validateStringLength("name", name); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("plan", plan); deny != nil {
				return deny, nil
			}
			org, err := s.controlPlane.CreateOrganization(name, plan)
			if err != nil {
				return s.toolError("Failed to create organization", err), nil
			}
			s.logAudit(agentID, "kafka_controlplane_org", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "created", "organization": org})

		case "get":
			if orgID == "" {
				return mcp.NewToolResultError("org_id is required"), nil
			}
			org, err := s.controlPlane.GetOrganization(orgID)
			if err != nil {
				return s.toolError("Organization not found", err), nil
			}
			s.logAudit(agentID, "kafka_controlplane_org", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"organization": org})

		case "add_member":
			if orgID == "" {
				return mcp.NewToolResultError("org_id is required"), nil
			}
			email := request.GetString("email", "")
			role := request.GetString("role", "viewer")
			if email == "" {
				return mcp.NewToolResultError("email is required"), nil
			}
			if deny := validateStringLength("email", email); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("role", role); deny != nil {
				return deny, nil
			}
			if err := s.controlPlane.AddMember(orgID, email, email, role); err != nil {
				return s.toolError("Failed to add member", err), nil
			}
			s.logAudit(agentID, "kafka_controlplane_org", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "member_added", "email": email, "role": role})

		case "create_api_key":
			if orgID == "" {
				return mcp.NewToolResultError("org_id is required"), nil
			}
			name := request.GetString("name", "default")
			key, err := s.controlPlane.CreateAPIKey(orgID, name, []string{"read", "write"})
			if err != nil {
				return s.toolError("Failed to create API key", err), nil
			}
			s.logAudit(agentID, "kafka_controlplane_org", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "api_key_created", "api_key": key})

		default:
			orgs := s.controlPlane.ListOrganizations()
			s.logAudit(agentID, "kafka_controlplane_org", args, "", "success", len(orgs), nil, start)
			return marshalResult(map[string]any{"organizations": orgs, "total": len(orgs)})
		}
	})(ctx, request)
}

func (s *Server) handleControlPlaneInstance(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_controlplane_instance",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "list")
		instanceID := request.GetString("instance_id", "")
		orgID := request.GetString("org_id", "")

		if deny := validateStringLength("instance_id", instanceID); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("org_id", orgID); deny != nil {
			return deny, nil
		}

		switch action {
		case "provision":
			if orgID == "" {
				return mcp.NewToolResultError("org_id is required"), nil
			}
			name := request.GetString("name", "")
			tier := request.GetString("tier", "standard")
			region := request.GetString("region", "us-east-1")
			bootstrap := request.GetString("bootstrap_servers", "")

			if name == "" || bootstrap == "" {
				return mcp.NewToolResultError("name and bootstrap_servers are required"), nil
			}
			if deny := validateStringLength("name", name); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("tier", tier); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("region", region); deny != nil {
				return deny, nil
			}

			inst, err := s.controlPlane.ProvisionInstance(orgID, name, tier, region, controlplane.InstanceConfig{
				BootstrapServers: bootstrap,
			})
			if err != nil {
				return s.toolError("Failed to provision instance", err), nil
			}
			s.logAudit(agentID, "kafka_controlplane_instance", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "provisioning", "instance": inst})

		case "get":
			if instanceID == "" {
				return mcp.NewToolResultError("instance_id is required"), nil
			}
			inst, err := s.controlPlane.GetInstance(instanceID)
			if err != nil {
				return s.toolError("Instance not found", err), nil
			}
			s.logAudit(agentID, "kafka_controlplane_instance", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"instance": inst})

		case "start":
			if instanceID == "" {
				return mcp.NewToolResultError("instance_id is required"), nil
			}
			if err := s.controlPlane.StartInstance(instanceID); err != nil {
				return s.toolError("Failed to start", err), nil
			}
			s.logAudit(agentID, "kafka_controlplane_instance", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "started", "instance_id": instanceID})

		case "stop":
			if instanceID == "" {
				return mcp.NewToolResultError("instance_id is required"), nil
			}
			if err := s.controlPlane.StopInstance(instanceID); err != nil {
				return s.toolError("Failed to stop", err), nil
			}
			s.logAudit(agentID, "kafka_controlplane_instance", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "stopped", "instance_id": instanceID})

		case "terminate":
			if instanceID == "" {
				return mcp.NewToolResultError("instance_id is required"), nil
			}
			if err := s.controlPlane.TerminateInstance(instanceID); err != nil {
				return s.toolError("Failed to terminate", err), nil
			}
			s.logAudit(agentID, "kafka_controlplane_instance", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "terminated", "instance_id": instanceID})

		default:
			instances := s.controlPlane.ListInstances(orgID)
			s.logAudit(agentID, "kafka_controlplane_instance", args, "", "success", len(instances), nil, start)
			return marshalResult(map[string]any{"instances": instances, "total": len(instances)})
		}
	})(ctx, request)
}

func (s *Server) handleControlPlaneUsage(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_controlplane_usage",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		orgID := request.GetString("org_id", "")
		period := request.GetString("period", "current")

		if orgID == "" {
			return mcp.NewToolResultError("org_id is required"), nil
		}
		if deny := validateStringLength("org_id", orgID); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("period", period); deny != nil {
			return deny, nil
		}

		summary := s.controlPlane.GetUsageSummary(orgID, period)
		s.logAudit(agentID, "kafka_controlplane_usage", args, "", "success", 0, nil, start)
		return marshalResult(summary)
	})(ctx, request)
}
