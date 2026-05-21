package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/federation"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleFederationJoin(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_federation_join", resourceType: auth.ResourceCluster, action: auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")

		switch action {
		case "join":
			instanceID := request.GetString("instance_id", "")
			name := request.GetString("name", "")
			endpoint := request.GetString("endpoint", "")

			if instanceID == "" || endpoint == "" {
				return mcp.NewToolResultError("instance_id and endpoint are required"), nil
			}
			if deny := validateStringLength("instance_id", instanceID); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("name", name); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("endpoint", endpoint); deny != nil {
				return deny, nil
			}

			var capabilities []string
			if c, ok := args["capabilities"].([]any); ok {
				for _, v := range c {
					if s, ok := v.(string); ok {
						capabilities = append(capabilities, s)
					}
				}
			}

			var clusters []string
			if c, ok := args["clusters"].([]any); ok {
				for _, v := range c {
					if s, ok := v.(string); ok {
						clusters = append(clusters, s)
					}
				}
			}

			inst, err := s.federationCoord.Join(federation.Instance{
				ID:           instanceID,
				Name:         name,
				Endpoint:     endpoint,
				Capabilities: capabilities,
				Clusters:     clusters,
			})
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_federation_join", args, "", "error", 0, err, start)
				return s.toolError("join failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_federation_join", args, "", "success", 1, nil, start)
			return marshalResult(inst)

		case "leave":
			instanceID := request.GetString("instance_id", "")
			if instanceID == "" {
				return mcp.NewToolResultError("instance_id is required"), nil
			}
			if err := s.federationCoord.Leave(instanceID); err != nil {
				return s.toolError("leave failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_federation_join", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "deregistered", "instance_id": instanceID})

		case "heartbeat":
			instanceID := request.GetString("instance_id", "")
			if instanceID == "" {
				return mcp.NewToolResultError("instance_id is required"), nil
			}
			if err := s.federationCoord.Heartbeat(instanceID); err != nil {
				return s.toolError("heartbeat failed", err), nil
			}
			return marshalResult(map[string]any{"status": "ok", "instance_id": instanceID})

		default:
			return mcp.NewToolResultError("action must be: join, leave, heartbeat"), nil
		}
	})(ctx, request)
}

func (s *Server) handleFederationQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_federation_query", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		tool := request.GetString("tool", "")
		if tool == "" {
			return mcp.NewToolResultError("tool is required"), nil
		}

		var toolArgs map[string]any
		if a, ok := args["args"].(map[string]any); ok {
			toolArgs = a
		}

		var targets []string
		if t, ok := args["targets"].([]any); ok {
			for _, v := range t {
				if s, ok := v.(string); ok {
					targets = append(targets, s)
				}
			}
		}

		query := s.federationCoord.Query(tool, toolArgs, targets)
		s.logAudit(ctx, agentID, "kafka_federation_query", args, "", "success", len(query.Results), nil, start)
		return marshalResult(query)
	})(ctx, request)
}

func (s *Server) handleFederationStatus(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_federation_status", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "status")

		switch action {
		case "status":
			status := s.federationCoord.GetStatus()
			s.logAudit(ctx, agentID, "kafka_federation_status", args, "", "success", 1, nil, start)
			return marshalResult(status)

		case "instances":
			instances := s.federationCoord.ListInstances()
			return marshalResult(map[string]any{"instances": instances, "total": len(instances)})

		case "policies":
			policies := s.federationCoord.ListPolicies()
			return marshalResult(map[string]any{"policies": policies, "total": len(policies)})

		case "audit":
			limit := 50
			if l, ok := args["limit"].(float64); ok && l > 0 {
				limit = int(l)
			}
			if limit > MaxDLQMessages {
				limit = MaxDLQMessages
			}
			entries := s.federationCoord.GetAuditLog(limit)
			return marshalResult(map[string]any{"audit_log": entries, "total": len(entries)})

		case "topology":
			topo := s.federationCoord.GetTopology()
			s.logAudit(ctx, agentID, "kafka_federation_status", args, "", "success", 1, nil, start)
			return marshalResult(topo)

		case "discover":
			services := s.federationCoord.DiscoverServices()
			s.logAudit(ctx, agentID, "kafka_federation_status", args, "", "success", len(services), nil, start)
			return marshalResult(map[string]any{"services": services, "total": len(services)})

		case "routes":
			rules := s.federationCoord.ListRoutingRules()
			s.logAudit(ctx, agentID, "kafka_federation_status", args, "", "success", len(rules), nil, start)
			return marshalResult(map[string]any{"routing_rules": rules, "total": len(rules)})

		default:
			return mcp.NewToolResultError("action must be: status, instances, policies, audit, topology, discover, or routes"), nil
		}
	})(ctx, request)
}

func (s *Server) handleFederationRoute(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_federation_route", resourceType: auth.ResourceCluster, action: auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")

		switch action {
		case "add":
			toolMatch, _ := args["tool_match"].(string)
			cluster, _ := args["cluster"].(string)
			priority := 0
			if p, ok := args["priority"].(float64); ok {
				priority = int(p)
			}

			var targets []string
			if t, ok := args["targets"].([]any); ok {
				for _, v := range t {
					if str, ok := v.(string); ok {
						targets = append(targets, str)
					}
				}
			}

			rule, err := s.federationCoord.AddRoutingRule(federation.RoutingRule{
				ToolMatch: toolMatch,
				Cluster:   cluster,
				Targets:   targets,
				Priority:  priority,
			})
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_federation_route", args, "", "error", 0, err, start)
				return s.toolError("failed to add routing rule", err), nil
			}

			s.logAudit(ctx, agentID, "kafka_federation_route", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "created", "rule": rule})

		case "list":
			rules := s.federationCoord.ListRoutingRules()
			s.logAudit(ctx, agentID, "kafka_federation_route", args, "", "success", len(rules), nil, start)
			return marshalResult(map[string]any{"routing_rules": rules, "total": len(rules)})

		case "resolve":
			toolMatch, _ := args["tool_match"].(string)
			cluster, _ := args["cluster"].(string)
			instances := s.federationCoord.ResolveRoute(toolMatch, cluster)
			s.logAudit(ctx, agentID, "kafka_federation_route", args, "", "success", len(instances), nil, start)
			return marshalResult(map[string]any{"resolved_instances": instances, "total": len(instances)})

		default:
			return mcp.NewToolResultError("action must be: add, list, or resolve"), nil
		}
	})(ctx, request)
}
