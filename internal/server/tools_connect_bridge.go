package server

import (
	"context"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
)

func (s *Server) handleConnectBridge(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_connect_bridge", resourceType: auth.ResourceCluster, action: auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		name := request.GetString("name", "")
		cluster := request.GetString("cluster", "")

		if action == "" {
			return mcp.NewToolResultError("action is required"), nil
		}

		if action != "list" && name == "" {
			return mcp.NewToolResultError("name is required for action: " + action), nil
		}
		if name != "" {
			if deny := validateStringLength("name", name); deny != nil {
				return deny, nil
			}
		}

		clCfg, err := s.kafkaManager.GetClusterConfig(cluster)
		if err != nil {
			return s.toolError("Cluster error", err), nil
		}

		client, err := s.getConnectClient(clCfg)
		if err != nil || client == nil {
			if err != nil {
				s.logger.Warn("Kafka Connect client error", "error", err)
			}
			return mcp.NewToolResultError("Kafka Connect is not available"), nil
		}

		switch action {
		case "list":
			connectors, err := client.ListConnectors(ctx)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_connect_bridge", args, cluster, "error", 0, err, start)
				return s.toolError("Failed to list connectors", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_connect_bridge", args, cluster, "success", len(connectors), nil, start)
			return marshalResult(map[string]any{"connectors": connectors, "total": len(connectors)})

		case "describe":
			detail, err := client.DescribeConnector(ctx, name)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_connect_bridge", args, cluster, "error", 0, err, start)
				return s.toolError("Failed to describe connector", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_connect_bridge", args, cluster, "success", 1, nil, start)
			return marshalResult(detail)

		case "create":
			config := extractStringMapArg(request, "config")
			if len(config) == 0 {
				return mcp.NewToolResultError("config is required for create"), nil
			}
			if deny, _ := validateConnectorConfig(config); deny != nil {
				return deny, nil
			}
			result, err := client.CreateConnector(ctx, name, config)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_connect_bridge", args, cluster, "error", 0, err, start)
				return s.toolError("Failed to create connector", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_connect_bridge", args, cluster, "success", 1, nil, start)
			return marshalResult(result)

		case "update":
			config := extractStringMapArg(request, "config")
			if len(config) == 0 {
				return mcp.NewToolResultError("config is required for update"), nil
			}
			if deny, _ := validateConnectorConfig(config); deny != nil {
				return deny, nil
			}
			result, err := client.UpdateConnector(ctx, name, config)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_connect_bridge", args, cluster, "error", 0, err, start)
				return s.toolError("Failed to update connector", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_connect_bridge", args, cluster, "success", 1, nil, start)
			return marshalResult(result)

		case "pause":
			if err := client.PauseConnector(ctx, name); err != nil {
				s.logAudit(ctx, agentID, "kafka_connect_bridge", args, cluster, "error", 0, err, start)
				return s.toolError("Failed to pause connector", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_connect_bridge", args, cluster, "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "paused", "name": name})

		case "resume":
			if err := client.ResumeConnector(ctx, name); err != nil {
				s.logAudit(ctx, agentID, "kafka_connect_bridge", args, cluster, "error", 0, err, start)
				return s.toolError("Failed to resume connector", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_connect_bridge", args, cluster, "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "resumed", "name": name})

		case "restart":
			taskID := request.GetInt("task_id", -1)
			failedOnly := request.GetBool("failed_only", false)
			result, err := client.RestartConnector(ctx, name, taskID, failedOnly)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_connect_bridge", args, cluster, "error", 0, err, start)
				return s.toolError("Failed to restart connector", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_connect_bridge", args, cluster, "success", 1, nil, start)
			return marshalResult(result)

		case "delete":
			if err := client.DeleteConnector(ctx, name); err != nil {
				s.logAudit(ctx, agentID, "kafka_connect_bridge", args, cluster, "error", 0, err, start)
				return s.toolError("Failed to delete connector", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_connect_bridge", args, cluster, "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "deleted", "name": name})

		case "health":
			health, err := client.ConnectorHealth(ctx, name)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_connect_bridge", args, cluster, "error", 0, err, start)
				return s.toolError("Failed to get connector health", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_connect_bridge", args, cluster, "success", 1, nil, start)
			return marshalResult(health)

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q, use: list, describe, create, update, pause, resume, restart, delete, health", action)), nil
		}
	})(ctx, request)
}
