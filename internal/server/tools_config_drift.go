package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

// handleConfigDrift detects configuration drift across clusters.
func (s *Server) handleConfigDrift(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_config_drift", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "compare")
		cluster := request.GetString("cluster", "")

		if deny := validateStringLength("cluster", cluster); deny != nil {
			return deny, nil
		}

		switch action {
		case "compare":
			sourceCluster := request.GetString("source_cluster", "")
			targetCluster := request.GetString("target_cluster", "")
			topic := request.GetString("topic", "")
			if sourceCluster == "" || targetCluster == "" {
				return mcp.NewToolResultError("source_cluster and target_cluster are required for compare"), nil
			}
			if deny := validateStringLength("source_cluster", sourceCluster); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("target_cluster", targetCluster); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("topic", topic); deny != nil {
				return deny, nil
			}
			result, err := s.kafkaManager.CompareConfigs(ctx, sourceCluster, targetCluster, topic)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_config_drift", args, cluster, "error", 0, err, start)
				return s.toolError("Failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_config_drift", args, cluster, "success", len(result.Diffs), nil, start)
			return marshalResult(result)

		case "audit":
			topic := request.GetString("topic", "")
			if deny := validateStringLength("topic", topic); deny != nil {
				return deny, nil
			}
			template := extractStringMapArg(request, "template")
			result, err := s.kafkaManager.AuditConfigs(ctx, cluster, topic, template)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_config_drift", args, cluster, "error", 0, err, start)
				return s.toolError("Failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_config_drift", args, cluster, "success", len(result.Deviations), nil, start)
			return marshalResult(result)

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; must be: compare, audit", action)), nil
		}
	})(ctx, request)
}
