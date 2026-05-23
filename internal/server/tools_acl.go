package server

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
)

// handleListACLs lists Kafka ACLs with security analysis.
func (s *Server) handleListACLs(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_list_acls", resourceType: auth.ResourceCluster, action: auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		cluster := request.GetString("cluster", "")

		if deny := validateStringLength("cluster", cluster); deny != nil {
			return deny, nil
		}

		principal := request.GetString("principal", "")
		resourceType := request.GetString("resource_type", "")
		if deny := validateStringLength("principal", principal); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("resource_type", resourceType); deny != nil {
			return deny, nil
		}
		includeAnalysis := request.GetBool("include_analysis", true)

		result, err := s.kafkaManager.ListACLs(ctx, cluster, principal, resourceType, includeAnalysis)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_list_acls", args, cluster, "error", 0, err, start)
			return s.toolError("Failed", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_list_acls", args, cluster, "success", result.TotalACLs, nil, start)
		return marshalResult(result)
	})(ctx, request)
}
