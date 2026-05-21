package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleReplicationStatusTool(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_replication_status",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		status := s.replicationMon.GetStatus(ctx)
		s.logAudit(ctx, agentID, "kafka_replication_status", args, "", "success", len(status.Links), nil, start)
		return marshalResult(status)
	})(ctx, request)
}

func (s *Server) handleReplicationLag(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_replication_lag",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		source := request.GetString("source_cluster", "")
		target := request.GetString("target_cluster", "")
		topicFilter := request.GetString("topic", "")

		if source == "" || target == "" {
			return mcp.NewToolResultError("source_cluster and target_cluster are required"), nil
		}
		if deny := validateStringLength("source_cluster", source); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("target_cluster", target); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("topic", topicFilter); deny != nil {
			return deny, nil
		}

		topics, err := s.replicationMon.GetLagDetails(source, target)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_replication_lag", args, "", "error", 0, err, start)
			return s.toolError("Failed to get replication lag", err), nil
		}

		if topicFilter != "" {
			var filtered []any
			for _, t := range topics {
				if t.Topic == topicFilter {
					filtered = append(filtered, t)
				}
			}
			s.logAudit(ctx, agentID, "kafka_replication_lag", args, "", "success", len(filtered), nil, start)
			return marshalResult(map[string]any{"source": source, "target": target, "topics": filtered})
		}

		s.logAudit(ctx, agentID, "kafka_replication_lag", args, "", "success", len(topics), nil, start)
		return marshalResult(map[string]any{"source": source, "target": target, "topics": topics, "total_topics": len(topics)})
	})(ctx, request)
}

func (s *Server) handleFailoverReadiness(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_failover_readiness",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		source := request.GetString("source_cluster", "")
		target := request.GetString("target_cluster", "")

		if source == "" || target == "" {
			return mcp.NewToolResultError("source_cluster and target_cluster are required"), nil
		}
		if deny := validateStringLength("source_cluster", source); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("target_cluster", target); deny != nil {
			return deny, nil
		}

		readiness, err := s.replicationMon.EvaluateFailoverReadiness(source, target)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_failover_readiness", args, "", "error", 0, err, start)
			return s.toolError("Failed to evaluate failover readiness", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_failover_readiness", args, "", "success", 0, nil, start)
		return marshalResult(readiness)
	})(ctx, request)
}
