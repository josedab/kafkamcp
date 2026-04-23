package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/lagdiag"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleDiagnoseLag(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_diagnose_lag", resourceType: auth.ResourceConsumerGroup, action: auth.ActionRead,
		resourceName: func(r mcp.CallToolRequest) string { return r.GetString("group_id", "") },
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		groupID := request.GetString("group_id", "")
		cluster := request.GetString("cluster", "")

		if groupID == "" {
			return mcp.NewToolResultError("group_id is required"), nil
		}
		if deny := validateStringLength("group_id", groupID); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("cluster", cluster); deny != nil {
			return deny, nil
		}

		// Get current group details for lag info
		groupDetail, err := s.kafkaManager.DescribeConsumerGroup(ctx, cluster, groupID)
		if err != nil {
			s.logAudit(agentID, "kafka_diagnose_lag", args, cluster, "error", 0, err, start)
			return s.toolError("failed to describe consumer group", err), nil
		}

		// Build partition lag snapshots
		var partitionLags []lagdiag.LagSnapshot
		for _, offset := range groupDetail.Offsets {
			partitionLags = append(partitionLags, lagdiag.LagSnapshot{
				GroupID:        groupID,
				Topic:          offset.Topic,
				Partition:      offset.Partition,
				Lag:            offset.Lag,
				ConsumerOffset: offset.CurrentOffset,
				LogEndOffset:   offset.LogEndOffset,
			})
		}

		diagnosis := s.lagAnalyzer.Diagnose(groupID, groupDetail.TotalLag, partitionLags)

		s.logAudit(agentID, "kafka_diagnose_lag", args, cluster, "success", 1, nil, start)
		return marshalResult(diagnosis)
	})(ctx, request)
}
