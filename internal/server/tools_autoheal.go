package server

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
)

func (s *Server) handleGroupHeal(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_group_heal",
		resourceType: auth.ResourceConsumerGroup,
		action:       auth.ActionWrite,
		resourceName: func(r mcp.CallToolRequest) string { return r.GetString("group_id", "") },
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		groupID := request.GetString("group_id", "")
		autoExecute := request.GetBool("auto_execute", false)
		cluster := request.GetString("cluster", "")

		if groupID == "" {
			return mcp.NewToolResultError("group_id is required"), nil
		}
		if deny := validateStringLength("group_id", groupID); deny != nil {
			return deny, nil
		}

		resolvedCluster := s.resolveClusterName(cluster)

		detail, err := s.kafkaManager.DescribeConsumerGroup(ctx, resolvedCluster, groupID)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_group_heal", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to describe consumer group", err), nil
		}

		totalLag := detail.TotalLag
		memberCount := len(detail.Members)

		// Estimate commit rate from health history lag trend
		var commitRate float64
		history := s.selfHealMon.GetHealthHistory(groupID)
		if len(history) >= 2 {
			prev := history[len(history)-1]
			if prev.Metrics.TotalLag > totalLag {
				commitRate = float64(prev.Metrics.TotalLag-totalLag) / 10.0
			} else {
				commitRate = 0.1
			}
		} else {
			commitRate = 1.0
		}

		// Estimate rebalance count from group state
		rebalanceCount := 0
		if detail.State == "PreparingRebalance" || detail.State == "CompletingRebalance" {
			rebalanceCount = 5
		}

		healResult := s.selfHealMon.Heal(groupID, totalLag, memberCount, commitRate, rebalanceCount, autoExecute)

		s.logAudit(ctx, agentID, "kafka_group_heal", args, cluster, "success", 0, nil, start)
		return marshalResult(healResult)
	})(ctx, request)
}
