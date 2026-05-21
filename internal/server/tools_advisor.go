package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/advisor"
	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleAdvisorScan(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_advisor_scan", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		cluster := request.GetString("cluster", "")
		severityFilter := request.GetString("severity", "")
		categoryFilter := request.GetString("category", "")

		if deny := validateStringLength("cluster", cluster); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("severity", severityFilter); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("category", categoryFilter); deny != nil {
			return deny, nil
		}

		metrics, err := s.collectTopicMetrics(ctx, cluster)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_advisor_scan", args, cluster, "error", 0, err, start)
			return s.toolError("failed to collect topic metrics", err), nil
		}

		result := s.advisorEngine.Scan(s.resolveClusterName(cluster), metrics)

		// Apply filters
		if severityFilter != "" || categoryFilter != "" {
			var filtered []advisor.Recommendation
			for _, r := range result.Recommendations {
				if severityFilter != "" && r.Severity != severityFilter {
					continue
				}
				if categoryFilter != "" && r.Category != categoryFilter {
					continue
				}
				filtered = append(filtered, r)
			}
			result.Recommendations = filtered
		}

		s.logAudit(ctx, agentID, "kafka_advisor_scan", args, cluster, "success", len(result.Recommendations), nil, start)
		return marshalResult(result)
	})(ctx, request)
}

func (s *Server) handleAdvisorReport(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_advisor_report", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		cluster := request.GetString("cluster", "")

		if deny := validateStringLength("cluster", cluster); deny != nil {
			return deny, nil
		}

		metrics, err := s.collectTopicMetrics(ctx, cluster)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_advisor_report", args, cluster, "error", 0, err, start)
			return s.toolError("failed to collect topic metrics", err), nil
		}

		report := s.advisorEngine.GenerateReport(s.resolveClusterName(cluster), metrics)
		s.logAudit(ctx, agentID, "kafka_advisor_report", args, cluster, "success", 1, nil, start)
		return marshalResult(report)
	})(ctx, request)
}

func (s *Server) collectTopicMetrics(ctx context.Context, cluster string) ([]advisor.TopicMetrics, error) {
	topics, err := s.kafkaManager.ListTopics(ctx, cluster, false)
	if err != nil {
		return nil, err
	}

	groups, _ := s.kafkaManager.ListConsumerGroups(ctx, cluster)
	topicGroups := make(map[string][]string)
	for _, g := range groups {
		detail, err := s.kafkaManager.DescribeConsumerGroup(ctx, cluster, g.GroupID)
		if err != nil {
			continue
		}
		for _, offset := range detail.Offsets {
			topicGroups[offset.Topic] = append(topicGroups[offset.Topic], g.GroupID)
		}
	}

	var metrics []advisor.TopicMetrics
	for _, t := range topics {
		metrics = append(metrics, advisor.TopicMetrics{
			Name:              t.Name,
			Partitions:        t.Partitions,
			ReplicationFactor: t.ReplicationFactor,
			RetentionMs:       t.RetentionMs,
			MessageCount:      t.MessageCount,
			IsInternal:        t.IsInternal,
			ConsumerGroups:    topicGroups[t.Name],
		})
	}
	return metrics, nil
}
