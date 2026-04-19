package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/cost"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/mark3labs/mcp-go/mcp"
)

// ===================== COST ANALYSIS HANDLERS =====================

// handleCostAnalyze performs cost analysis for a Kafka topic.
func (s *Server) handleCostAnalyze(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_cost_analyze",
		resourceType: auth.ResourceTopic,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		topic := request.GetString("topic", "")
		cluster := request.GetString("cluster", "")
		provider := request.GetString("provider", "aws-msk")

		if topic == "" {
			return mcp.NewToolResultError("topic is required"), nil
		}
		if deny := validateStringLength("topic", topic); deny != nil {
			return deny, nil
		}

		detail, err := s.kafkaManager.DescribeTopic(ctx, cluster, topic)
		if err != nil {
			s.logAudit(agentID, "kafka_cost_analyze", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed to describe topic: %v", err)), nil
		}

		retentionMs := int64(604800000) // default 7 days
		if ret, ok := detail.Configs["retention.ms"]; ok {
			fmt.Sscanf(ret, "%d", &retentionMs)
		}

		input := cost.TopicCostInput{
			Name:              topic,
			Partitions:        len(detail.Partitions),
			ReplicationFactor: detail.ReplicationFactor,
			RetentionMs:       retentionMs,
			AvgMsgSizeBytes:   500,
			MsgsPerSecond:     estimateThroughput(detail),
		}

		result, err := s.costAnalyzer.AnalyzeTopic(input, provider)
		if err != nil {
			s.logAudit(agentID, "kafka_cost_analyze", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed to analyze cost: %v", err)), nil
		}

		s.logAudit(agentID, "kafka_cost_analyze", args, cluster, "success", 1, nil, start)
		return marshalResult(result)
	})(ctx, request)
}

// handleCostWaste detects waste across all topics.
func (s *Server) handleCostWaste(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_cost_waste",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		cluster := request.GetString("cluster", "")
		provider := request.GetString("provider", "aws-msk")

		topics, err := s.kafkaManager.ListTopics(ctx, cluster, false)
		if err != nil {
			s.logAudit(agentID, "kafka_cost_waste", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed to list topics: %v", err)), nil
		}

		var inputs []cost.TopicCostInput
		for _, t := range topics {
			inputs = append(inputs, cost.TopicCostInput{
				Name:              t.Name,
				Partitions:        t.Partitions,
				ReplicationFactor: t.ReplicationFactor,
				RetentionMs:       t.RetentionMs,
				AvgMsgSizeBytes:   500,
				MsgsPerSecond:     float64(t.MessageCount) / 3600,
			})
		}

		report, err := s.costAnalyzer.DetectWaste(inputs, map[string]time.Time{}, provider)
		if err != nil {
			s.logAudit(agentID, "kafka_cost_waste", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed to detect waste: %v", err)), nil
		}

		s.logAudit(agentID, "kafka_cost_waste", args, cluster, "success", len(topics), nil, start)
		return marshalResult(report)
	})(ctx, request)
}

// handleCostWhatIf simulates configuration changes and returns cost impact.
func (s *Server) handleCostWhatIf(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_cost_whatif",
		resourceType: auth.ResourceTopic,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		topic := request.GetString("topic", "")
		cluster := request.GetString("cluster", "")
		provider := request.GetString("provider", "aws-msk")

		if topic == "" {
			return mcp.NewToolResultError("topic is required"), nil
		}

		detail, err := s.kafkaManager.DescribeTopic(ctx, cluster, topic)
		if err != nil {
			s.logAudit(agentID, "kafka_cost_whatif", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed to describe topic: %v", err)), nil
		}

		retentionMs := int64(604800000)
		if ret, ok := detail.Configs["retention.ms"]; ok {
			fmt.Sscanf(ret, "%d", &retentionMs)
		}

		current := cost.TopicCostInput{
			Name:              topic,
			Partitions:        len(detail.Partitions),
			ReplicationFactor: detail.ReplicationFactor,
			RetentionMs:       retentionMs,
			AvgMsgSizeBytes:   500,
			MsgsPerSecond:     estimateThroughput(detail),
		}

		scenario := cost.WhatIfScenario{Topic: topic}
		if newRet := int64(request.GetInt("new_retention_ms", 0)); newRet > 0 {
			scenario.NewRetentionMs = &newRet
		}
		if newComp := request.GetString("new_compression", ""); newComp != "" {
			scenario.NewCompression = newComp
		}
		if newPart := request.GetInt("new_partitions", 0); newPart > 0 {
			scenario.NewPartitions = &newPart
		}
		if newRF := request.GetInt("new_replication_factor", 0); newRF > 0 {
			scenario.NewReplication = &newRF
		}

		result, err := s.costAnalyzer.WhatIf(current, scenario, provider)
		if err != nil {
			s.logAudit(agentID, "kafka_cost_whatif", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed to simulate: %v", err)), nil
		}

		s.logAudit(agentID, "kafka_cost_whatif", args, cluster, "success", 1, nil, start)
		return marshalResult(result)
	})(ctx, request)
}

func estimateThroughput(detail *kafka.TopicDetail) float64 {
	var totalMsgs int64
	for _, p := range detail.Partitions {
		totalMsgs += p.LatestOffset - p.EarliestOffset
	}
	if totalMsgs <= 0 {
		return 1
	}
	return float64(totalMsgs) / 3600
}
