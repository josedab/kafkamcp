package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/anomaly"
	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/cost"
	"github.com/kafkamcp/kafkamcp/internal/gitops"
	"github.com/kafkamcp/kafkamcp/internal/graph"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/kafkamcp/kafkamcp/internal/nlquery"
	"github.com/mark3labs/mcp-go/mcp"
)

// ===================== FEATURE TOOL HANDLERS =====================

// handleAnomalies returns detected anomalies from the anomaly detection engine.
func (s *Server) handleAnomalies(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_anomalies",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		if s.anomalyDetector == nil {
			return mcp.NewToolResultError("Anomaly detection is not configured"), nil
		}

		severity := request.GetString("severity", "")
		metric := request.GetString("metric", "")
		limit := request.GetInt("limit", 50)
		sinceHours := request.GetInt("since_hours", 24)

		since := time.Now().Add(-time.Duration(sinceHours) * time.Hour)
		alerts := s.anomalyDetector.GetAlerts(since, severity, metric, limit)
		status := s.anomalyDetector.GetStatus()

		s.logAudit(agentID, "kafka_anomalies", args, "", "success", len(alerts), nil, start)
		return marshalResult(map[string]any{
			"alerts":      alerts,
			"alert_count": len(alerts),
			"status":      status,
		})
	})(ctx, request)
}

// handleIngestAnomaly allows manual data point ingestion for anomaly detection.
func (s *Server) handleIngestAnomaly(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_anomaly_ingest",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		if s.anomalyDetector == nil {
			return mcp.NewToolResultError("Anomaly detection is not configured"), nil
		}

		metric := request.GetString("metric", "")
		source := request.GetString("source", "")
		value := request.GetFloat("value", 0)

		if metric == "" || source == "" {
			return mcp.NewToolResultError("metric and source are required"), nil
		}

		dp := anomaly.DataPoint{
			Timestamp: time.Now(),
			Value:     value,
			Metric:    anomaly.MetricType(metric),
			Source:    source,
		}
		s.anomalyDetector.IngestPoint(dp)

		s.logAudit(agentID, "kafka_anomaly_ingest", args, "", "success", 1, nil, start)
		return marshalResult(map[string]any{"status": "ingested", "data_point": dp})
	})(ctx, request)
}

// handleNLQuery processes natural language queries against Kafka topics.
func (s *Server) handleNLQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_nl_query",
		resourceType: auth.ResourceTopic,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		query := request.GetString("query", "")
		cluster := request.GetString("cluster", "")

		if query == "" {
			return mcp.NewToolResultError("query is required"), nil
		}
		if deny := validateStringLength("query", query); deny != nil {
			return deny, nil
		}

		// Get available topics for context
		topics, err := s.kafkaManager.ListTopics(ctx, cluster, false)
		if err != nil {
			s.logAudit(agentID, "kafka_nl_query", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed to list topics: %v", err)), nil
		}

		topicNames := make([]string, len(topics))
		for i, t := range topics {
			topicNames[i] = t.Name
		}

		plan, err := s.nlQueryEngine.Parse(query, topicNames)
		if err != nil {
			s.logAudit(agentID, "kafka_nl_query", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed to parse query: %v", err)), nil
		}

		// Execute the query plan against matching topics
		var results []map[string]any
		messagesScanned := 0
		for _, topic := range plan.Topics {
			msgs, err := s.kafkaManager.Consume(ctx, cluster, topic, -1, "latest", 100, 5000, "auto")
			if err != nil {
				continue
			}
			messagesScanned += len(msgs)

			for _, msg := range msgs {
				record := map[string]any{
					"topic":     topic,
					"partition": msg.Partition,
					"offset":    msg.Offset,
					"timestamp": msg.Timestamp,
					"key":       msg.Key,
					"value":     msg.Value,
				}

				if matchesFilters(msg, plan.Filters) {
					results = append(results, record)
				}
			}
		}

		if plan.Limit > 0 && len(results) > plan.Limit {
			results = results[:plan.Limit]
		}

		queryResult := nlquery.QueryResult{
			Plan:       *plan,
			Data:       results,
			TotalCount: len(results),
			Provenance: nlquery.Provenance{
				TopicsScanned:   plan.Topics,
				MessagesScanned: messagesScanned,
				MessagesMatched: len(results),
				FiltersApplied:  len(plan.Filters),
			},
			Duration: time.Since(start).String(),
		}

		s.logAudit(agentID, "kafka_nl_query", args, cluster, "success", len(results), nil, start)
		return marshalResult(queryResult)
	})(ctx, request)
}

// matchesFilters checks if a message matches all filters.
func matchesFilters(msg kafka.Message, filters []nlquery.Filter) bool {
	if len(filters) == 0 {
		return true
	}

	for _, f := range filters {
		val := extractFieldValue(msg.Value, f.Field)
		if val == nil {
			return false
		}
		valStr := fmt.Sprintf("%v", val)
		switch f.Operator {
		case "eq":
			if valStr != f.Value {
				return false
			}
		case "neq":
			if valStr == f.Value {
				return false
			}
		case "contains":
			if !containsStr(valStr, f.Value) {
				return false
			}
		}
	}
	return true
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && searchString(s, substr)))
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func extractFieldValue(value any, field string) any {
	if field == "" {
		return value
	}
	m, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	return m[field]
}

// handleCostAnalyze performs cost analysis on Kafka topics.
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

// handleWorkflows manages workflow template operations.
func (s *Server) handleWorkflows(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_workflows",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "list")
		category := request.GetString("category", "")
		templateName := request.GetString("template", "")

		switch action {
		case "list":
			templates := s.workflowEngine.ListTemplates(category)
			s.logAudit(agentID, "kafka_workflows", args, "", "success", len(templates), nil, start)
			return marshalResult(map[string]any{"templates": templates, "total_count": len(templates)})

		case "describe":
			if templateName == "" {
				return mcp.NewToolResultError("template name is required for describe"), nil
			}
			tmpl, err := s.workflowEngine.GetTemplate(templateName)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return marshalResult(tmpl)

		case "execute":
			if templateName == "" {
				return mcp.NewToolResultError("template name is required for execute"), nil
			}
			params := make(map[string]any)
			if p, ok := args["params"].(map[string]any); ok {
				params = p
			}
			// Also check for common params passed directly
			for _, key := range []string{"topic", "group_id", "subject", "change", "correlation_id", "source_cluster", "target_cluster"} {
				if v := request.GetString(key, ""); v != "" {
					params[key] = v
				}
			}

			exec, err := s.workflowEngine.StartExecution(templateName, params)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}

			tmpl, _ := s.workflowEngine.GetTemplate(templateName)
			s.logAudit(agentID, "kafka_workflows", args, "", "success", exec.TotalSteps, nil, start)
			return marshalResult(map[string]any{
				"execution":   exec,
				"next_steps":  tmpl.Steps,
				"instruction": "Execute each step using the specified tool with the resolved arguments. Report results back via kafka_workflow_step.",
			})

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; use: list, describe, execute", action)), nil
		}
	})(ctx, request)
}

// handleGitOpsDiff computes diff between desired state and live cluster.
func (s *Server) handleGitOpsDiff(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_gitops_diff",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		cluster := request.GetString("cluster", "")

		// Get live state from cluster
		topics, err := s.kafkaManager.ListTopics(ctx, cluster, false)
		if err != nil {
			s.logAudit(agentID, "kafka_gitops_diff", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed to list topics: %v", err)), nil
		}

		live := gitops.LiveState{Topics: make(map[string]gitops.TopicSpec)}
		for _, t := range topics {
			detail, err := s.kafkaManager.DescribeTopic(ctx, cluster, t.Name)
			if err != nil {
				continue
			}
			live.Topics[t.Name] = gitops.TopicSpec{
				Name:              t.Name,
				Partitions:        t.Partitions,
				ReplicationFactor: t.ReplicationFactor,
				Configs:           detail.Configs,
			}
		}

		// Parse desired state from request
		desired := gitops.DesiredState{Version: "1"}
		if desiredTopics, ok := args["topics"].([]any); ok {
			for _, dt := range desiredTopics {
				if tm, ok := dt.(map[string]any); ok {
					spec := gitops.TopicSpec{
						Name:              fmt.Sprintf("%v", tm["name"]),
						Partitions:        intFromAny(tm["partitions"], 1),
						ReplicationFactor: intFromAny(tm["replication_factor"], 1),
					}
					if configs, ok := tm["configs"].(map[string]any); ok {
						spec.Configs = make(map[string]string)
						for k, v := range configs {
							spec.Configs[k] = fmt.Sprintf("%v", v)
						}
					}
					desired.Topics = append(desired.Topics, spec)
				}
			}
		}

		if errs := s.gitopsManager.ValidateDesiredState(desired); len(errs) > 0 {
			return mcp.NewToolResultError(fmt.Sprintf("Validation errors: %v", errs)), nil
		}

		diff, err := s.gitopsManager.Diff(desired, live)
		if err != nil {
			s.logAudit(agentID, "kafka_gitops_diff", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed to compute diff: %v", err)), nil
		}

		plan := s.gitopsManager.GeneratePlan(diff, true)

		s.logAudit(agentID, "kafka_gitops_diff", args, cluster, "success", diff.TotalChanges, nil, start)
		return marshalResult(map[string]any{
			"diff":    diff,
			"plan":    plan,
			"summary": diff.Summary,
		})
	})(ctx, request)
}

// handleKnowledgeGraphQuery queries the embedded knowledge graph.
func (s *Server) handleKnowledgeGraphQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_knowledge_graph",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		queryStr := request.GetString("query", "")
		if queryStr == "" {
			return mcp.NewToolResultError("query is required"), nil
		}
		if deny := validateStringLength("query", queryStr); deny != nil {
			return deny, nil
		}

		// Auto-discover if graph is empty
		stats := s.knowledgeGraph.GetStats()
		if stats["total_nodes"].(int) == 0 {
			if err := s.refreshKnowledgeGraph(ctx, request.GetString("cluster", "")); err != nil {
				s.logger.Warn("failed to refresh knowledge graph", "error", err)
			}
		}

		result, err := s.knowledgeGraph.Query(queryStr)
		if err != nil {
			s.logAudit(agentID, "kafka_knowledge_graph", args, "", "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Query failed: %v", err)), nil
		}

		s.logAudit(agentID, "kafka_knowledge_graph", args, "", "success", result.TotalNodes, nil, start)
		return marshalResult(result)
	})(ctx, request)
}

func (s *Server) refreshKnowledgeGraph(ctx context.Context, cluster string) error {
	topics, err := s.kafkaManager.ListTopics(ctx, cluster, false)
	if err != nil {
		return err
	}

	var topicInfos []graph.TopicInfo
	for _, t := range topics {
		topicInfos = append(topicInfos, graph.TopicInfo{
			Name:              t.Name,
			Partitions:        t.Partitions,
			ReplicationFactor: t.ReplicationFactor,
		})
	}

	groups, err := s.kafkaManager.ListConsumerGroups(ctx, cluster)
	if err != nil {
		return err
	}

	var groupInfos []graph.GroupInfo
	for _, g := range groups {
		detail, err := s.kafkaManager.DescribeConsumerGroup(ctx, cluster, g.GroupID)
		if err != nil {
			continue
		}
		topicSet := make(map[string]bool)
		for _, offset := range detail.Offsets {
			topicSet[offset.Topic] = true
		}
		var groupTopics []string
		for t := range topicSet {
			groupTopics = append(groupTopics, t)
		}
		groupInfos = append(groupInfos, graph.GroupInfo{
			GroupID:     g.GroupID,
			State:      g.State,
			MemberCount: g.MemberCount,
			Topics:     groupTopics,
		})
	}

	s.knowledgeGraph.DiscoverFromTopology(topicInfos, groupInfos, nil)
	return nil
}

// handlePlatformInfo returns platform information and capabilities.
func (s *Server) handlePlatformInfo(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_platform_info",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		platforms := s.platformMgr.ListPlatforms(ctx)
		s.logAudit(agentID, "kafka_platform_info", args, "", "success", len(platforms), nil, start)
		return marshalResult(map[string]any{
			"platforms":      platforms,
			"total_count":    len(platforms),
			"default_type":   "kafka",
		})
	})(ctx, request)
}

func intFromAny(v any, defaultVal int) int {
	switch val := v.(type) {
	case float64:
		return int(val)
	case int:
		return val
	case int64:
		return int(val)
	default:
		return defaultVal
	}
}
