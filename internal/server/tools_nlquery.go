package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/graph"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/kafkamcp/kafkamcp/internal/nlquery"
	"github.com/mark3labs/mcp-go/mcp"
)

// handleNLQuery processes natural language queries against Kafka topics.
func (s *Server) handleNLQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_nl_query",
		resourceType: auth.ResourceTopic,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		query := request.GetString("query", "")
		cluster := request.GetString("cluster", "")
		action := request.GetString("action", "execute")

		if action == "history" {
			limit := 20
			if l, ok := args["limit"].(float64); ok && l > 0 {
				limit = int(l)
			}
			if limit > MaxDLQMessages {
				limit = MaxDLQMessages
			}
			history := s.nlQueryEngine.GetHistory(limit)
			s.logAudit(agentID, "kafka_nl_query", args, cluster, "success", len(history), nil, start)
			return marshalResult(map[string]any{"history": history, "total": len(history)})
		}

		if action == "suggest" {
			topics, _ := s.kafkaManager.ListTopics(ctx, cluster, false)
			topicNames := make([]string, len(topics))
			for i, t := range topics {
				topicNames[i] = t.Name
			}
			suggestions := s.nlQueryEngine.Suggest(query, topicNames)
			return marshalResult(map[string]any{"suggestions": suggestions})
		}

		if query == "" {
			return mcp.NewToolResultError("query is required"), nil
		}
		if deny := validateStringLength("query", query); deny != nil {
			return deny, nil
		}

		// Check cache first
		if cached, ok := s.nlQueryEngine.GetCached(query); ok {
			s.logAudit(agentID, "kafka_nl_query", args, cluster, "success", cached.TotalCount, nil, start)
			return marshalResult(cached)
		}

		// Get available topics for context
		topics, err := s.kafkaManager.ListTopics(ctx, cluster, false)
		if err != nil {
			s.logAudit(agentID, "kafka_nl_query", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to list topics", err), nil
		}

		topicNames := make([]string, len(topics))
		for i, t := range topics {
			topicNames[i] = t.Name
		}

		// Use semantic parser for richer intent extraction
		plan, err := s.nlQueryEngine.SemanticParse(query, topicNames)
		if err != nil {
			s.logAudit(agentID, "kafka_nl_query", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to parse query", err), nil
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

		// Apply aggregation if specified
		if plan.Aggregation != nil {
			results = applyAggregation(results, plan.Aggregation)
		}

		if plan.Limit > 0 && len(results) > plan.Limit {
			results = results[:plan.Limit]
		}

		queryResult := nlquery.ExecutionResult{
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

		// Cache and record history
		durationMs := time.Since(start).Milliseconds()
		s.nlQueryEngine.CacheResult(query, queryResult)
		s.nlQueryEngine.RecordHistory(query, *plan, len(results), durationMs)

		s.logAudit(agentID, "kafka_nl_query", args, cluster, "success", len(results), nil, start)
		return marshalResult(queryResult)
	})(ctx, request)
}

// applyAggregation performs in-memory aggregation on results.
func applyAggregation(data []map[string]any, agg *nlquery.AggregationStep) []map[string]any {
	if agg == nil || len(data) == 0 {
		return data
	}

	switch agg.Function {
	case "count":
		if agg.GroupBy != "" {
			groups := make(map[string]int)
			for _, row := range data {
				key := fmt.Sprintf("%v", extractNestedField(row, agg.GroupBy))
				groups[key]++
			}
			var result []map[string]any
			for k, v := range groups {
				result = append(result, map[string]any{agg.GroupBy: k, "count": v})
			}
			return result
		}
		return []map[string]any{{"count": len(data)}}

	case "avg", "sum", "min", "max":
		if agg.Field == "" {
			return data
		}
		var values []float64
		for _, row := range data {
			if v := extractNestedField(row, agg.Field); v != nil {
				if f, ok := toFloat64(v); ok {
					values = append(values, f)
				}
			}
		}
		if len(values) == 0 {
			return []map[string]any{{agg.Function: nil, "field": agg.Field}}
		}
		var aggVal float64
		switch agg.Function {
		case "avg":
			sum := 0.0
			for _, v := range values {
				sum += v
			}
			aggVal = sum / float64(len(values))
		case "sum":
			for _, v := range values {
				aggVal += v
			}
		case "min":
			aggVal = values[0]
			for _, v := range values[1:] {
				if v < aggVal {
					aggVal = v
				}
			}
		case "max":
			aggVal = values[0]
			for _, v := range values[1:] {
				if v > aggVal {
					aggVal = v
				}
			}
		}
		return []map[string]any{{agg.Function: aggVal, "field": agg.Field, "sample_count": len(values)}}

	case "group_by":
		if agg.GroupBy == "" {
			return data
		}
		groups := make(map[string]int)
		for _, row := range data {
			key := fmt.Sprintf("%v", extractNestedField(row, agg.GroupBy))
			groups[key]++
		}
		var result []map[string]any
		for k, v := range groups {
			result = append(result, map[string]any{agg.GroupBy: k, "count": v})
		}
		return result
	}
	return data
}

func extractNestedField(row map[string]any, field string) any {
	// Check top-level first
	if v, ok := row[field]; ok {
		return v
	}
	// Check inside "value" (common for Kafka messages)
	if val, ok := row["value"].(map[string]any); ok {
		if v, ok := val[field]; ok {
			return v
		}
	}
	return nil
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	default:
		return 0, false
	}
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
			return s.toolError("Query failed", err), nil
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

// handleNLOperate processes natural language operational commands.
func (s *Server) handleNLOperate(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	command, _ := args["command"].(string)
	if command == "" {
		return mcp.NewToolResultError("command is required"), nil
	}
	cluster, _ := args["cluster"].(string)
	dryRun, _ := args["dry_run"].(bool)

	cmd, err := s.nlQueryEngine.ParseOperational(command)
	if err != nil {
		s.logAudit(agentID, "kafka_nl_operate", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Could not parse command: %v", err)), nil
	}

	if cluster != "" {
		cmd.Cluster = cluster
	}
	if cmd.Cluster == "" {
		cmd.Cluster = s.kafkaManager.DefaultClusterName()
	}

	if dryRun {
		s.logAudit(agentID, "kafka_nl_operate", args, cmd.Cluster, "success", 0, nil, start)
		return marshalResult(map[string]any{
			"dry_run": true,
			"command": cmd,
			"message": "Command parsed successfully. Remove dry_run to execute.",
		})
	}

	result, execErr := s.executeNLCommand(ctx, cmd)
	if execErr != nil {
		s.logAudit(agentID, "kafka_nl_operate", args, cmd.Cluster, "error", 0, execErr, start)
		return s.toolError("Command execution failed", execErr), nil
	}

	s.logAudit(agentID, "kafka_nl_operate", args, cmd.Cluster, "success", 1, nil, start)
	return marshalResult(map[string]any{
		"command": cmd,
		"result":  result,
	})
}

func (s *Server) executeNLCommand(ctx context.Context, cmd *nlquery.OperationalCommand) (any, error) {
	switch cmd.Action {
	case "list_topics":
		topics, err := s.kafkaManager.ListTopics(ctx, cmd.Cluster, false)
		if err != nil {
			return nil, fmt.Errorf("listing topics: %w", err)
		}
		return map[string]any{"topics": topics, "count": len(topics)}, nil

	case "describe_topic":
		if cmd.Name == "" {
			return nil, fmt.Errorf("topic name is required")
		}
		detail, err := s.kafkaManager.DescribeTopic(ctx, cmd.Cluster, cmd.Name)
		if err != nil {
			return nil, fmt.Errorf("describing topic %q: %w", cmd.Name, err)
		}
		return detail, nil

	case "create_topic":
		if cmd.Name == "" {
			return nil, fmt.Errorf("topic name is required")
		}
		partitions := int32(3)
		replication := int16(1)
		if p, ok := cmd.Parameters["partitions"].(int); ok && p > 0 {
			partitions = int32(p)
		} else if p, ok := cmd.Parameters["partitions"].(float64); ok && p > 0 {
			partitions = int32(p)
		}
		if r, ok := cmd.Parameters["replication_factor"].(int); ok && r > 0 {
			replication = int16(r)
		} else if r, ok := cmd.Parameters["replication_factor"].(float64); ok && r > 0 {
			replication = int16(r)
		}
		err := s.kafkaManager.CreateTopic(ctx, cmd.Cluster, kafka.CreateTopicRequest{
			Name:              cmd.Name,
			Partitions:        partitions,
			ReplicationFactor: replication,
		})
		if err != nil {
			return nil, fmt.Errorf("creating topic %q: %w", cmd.Name, err)
		}
		return map[string]any{"status": "created", "topic": cmd.Name, "partitions": partitions, "replication_factor": replication}, nil

	case "delete_topic":
		if cmd.Name == "" {
			return nil, fmt.Errorf("topic name is required")
		}
		err := s.kafkaManager.DeleteTopic(ctx, cmd.Cluster, cmd.Name)
		if err != nil {
			return nil, fmt.Errorf("deleting topic %q: %w", cmd.Name, err)
		}
		return map[string]any{"status": "deleted", "topic": cmd.Name}, nil

	case "list_groups":
		groups, err := s.kafkaManager.ListConsumerGroups(ctx, cmd.Cluster)
		if err != nil {
			return nil, fmt.Errorf("listing consumer groups: %w", err)
		}
		return map[string]any{"consumer_groups": groups, "count": len(groups)}, nil

	case "describe_group":
		if cmd.Name == "" {
			return nil, fmt.Errorf("group name is required")
		}
		detail, err := s.kafkaManager.DescribeConsumerGroup(ctx, cmd.Cluster, cmd.Name)
		if err != nil {
			return nil, fmt.Errorf("describing group %q: %w", cmd.Name, err)
		}
		return detail, nil

	case "reset_offsets":
		if cmd.Name == "" {
			return nil, fmt.Errorf("consumer group name is required")
		}
		target := "earliest"
		if t, ok := cmd.Parameters["target"].(string); ok {
			target = t
		}
		topic, _ := cmd.Parameters["topic"].(string)
		result, err := s.kafkaManager.ResetOffsets(ctx, cmd.Cluster, kafka.ResetOffsetsRequest{
			GroupID:  cmd.Name,
			Topic:    topic,
			Strategy: target,
		})
		if err != nil {
			return nil, fmt.Errorf("resetting offsets for group %q: %w", cmd.Name, err)
		}
		return result, nil

	default:
		return nil, fmt.Errorf("unsupported operational command: %s", cmd.Action)
	}
}
