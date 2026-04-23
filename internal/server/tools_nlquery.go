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
			return s.toolError("Failed to list topics", err), nil
		}

		topicNames := make([]string, len(topics))
		for i, t := range topics {
			topicNames[i] = t.Name
		}

		plan, err := s.nlQueryEngine.Parse(query, topicNames)
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
