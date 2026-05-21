package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleTopicSearch(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_topic_search", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")

		switch action {
		case "search":
			query := request.GetString("query", "")
			if query == "" {
				return mcp.NewToolResultError("query is required for search action"), nil
			}
			limit := 10
			if l, ok := args["limit"].(float64); ok && l > 0 {
				limit = int(l)
			}
			results := s.semanticEngine.TopicIndex().SearchTopics(query, limit)
			s.logAudit(ctx, agentID, "kafka_topic_search", args, "", "success", len(results), nil, start)
			return marshalResult(map[string]any{
				"query":   query,
				"results": results,
				"total":   len(results),
			})

		case "index_topic":
			topic := request.GetString("topic", "")
			if topic == "" {
				return mcp.NewToolResultError("topic is required for index_topic action"), nil
			}
			description := request.GetString("description", "")
			var tags []string
			if t, ok := args["tags"].([]any); ok {
				for _, v := range t {
					if s, ok := v.(string); ok {
						tags = append(tags, s)
					}
				}
			}
			s.semanticEngine.TopicIndex().IndexTopic(topic, description, tags, nil, 0, nil)
			s.logAudit(ctx, agentID, "kafka_topic_search", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{
				"indexed": topic,
				"total":   s.semanticEngine.TopicIndex().Count(),
			})

		case "index_all":
			cluster := request.GetString("cluster", "")
			cluster = s.resolveClusterName(cluster)
			topics, err := s.kafkaManager.ListTopics(ctx, cluster, false)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_topic_search", args, cluster, "error", 0, err, start)
				return s.toolError("failed to list topics", err), nil
			}
			indexed := 0
			for _, t := range topics {
				desc := fmt.Sprintf("Kafka topic with %d partitions, RF=%d", t.Partitions, t.ReplicationFactor)
				s.semanticEngine.TopicIndex().IndexTopic(t.Name, desc, nil, nil, t.MessageCount, nil)
				indexed++
			}
			s.logAudit(ctx, agentID, "kafka_topic_search", args, cluster, "success", indexed, nil, start)
			return marshalResult(map[string]any{
				"indexed": indexed,
				"cluster": cluster,
				"total":   s.semanticEngine.TopicIndex().Count(),
			})

		case "list_indexed":
			profiles := s.semanticEngine.TopicIndex().ListProfiles()
			s.logAudit(ctx, agentID, "kafka_topic_search", args, "", "success", len(profiles), nil, start)
			return marshalResult(map[string]any{
				"profiles": profiles,
				"total":    len(profiles),
			})

		default:
			return mcp.NewToolResultError("action must be: search, index_topic, index_all, list_indexed"), nil
		}
	})(ctx, request)
}
