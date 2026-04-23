package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

// handlePlayground manages sandboxed Kafka playground environments.
func (s *Server) handlePlayground(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_playground", resourceType: auth.ResourceTopic, action: auth.ActionCreate,
		resourceName: func(r mcp.CallToolRequest) string { return "_playground_*" },
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		cluster := request.GetString("cluster", "")

		switch action {
		case "create":
			name := request.GetString("name", "test")
			if deny := validateStringLength("name", name); deny != nil {
				return deny, nil
			}
			partitions := int32(request.GetInt("partitions", 3))
			if partitions > 12 {
				partitions = 12
			}
			result, err := s.kafkaManager.PlaygroundCreate(ctx, cluster, agentID, name, partitions)
			if err != nil {
				s.logAudit(agentID, "kafka_playground", args, cluster, "error", 0, err, start)
				return s.toolError("Failed", err), nil
			}
			s.logAudit(agentID, "kafka_playground", args, cluster, "success", 0, nil, start)
			return marshalResult(result)

		case "produce_sample":
			name := request.GetString("name", "test")
			pattern := request.GetString("data_pattern", "ecommerce")
			count := request.GetInt("count", 10)
			if count > 100 {
				count = 100
			}
			result, err := s.kafkaManager.PlaygroundProduceSample(ctx, cluster, agentID, name, pattern, count)
			if err != nil {
				s.logAudit(agentID, "kafka_playground", args, cluster, "error", 0, err, start)
				return s.toolError("Failed", err), nil
			}
			s.logAudit(agentID, "kafka_playground", args, cluster, "success", count, nil, start)
			return marshalResult(result)

		case "list":
			topics, err := s.kafkaManager.PlaygroundList(ctx, cluster, agentID)
			if err != nil {
				s.logAudit(agentID, "kafka_playground", args, cluster, "error", 0, err, start)
				return s.toolError("Failed", err), nil
			}
			s.logAudit(agentID, "kafka_playground", args, cluster, "success", len(topics), nil, start)
			return marshalResult(map[string]any{"playground_topics": topics, "count": len(topics)})

		case "cleanup":
			count, err := s.kafkaManager.PlaygroundCleanup(ctx, cluster, agentID)
			if err != nil {
				s.logAudit(agentID, "kafka_playground", args, cluster, "error", 0, err, start)
				return s.toolError("Failed", err), nil
			}
			s.logAudit(agentID, "kafka_playground", args, cluster, "success", count, nil, start)
			return marshalResult(map[string]any{"status": "cleaned_up", "topics_deleted": count})

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; must be: create, produce_sample, list, cleanup", action)), nil
		}
	})(ctx, request)
}
