package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

// handleTraceMessage traces a message across topics by correlation ID.
func (s *Server) handleTraceMessage(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_trace_message", resourceType: auth.ResourceTopic, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		correlationID := request.GetString("correlation_id", "")
		cluster := request.GetString("cluster", "")

		if correlationID == "" {
			return mcp.NewToolResultError("correlation_id is required"), nil
		}
		if deny := validateStringLength("correlation_id", correlationID); deny != nil {
			return deny, nil
		}

		if err := s.acquireExpensiveOp(ctx); err != nil {
			return s.toolError("operation capacity exceeded", err), nil
		}
		defer s.releaseExpensiveOp()

		headerName := request.GetString("header_name", "")
		maxScan := int(clampInt64(int64(request.GetInt("max_scan", 10000)), 1, int64(MaxSearchScan)))

		// Get topics to scan
		var topicNames []string
		if rawArgs := request.GetRawArguments(); rawArgs != nil {
			if m, ok := rawArgs.(map[string]any); ok {
				if t, ok := m["topics"].([]any); ok {
					for _, v := range t {
						if s, ok := v.(string); ok {
							topicNames = append(topicNames, s)
						}
					}
				}
			}
		}

		// If no topics specified, get all
		if len(topicNames) == 0 {
			topics, err := s.kafkaManager.ListTopics(ctx, cluster, false)
			if err != nil {
				s.logAudit(agentID, "kafka_trace_message", args, cluster, "error", 0, err, start)
				return s.toolError("Failed to list topics", err), nil
			}
			for _, t := range topics {
				topicNames = append(topicNames, t.Name)
			}
		}

		result, err := s.kafkaManager.TraceMessage(ctx, cluster, correlationID, headerName, topicNames, maxScan)
		if err != nil {
			s.logAudit(agentID, "kafka_trace_message", args, cluster, "error", 0, err, start)
			return s.toolError("Failed", err), nil
		}

		s.logAudit(agentID, "kafka_trace_message", args, cluster, "success", len(result.Hops), nil, start)
		return marshalResult(result)
	})(ctx, request)
}
