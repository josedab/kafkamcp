package server

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
)

func (s *Server) handleGraphQLSchema(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_graphql_schema", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		format := request.GetString("format", "json")

		if format == "sdl" {
			sdl := s.graphqlEngine.GetSchemaSDL()
			s.logAudit(ctx, agentID, "kafka_graphql_schema", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"format": "sdl", "schema": sdl})
		}

		schema := s.graphqlEngine.GetSchema()
		s.logAudit(ctx, agentID, "kafka_graphql_schema", args, "", "success", 1, nil, start)
		return marshalResult(schema)
	})(ctx, request)
}

func (s *Server) handleGraphQLQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_graphql_query", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		query := request.GetString("query", "")
		if query == "" {
			return mcp.NewToolResultError("query is required"), nil
		}
		if deny := validateStringLength("query", query); deny != nil {
			return deny, nil
		}

		result := s.graphqlEngine.ExecuteQuery(query, nil)
		s.logAudit(ctx, agentID, "kafka_graphql_query", args, "", "success", 1, nil, start)
		return marshalResult(result)
	})(ctx, request)
}

func (s *Server) handleGraphQLSubscribe(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_graphql_subscribe", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "list")

		switch action {
		case "subscribe":
			subName := request.GetString("subscription", "")
			topic := request.GetString("topic", "")
			if subName == "" {
				return mcp.NewToolResultError("subscription name is required"), nil
			}
			subscriber, err := s.graphqlEngine.Subscribe(subName, topic)
			if err != nil {
				return s.toolError("subscription failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_graphql_subscribe", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"subscriber": subscriber, "message": "Subscribed successfully"})

		case "unsubscribe":
			subID := request.GetString("subscriber_id", "")
			if subID == "" {
				return mcp.NewToolResultError("subscriber_id is required"), nil
			}
			if err := s.graphqlEngine.Unsubscribe(subID); err != nil {
				return s.toolError("unsubscribe failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_graphql_subscribe", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "unsubscribed"})

		default:
			subscribers := s.graphqlEngine.ListSubscribers()
			return marshalResult(map[string]any{"subscribers": subscribers, "total": len(subscribers)})
		}
	})(ctx, request)
}
