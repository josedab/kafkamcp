package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleMarketplaceDiscover(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_marketplace_discover", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		query := request.GetString("query", "")
		category := request.GetString("category", "")

		result := s.marketplace.Discover(query, category, s.agentConfigStore)
		s.logAudit(ctx, agentID, "kafka_marketplace_discover", args, "", "success", result.TotalResults, nil, start)
		return marshalResult(result)
	})(ctx, request)
}

func (s *Server) handleMarketplaceStats(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_marketplace_stats", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		stats := s.marketplace.GetStats(s.agentConfigStore, s.reviewStore)
		s.logAudit(ctx, agentID, "kafka_marketplace_stats", args, "", "success", 1, nil, start)
		return marshalResult(stats)
	})(ctx, request)
}

func (s *Server) handleMarketplaceReview(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_marketplace_review", resourceType: auth.ResourceCluster, action: auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "list")
		itemName := request.GetString("item_name", "")

		if itemName == "" {
			return mcp.NewToolResultError("item_name is required"), nil
		}

		switch action {
		case "add":
			rating := request.GetFloat("rating", 0)
			if rating < 1.0 || rating > 5.0 {
				return mcp.NewToolResultError("rating must be between 1.0 and 5.0"), nil
			}
			itemType := request.GetString("item_type", "plugin")
			title := request.GetString("title", "")
			body := request.GetString("body", "")

			review, err := s.reviewStore.AddReview(itemName, itemType, agentID, rating, title, body)
			if err != nil {
				return s.toolError("failed to add review", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_marketplace_review", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"review": review, "message": "Review added successfully"})

		default:
			reviews := s.reviewStore.GetReviews(itemName)
			avg := s.reviewStore.AverageRating(itemName)
			return marshalResult(map[string]any{
				"item_name":      itemName,
				"reviews":        reviews,
				"total":          len(reviews),
				"average_rating": avg,
			})
		}
	})(ctx, request)
}
