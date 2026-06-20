package server

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/josedab/kafkamcp/internal/auth"
)

func (s *Server) handleDiscoverTools(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_discover_tools",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		category := request.GetString("category", "")
		if deny := validateStringLength("category", category); deny != nil {
			return deny, nil
		}
		provider := s.operationProvider
		var providerInfo any
		if provider != nil {
			providerInfo = provider.ProviderInfo()
		}
		var packs []string
		if s.toolPackFilter != nil {
			packs = s.toolPackFilter.Packs()
		}
		registered := s.RegisteredTools()
		s.logAudit(ctx, agentID, "kafka_discover_tools", args, "", "success", len(registered), nil, start)
		return marshalResult(map[string]any{
			"provider":           providerInfo,
			"tool_packs":         packs,
			"registered_tools":   registered,
			"requested_category": category,
			"category_note":      "category metadata is informational; only registered_tools are callable",
		})
	})(ctx, request)
}
