package server

import (
	"context"
	"strings"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/capability"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleNegotiateCapabilities(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_negotiate_capabilities",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		role := capability.Role(request.GetString("role", "viewer"))
		contextStr := request.GetString("context", "")

		if deny := validateStringLength("role", string(role)); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("context", contextStr); deny != nil {
			return deny, nil
		}

		var contextHints []string
		if contextStr != "" {
			contextHints = strings.Split(contextStr, ",")
			for i := range contextHints {
				contextHints[i] = strings.TrimSpace(contextHints[i])
			}
		}

		result := s.capabilityNegotiator.Negotiate(agentID, role, contextHints)

		s.logAudit(ctx, agentID, "kafka_negotiate_capabilities", args, "", "success", result.FilteredTools, nil, start)
		return marshalResult(result)
	})(ctx, request)
}

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
		if category == "" {
			// Return all categories
			cats := s.capabilityNegotiator.ListCategories()
			s.logAudit(ctx, agentID, "kafka_discover_tools", args, "", "success", len(cats), nil, start)
			return marshalResult(map[string]any{"categories": cats})
		}

		result := s.capabilityNegotiator.DiscoverTools(agentID, category)

		s.logAudit(ctx, agentID, "kafka_discover_tools", args, "", "success", result.Total, nil, start)
		return marshalResult(result)
	})(ctx, request)
}

func (s *Server) handleCapabilityProfile(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_capability_profile",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		profile, err := s.capabilityNegotiator.GetProfile(agentID)
		if err != nil {
			return marshalResult(map[string]any{
				"status":  "no_profile",
				"message": "No capability profile. Use kafka_negotiate_capabilities to set up.",
			})
		}

		s.logAudit(ctx, agentID, "kafka_capability_profile", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{
			"profile":      profile,
			"tools_count":  len(profile.AllowedTools),
		})
	})(ctx, request)
}
