package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handlePluginSearch(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_plugin_search",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		query := request.GetString("query", "")
		category := request.GetString("category", "")
		verifiedOnly := request.GetBool("verified_only", false)

		results := s.marketplace.Search(query, category, verifiedOnly)

		s.logAudit(ctx, agentID, "kafka_plugin_search", args, "", "success", len(results), nil, start)
		return marshalResult(map[string]any{
			"plugins":     results,
			"total_count": len(results),
			"query":       query,
			"category":    category,
		})
	})(ctx, request)
}

func (s *Server) handlePluginInstall(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_plugin_install",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		name := request.GetString("name", "")
		if name == "" {
			return mcp.NewToolResultError("name is required"), nil
		}
		if deny := validateStringLength("name", name); deny != nil {
			return deny, nil
		}

		entry, err := s.marketplace.Install(name)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_plugin_install", args, "", "error", 0, err, start)
			return s.toolError("Failed to install plugin", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_plugin_install", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{
			"status":  "installed",
			"plugin":  entry,
			"message": fmt.Sprintf("Plugin %q v%s installed successfully", entry.Name, entry.Version),
		})
	})(ctx, request)
}

func (s *Server) handlePluginList(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_plugin_list",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		plugins := s.pluginRegistry.List()

		s.logAudit(ctx, agentID, "kafka_plugin_list", args, "", "success", len(plugins), nil, start)
		return marshalResult(map[string]any{
			"installed_plugins": plugins,
			"total_count":       len(plugins),
		})
	})(ctx, request)
}
