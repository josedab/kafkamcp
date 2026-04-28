package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/plugin"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleAgentConfigSearch(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_agent_config_search",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		query := request.GetString("query", "")
		category := request.GetString("category", "")
		searchType := request.GetString("type", "all")
		verifiedOnly := request.GetBool("verified_only", false)

		result := map[string]any{"query": query, "category": category}

		if searchType == "all" || searchType == "configs" {
			configs := s.agentConfigStore.SearchConfigs(query, category, verifiedOnly)
			result["agent_configs"] = configs
			result["agent_config_count"] = len(configs)
		}
		if searchType == "all" || searchType == "bundles" {
			bundles := s.agentConfigStore.SearchBundles(query, category)
			result["tool_bundles"] = bundles
			result["tool_bundle_count"] = len(bundles)
		}

		s.logAudit(agentID, "kafka_agent_config_search", args, "", "success", 0, nil, start)
		return marshalResult(result)
	})(ctx, request)
}

func (s *Server) handleAgentConfigGet(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_agent_config_get",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		name := request.GetString("name", "")
		if name == "" {
			return mcp.NewToolResultError("name is required"), nil
		}
		if deny := validateStringLength("name", name); deny != nil {
			return deny, nil
		}

		entryType := request.GetString("type", "config")

		if entryType == "bundle" {
			bundle, err := s.agentConfigStore.GetBundle(name)
			if err != nil {
				s.logAudit(agentID, "kafka_agent_config_get", args, "", "error", 0, err, start)
				return s.toolError("Bundle not found", err), nil
			}
			s.logAudit(agentID, "kafka_agent_config_get", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"type": "bundle", "bundle": bundle})
		}

		cfg, err := s.agentConfigStore.GetConfig(name)
		if err != nil {
			s.logAudit(agentID, "kafka_agent_config_get", args, "", "error", 0, err, start)
			return s.toolError("Agent config not found", err), nil
		}
		s.logAudit(agentID, "kafka_agent_config_get", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{"type": "config", "config": cfg})
	})(ctx, request)
}

func (s *Server) handleAgentConfigPublish(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_agent_config_publish",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		name := request.GetString("name", "")
		version := request.GetString("version", "")
		description := request.GetString("description", "")
		category := request.GetString("category", "")
		entryType := request.GetString("type", "config")

		if name == "" || version == "" || description == "" {
			return mcp.NewToolResultError("name, version, and description are required"), nil
		}
		if deny := validateStringLength("name", name); deny != nil {
			return deny, nil
		}

		if entryType == "bundle" {
			err := s.agentConfigStore.PublishBundle(plugin.ToolBundle{
				Name: name, Version: version, Description: description,
				Category: category, Author: agentID, Tools: []string{},
			})
			if err != nil {
				s.logAudit(agentID, "kafka_agent_config_publish", args, "", "error", 0, err, start)
				return s.toolError("Failed to publish bundle", err), nil
			}
			s.logAudit(agentID, "kafka_agent_config_publish", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "published", "type": "bundle", "name": name})
		}

		err := s.agentConfigStore.PublishConfig(plugin.AgentConfig{
			Name: name, Version: version, Description: description,
			Category: category, Author: agentID,
		})
		if err != nil {
			s.logAudit(agentID, "kafka_agent_config_publish", args, "", "error", 0, err, start)
			return s.toolError("Failed to publish config", err), nil
		}
		s.logAudit(agentID, "kafka_agent_config_publish", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{"status": "published", "type": "config", "name": name})
	})(ctx, request)
}
