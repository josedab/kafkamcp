package server

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/wasmruntime"
)

func (s *Server) handleWASMLoad(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_wasm_load", resourceType: auth.ResourceCluster, action: auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		name := request.GetString("name", "")
		version := request.GetString("version", "1.0.0")
		description := request.GetString("description", "")
		language := wasmruntime.PluginLanguage(request.GetString("language", "other"))

		if name == "" {
			return mcp.NewToolResultError("name is required"), nil
		}
		if deny := validateStringLength("name", name); deny != nil {
			return deny, nil
		}

		manifest := wasmruntime.PluginManifest{
			Name:        name,
			Version:     version,
			Description: description,
			Language:    language,
			Exports: []wasmruntime.ExportDef{
				{Name: "run", Description: "Default entry point"},
			},
		}

		// Create a minimal valid WASM module stub
		wasmBytes := append([]byte("\x00asm"), make([]byte, 64)...)

		plugin, err := s.wasmEngine.LoadPlugin(manifest, wasmBytes)
		if err != nil {
			return s.toolError("failed to load WASM plugin", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_wasm_load", args, "", "success", 1, nil, start)
		return marshalResult(map[string]any{
			"plugin":  plugin,
			"message": "WASM plugin loaded. Use kafka_wasm_execute to run exported functions.",
		})
	})(ctx, request)
}

func (s *Server) handleWASMExecute(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_wasm_execute", resourceType: auth.ResourceCluster, action: auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		pluginID := request.GetString("plugin_id", "")
		function := request.GetString("function", "")
		input := request.GetString("input", "")

		if pluginID == "" || function == "" {
			return mcp.NewToolResultError("plugin_id and function are required"), nil
		}
		if deny := validateStringLength("plugin_id", pluginID); deny != nil {
			return deny, nil
		}

		result, err := s.wasmEngine.Execute(ctx, pluginID, function, input, nil)
		if err != nil {
			return s.toolError("WASM execution failed", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_wasm_execute", args, "", "success", 1, nil, start)
		return marshalResult(result)
	})(ctx, request)
}

func (s *Server) handleWASMManage(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_wasm_manage", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "list")
		pluginID := request.GetString("plugin_id", "")

		switch action {
		case "get":
			if pluginID == "" {
				return mcp.NewToolResultError("plugin_id is required for get"), nil
			}
			plugin, err := s.wasmEngine.GetPlugin(pluginID)
			if err != nil {
				return s.toolError("plugin not found", err), nil
			}
			return marshalResult(plugin)

		case "unload":
			if pluginID == "" {
				return mcp.NewToolResultError("plugin_id is required for unload"), nil
			}
			if err := s.wasmEngine.UnloadPlugin(pluginID); err != nil {
				return s.toolError("failed to unload plugin", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_wasm_manage", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "unloaded", "plugin_id": pluginID})

		default:
			plugins := s.wasmEngine.ListPlugins()
			return marshalResult(map[string]any{"plugins": plugins, "total": len(plugins)})
		}
	})(ctx, request)
}
