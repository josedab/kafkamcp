package server

import (
	mcp "github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerWASMTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_wasm_load",
		withCategory("plugin"),
		mcp.WithDescription("Load a WASM plugin module into the sandboxed runtime. Supports plugins written in Rust, TinyGo, JavaScript, or Python compiled to WASM."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Plugin name")),
		mcp.WithString("version", mcp.Description("Plugin version. Default: 1.0.0")),
		mcp.WithString("description", mcp.Description("Plugin description")),
		mcp.WithString("language", mcp.Description("Source language: rust, tinygo, javascript, python")),
	), s.handleWASMLoad)

	s.mcpServer.AddTool(mcp.NewTool("kafka_wasm_execute",
		withCategory("plugin"),
		mcp.WithDescription("Execute a function in a loaded WASM plugin within sandboxed resource limits."),
		mcp.WithString("plugin_id", mcp.Required(), mcp.Description("WASM plugin ID")),
		mcp.WithString("function", mcp.Required(), mcp.Description("Function name to execute")),
		mcp.WithString("input", mcp.Description("JSON input to pass to the function")),
	), s.handleWASMExecute)

	s.mcpServer.AddTool(mcp.NewTool("kafka_wasm_manage",
		withCategory("plugin"),
		mcp.WithDescription("Manage WASM plugins: list, get, or unload."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: list, get, unload")),
		mcp.WithString("plugin_id", mcp.Description("Plugin ID (required for get/unload)")),
	), s.handleWASMManage)
}
