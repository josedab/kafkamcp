package server

import (
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// addTool is the canonical way to register an MCP tool from inside the
// server package. It delegates to the underlying mcp-go server but first
// consults the active tool-pack filter; tools that are not in the active
// pack are silently skipped (and the skip is counted for startup logs).
//
// Every registration site MUST go through this wrapper so adaptive tool
// surfaces stay consistent. The previous direct call pattern
// (`s.addTool(...)`) is preserved for backward compatibility with
// existing register_*.go files: see init() in this file.
func (s *Server) addTool(tool mcp.Tool, handler server.ToolHandlerFunc) {
	name := tool.Name
	tier := toolTier(&tool)
	if s.registeredTools == nil {
		s.registeredTools = make(map[string]string)
	}
	if s.toolPackFilter != nil && !s.toolPackFilter.ShouldRegister(name, tier) {
		s.skippedToolCount++
		if s.logger != nil {
			s.logger.Debug("tool filtered by tool pack",
				"tool", name,
				"tier", tier,
				"category", toolCategoryOf(&tool))
		}
		return
	}
	s.registeredTools[name] = tier
	s.addTool(tool, handler)
}

// RegisteredToolCount returns the number of tools actually exposed to MCP
// clients after pack filtering. Useful for tests and operational dashboards.
func (s *Server) RegisteredToolCount() int {
	return len(s.registeredTools)
}

// RegisteredTools returns a copy of name → tier for every registered tool.
func (s *Server) RegisteredTools() map[string]string {
	out := make(map[string]string, len(s.registeredTools))
	for k, v := range s.registeredTools {
		out[k] = v
	}
	return out
}

// SkippedToolCount returns the number of tools the active tool pack filter
// suppressed during registration.
func (s *Server) SkippedToolCount() int { return s.skippedToolCount }
