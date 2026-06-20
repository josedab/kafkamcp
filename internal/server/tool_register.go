package server

import (
	"fmt"
	"sort"
	"strings"

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
	decorateWriteTool(&tool)
	stability := toolStability(name)
	if s.registeredTools == nil {
		s.registeredTools = make(map[string]string)
	}
	if s.seenToolNames == nil {
		s.seenToolNames = make(map[string]struct{})
	}

	// Central unique-name enforcement: a tool name may only be registered once,
	// regardless of pack filtering. A duplicate is always a code bug (two
	// register_*.go sites claiming the same name) and must fail server
	// construction deterministically rather than silently shadowing a handler.
	if _, seen := s.seenToolNames[name]; seen && name != "" {
		s.duplicateTools = append(s.duplicateTools, name)
	}
	s.seenToolNames[name] = struct{}{}

	if s.toolPackFilter != nil && !s.toolPackFilter.ShouldRegister(name) {
		s.skippedToolCount++
		if s.logger != nil {
			s.logger.Debug("tool filtered by tool pack",
				"tool", name,
				"stability", stability,
				"category", toolCategoryOf(&tool))
		}
		return
	}
	s.registeredTools[name] = stability
	s.mcpServer.AddTool(tool, handler)
}

func decorateWriteTool(tool *mcp.Tool) {
	if tool == nil || !IsWriteTool(tool.Name) {
		return
	}
	if tool.InputSchema.Properties == nil {
		tool.InputSchema.Properties = make(map[string]any)
	}
	tool.InputSchema.Properties[approvalTokenArg] = map[string]any{
		"type":        "string",
		"description": "Short-lived, single-use token issued by kafka_approval for this exact agent/tool/cluster/resource/argument set.",
		"minLength":   32,
	}
	if isIdempotentWriteTool(tool.Name) {
		tool.InputSchema.Properties[idempotencyKeyArg] = map[string]any{
			"type":        "string",
			"description": "Optional caller-generated retry key. Reusing it with identical arguments returns the stored success without re-executing the mutation.",
			"minLength":   8,
			"maxLength":   128,
		}
	}
	tool.Description += " Requires a matching short-lived approval_token when the ops-write pack is enabled."
}

// DuplicateToolError returns a non-nil error when any tool name was registered
// more than once during registerTools(). It is checked during server
// construction so duplicate registrations fail loudly and deterministically.
func (s *Server) DuplicateToolError() error {
	if len(s.duplicateTools) == 0 {
		return nil
	}
	// Deduplicate and sort for a stable, useful message.
	seen := make(map[string]struct{}, len(s.duplicateTools))
	names := make([]string, 0, len(s.duplicateTools))
	for _, n := range s.duplicateTools {
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		names = append(names, n)
	}
	sort.Strings(names)
	return fmt.Errorf("duplicate tool registration detected for: %s; each MCP tool name must be registered exactly once", strings.Join(names, ", "))
}

// RegisteredToolCount returns the number of tools actually exposed to MCP
// clients after pack filtering. Useful for tests and operational dashboards.
func (s *Server) RegisteredToolCount() int {
	return len(s.registeredTools)
}

// RegisteredTools returns a copy of name to stability for every registered tool.
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
