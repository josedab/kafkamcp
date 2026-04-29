package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerSessionReplayTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_session_replay",
		withCategory("debugging"),
		mcp.WithDescription("Record and replay AI agent sessions for debugging. Captures tool call traces, timing, and response payloads."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: start, record, end, get, list, replay, delete")),
		mcp.WithString("session_id", mcp.Description("Session ID (for record/end/get/replay/delete)")),
		mcp.WithString("agent_id", mcp.Description("Agent ID filter (for list) or session owner (for start)")),
		mcp.WithString("tool", mcp.Description("Tool name (for record action)")),
		mcp.WithObject("call_args", mcp.Description("Tool call arguments (for record action)")),
		mcp.WithObject("call_response", mcp.Description("Tool call response (for record action)")),
		mcp.WithBoolean("is_error", mcp.Description("Whether the call was an error (for record action)")),
		mcp.WithString("error_message", mcp.Description("Error message if call failed (for record action)")),
		mcp.WithNumber("duration_ms", mcp.Description("Call duration in milliseconds (for record action)")),
		mcp.WithNumber("limit", mcp.Description("Max results to return (for list, default: 50)")),
	), s.handleSessionReplay)
}
