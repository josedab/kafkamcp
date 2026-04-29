package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerChatTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_chat_session",
		withCategory("chat"),
		mcp.WithDescription("Manage conversational chat sessions for multi-turn Kafka exploration. Supports creating, listing, and closing sessions that maintain context across turns."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: create, get, close, list")),
		mcp.WithString("session_id", mcp.Description("Session ID (required for get/close)")),
	), s.handleChatSession)

	s.mcpServer.AddTool(mcp.NewTool("kafka_chat_message",
		withCategory("chat"),
		mcp.WithDescription("Send a message in a chat session. The engine classifies intent, resolves references to previous context (e.g., 'that topic', 'same group'), and tracks entities across turns."),
		mcp.WithString("session_id", mcp.Required(), mcp.Description("Chat session ID")),
		mcp.WithString("message", mcp.Required(), mcp.Description("User message to process")),
	), s.handleChatMessage)

	s.mcpServer.AddTool(mcp.NewTool("kafka_chat_context",
		withCategory("chat"),
		mcp.WithDescription("Get the accumulated context for a chat session, including tracked entities, turn count, and session age."),
		mcp.WithString("session_id", mcp.Required(), mcp.Description("Chat session ID")),
	), s.handleChatContext)

	s.mcpServer.AddTool(mcp.NewTool("kafka_chat_template",
		withCategory("chat"),
		mcp.WithDescription("Get investigation templates for common scenarios like incident triage, capacity review, and schema audit. Templates provide step-by-step guided workflows."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: list, get")),
		mcp.WithString("template_id", mcp.Description("Template ID (required for get)")),
		mcp.WithString("category", mcp.Description("Filter by category: incident, capacity, schema, governance")),
	), s.handleChatTemplate)
}
