package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerStreamingTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_stream",
		mcp.WithDescription("Manage streaming consumer sessions for real-time message delivery. Actions: start (create session), poll (get messages), pause/resume/stop (control), list (show sessions)."),
		mcp.WithString("action", mcp.Description("Action: start, poll, pause, resume, stop, list. Default: start")),
		mcp.WithString("topic", mcp.Description("Topic name (required for start)")),
		mcp.WithString("group_id", mcp.Description("Consumer group ID. Auto-generated if omitted.")),
		mcp.WithString("session_id", mcp.Description("Session ID (required for poll/pause/resume/stop)")),
		mcp.WithNumber("batch_size", mcp.Description("Messages per batch (1-100). Default: 10")),
		mcp.WithNumber("max_msg_per_sec", mcp.Description("Throttle: max messages/sec. Default: 1000")),
		mcp.WithNumber("max_messages", mcp.Description("Max total messages before auto-stop. Default: 1000")),
		mcp.WithNumber("timeout_ms", mcp.Description("Session timeout in ms (max 60000). Default: 30000")),
		mcp.WithNumber("poll_timeout_ms", mcp.Description("Poll wait time in ms. Default: 5000")),
		mcp.WithString("deserializer", mcp.Description("Deserialization: auto, json, string, bytes. Default: auto")),
		mcp.WithBoolean("auto_commit", mcp.Description("Auto-commit offsets. Default: true")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleStream)

	// Multi-agent collaboration
	s.mcpServer.AddTool(mcp.NewTool("kafka_agent_registry",
		mcp.WithDescription("Multi-agent collaboration: register agents, claim partitions, share cursors, and handoff messages. Actions: register, heartbeat, claim, release, share_cursor, handoff, list_agents."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: register, heartbeat, claim, release, share_cursor, handoff, list_agents")),
		mcp.WithString("agent_name", mcp.Description("Agent name for registration")),
		mcp.WithString("topic", mcp.Description("Topic for partition operations")),
		mcp.WithNumber("partition", mcp.Description("Partition number to claim/release")),
		mcp.WithNumber("offset", mcp.Description("Offset for cursor sharing")),
		mcp.WithString("target_agent", mcp.Description("Target agent for handoff")),
		mcp.WithString("message_data", mcp.Description("Message data for handoff (JSON string)")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleAgentRegistry)
}
