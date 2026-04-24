package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerReplayTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_replay_create",
		mcp.WithDescription("Create a bookmark capturing message state at the current point in time. Stores offsets, sample messages, and metadata for later comparison or replay."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic to bookmark")),
		mcp.WithString("name", mcp.Description("Bookmark name")),
		mcp.WithString("cluster", mcp.Description("Cluster name")),
		mcp.WithObject("tags", mcp.Description("Tags for categorization")),
		mcp.WithString("action", mcp.Description("Action: create, list, get, delete. Default: create")),
		mcp.WithString("bookmark_id", mcp.Description("Bookmark ID (for get/delete)")),
	), s.handleReplayCreate)

	s.mcpServer.AddTool(mcp.NewTool("kafka_replay_diff",
		mcp.WithDescription("Compare message payloads between two bookmarks. Detects schema changes, content drift, and message volume differences."),
		mcp.WithString("bookmark_a", mcp.Required(), mcp.Description("First bookmark ID")),
		mcp.WithString("bookmark_b", mcp.Required(), mcp.Description("Second bookmark ID")),
	), s.handleReplayDiff)

	s.mcpServer.AddTool(mcp.NewTool("kafka_replay_simulate",
		mcp.WithDescription("Simulate replaying messages through a consumer group. Predicts processing impact and estimates reprocessing time."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: create, run, get, list")),
		mcp.WithString("topic", mcp.Description("Topic for simulation")),
		mcp.WithString("group_id", mcp.Description("Consumer group ID")),
		mcp.WithString("from_bookmark", mcp.Description("Start from a bookmark")),
		mcp.WithString("simulation_id", mcp.Description("Simulation ID (for run/get)")),
		mcp.WithString("name", mcp.Description("Simulation name")),
	), s.handleReplaySimulate)
}
