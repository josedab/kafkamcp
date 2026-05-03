package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerTimeTravelTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_timetravel_record",
		withCategory("analytics"),
		mcp.WithDescription("Record a point-in-time state snapshot for a topic or consumer group."),
		mcp.WithString("key", mcp.Required(), mcp.Description("Entity key, e.g. 'topic:orders' or 'group:my-consumers'")),
		mcp.WithString("type", mcp.Required(), mcp.Description("Snapshot type: topic, consumer_group")),
		mcp.WithObject("state", mcp.Required(), mcp.Description("Arbitrary state data to capture")),
	), s.handleTimeTravelRecord)

	s.mcpServer.AddTool(mcp.NewTool("kafka_timetravel_query",
		withCategory("analytics"),
		mcp.WithDescription("Query state at a specific point in time using binary search for the closest snapshot."),
		mcp.WithString("key", mcp.Required(), mcp.Description("Entity key to query")),
		mcp.WithString("type", mcp.Required(), mcp.Description("Snapshot type: topic, consumer_group")),
		mcp.WithString("timestamp", mcp.Required(), mcp.Description("Point in time (RFC3339)")),
	), s.handleTimeTravelQuery)

	s.mcpServer.AddTool(mcp.NewTool("kafka_timetravel_range",
		withCategory("analytics"),
		mcp.WithDescription("Query all snapshots within a time range."),
		mcp.WithString("key", mcp.Required(), mcp.Description("Entity key to query")),
		mcp.WithString("type", mcp.Required(), mcp.Description("Snapshot type: topic, consumer_group")),
		mcp.WithString("from", mcp.Required(), mcp.Description("Range start (RFC3339)")),
		mcp.WithString("to", mcp.Required(), mcp.Description("Range end (RFC3339)")),
	), s.handleTimeTravelRange)

	s.mcpServer.AddTool(mcp.NewTool("kafka_timetravel_diff",
		withCategory("analytics"),
		mcp.WithDescription("Compare state at two points in time and return field-level changes."),
		mcp.WithString("key", mcp.Required(), mcp.Description("Entity key to diff")),
		mcp.WithString("type", mcp.Required(), mcp.Description("Snapshot type: topic, consumer_group")),
		mcp.WithString("t1", mcp.Required(), mcp.Description("First timestamp (RFC3339)")),
		mcp.WithString("t2", mcp.Required(), mcp.Description("Second timestamp (RFC3339)")),
	), s.handleTimeTravelDiff)

	s.mcpServer.AddTool(mcp.NewTool("kafka_timetravel_list",
		withCategory("analytics"),
		mcp.WithDescription("List all tracked entities, optionally filtered by type."),
		mcp.WithString("type", mcp.Description("Filter by snapshot type: topic, consumer_group. Empty returns all.")),
	), s.handleTimeTravelList)
}
