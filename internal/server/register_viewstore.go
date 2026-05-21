package server

import (
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerViewStoreTools() {
	if s.viewStoreEngine == nil {
		return
	}

	s.addTool(mcp.NewTool("kafka_view_create",
		withCategory("streaming"),
		mcp.WithDescription("Create a materialized view over a Kafka topic with projections, aggregations (count/sum/avg/min/max), windowed computations, and GROUP BY support for instant analytical queries."),
		mcp.WithString("name", mcp.Required(), mcp.Description("View name")),
		mcp.WithString("source_topic", mcp.Required(), mcp.Description("Source Kafka topic")),
		mcp.WithString("key_field", mcp.Description("Field to use as view key (default: _key)")),
		mcp.WithString("aggregation_type", mcp.Description("Aggregation: count, sum, avg, min, max, group_by")),
		mcp.WithString("window_type", mcp.Description("Window type: tumbling, sliding, session, none")),
		mcp.WithNumber("window_size_ms", mcp.Description("Window size in milliseconds")),
		mcp.WithObject("group_by_fields", mcp.Description("Fields for GROUP BY")),
		mcp.WithObject("value_fields", mcp.Description("Fields to project into the view")),
		mcp.WithString("filter_expr", mcp.Description("Filter expression")),
		mcp.WithString("description", mcp.Description("View description")),
		mcp.WithNumber("retention_ms", mcp.Description("Retention period in milliseconds")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_view_create",
		resourceType: "topic",
		action:       "write",
	}, s.handleViewCreate))

	s.addTool(mcp.NewTool("kafka_view_query",
		withCategory("streaming"),
		mcp.WithDescription("Query a materialized view with point lookups, range scans, or full scans. Returns instant results without re-scanning the source topic."),
		mcp.WithString("view_id", mcp.Required(), mcp.Description("View ID to query")),
		mcp.WithString("key_exact", mcp.Description("Exact key lookup")),
		mcp.WithString("key_prefix", mcp.Description("Key prefix for range scan")),
		mcp.WithNumber("limit", mcp.Description("Max results (default: 100)")),
		mcp.WithNumber("offset", mcp.Description("Pagination offset")),
		mcp.WithBoolean("descending", mcp.Description("Reverse sort order")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_view_query",
		resourceType: "topic",
		action:       "read",
	}, s.handleViewQuery))

	s.addTool(mcp.NewTool("kafka_view_status",
		withCategory("streaming"),
		mcp.WithDescription("Manage materialized views. Actions: list, get, delete, stats."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: list, get, delete, stats")),
		mcp.WithString("view_id", mcp.Description("View ID")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_view_status",
		resourceType: "topic",
		action:       "read",
	}, s.handleViewStatus))
}
