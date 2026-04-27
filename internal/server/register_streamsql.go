package server

import (
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerStreamSQLTools() {
	if s.streamSQLEngine == nil {
		return
	}

	s.mcpServer.AddTool(mcp.NewTool("kafka_sql_execute",
		withCategory("streaming"),
		mcp.WithDescription("Execute a streaming SQL query against Kafka topics. Supports SELECT, WHERE, GROUP BY, aggregations (COUNT/SUM/AVG/MIN/MAX), and WINDOW (TUMBLING/SLIDING/SESSION) clauses."),
		mcp.WithString("sql", mcp.Required(), mcp.Description("SQL query (e.g., SELECT * FROM topic WHERE field > 100 WINDOW TUMBLING 5m GROUP BY region)")),
		mcp.WithNumber("limit", mcp.Description("Max messages to process (default: 100)")),
		mcp.WithString("cluster", mcp.Description("Cluster name")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_sql_execute",
		resourceType: "topic",
		action:       "read",
	}, s.handleSQLExecute))

	s.mcpServer.AddTool(mcp.NewTool("kafka_sql_explain",
		withCategory("streaming"),
		mcp.WithDescription("Explain a streaming SQL query plan without executing it. Shows execution steps and estimated cost."),
		mcp.WithString("sql", mcp.Required(), mcp.Description("SQL query to explain")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_sql_explain",
		resourceType: "topic",
		action:       "read",
	}, s.handleSQLExplain))

	s.mcpServer.AddTool(mcp.NewTool("kafka_sql_manage",
		withCategory("streaming"),
		mcp.WithDescription("Manage SQL queries. Actions: list, get, delete."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: list, get, delete")),
		mcp.WithString("query_id", mcp.Description("Query ID")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_sql_manage",
		resourceType: "topic",
		action:       "read",
	}, s.handleSQLManage))
}
