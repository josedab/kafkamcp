package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerCostOptimizerTools() {
	s.addTool(mcp.NewTool("kafka_sql_optimize",
		withCategory("streaming"),
		mcp.WithDescription("Analyze a streaming SQL query and generate a cost-aware execution plan with budget enforcement. Shows estimated cost, data volume, optimizations, and budget status."),
		mcp.WithString("sql", mcp.Required(), mcp.Description("SQL query to optimize")),
		mcp.WithString("topic", mcp.Description("Topic for statistics lookup")),
		mcp.WithString("budget_id", mcp.Description("Budget ID for cost enforcement")),
	), s.handleSQLOptimize)

	s.addTool(mcp.NewTool("kafka_sql_budget",
		withCategory("streaming"),
		mcp.WithDescription("Manage query cost budgets: create, get, or list budgets with spending limits and alert thresholds."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: create, get, list")),
		mcp.WithString("budget_id", mcp.Description("Budget ID (for get)")),
		mcp.WithString("name", mcp.Description("Budget name (for create)")),
		mcp.WithNumber("max_cost_per_query", mcp.Description("Maximum cost per query (for create)")),
		mcp.WithNumber("max_cost_per_hour", mcp.Description("Maximum cost per hour (for create)")),
	), s.handleSQLBudget)
}
