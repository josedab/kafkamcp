package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerFinOpsTools() {
	s.addTool(mcp.NewTool("kafka_finops_summary",
		withCategory("analytics"),
		mcp.WithDescription("Get a cost intelligence summary including total spend, topic costs, optimization opportunities, and budget alerts."),
		mcp.WithString("action", mcp.Description("Action: summary, snapshot, recommendations, forecast, budgets. Default: summary")),
		mcp.WithNumber("months", mcp.Description("Forecast months (for forecast action). Default: 6")),
	), s.handleFinOpsSummary)

	s.addTool(mcp.NewTool("kafka_finops_budget",
		withCategory("analytics"),
		mcp.WithDescription("Set a budget alert for Kafka infrastructure spend."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Budget alert name")),
		mcp.WithNumber("threshold", mcp.Required(), mcp.Description("Monthly spend threshold in USD")),
	), s.handleFinOpsBudget)

	s.addTool(mcp.NewTool("kafka_finops_record_cost",
		withCategory("analytics"),
		mcp.WithDescription("Record topic cost data for FinOps tracking and analysis."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
		mcp.WithNumber("monthly_cost", mcp.Required(), mcp.Description("Monthly cost in USD")),
		mcp.WithNumber("storage_gb", mcp.Description("Storage in GB")),
		mcp.WithNumber("partitions", mcp.Description("Number of partitions")),
		mcp.WithNumber("msgs_per_sec", mcp.Description("Messages per second")),
	), s.handleFinOpsRecordCost)
}
