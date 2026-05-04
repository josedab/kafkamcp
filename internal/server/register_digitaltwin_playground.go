package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerDigitalTwinPlaygroundTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_twin_playground",
		withCategory("ops"),
		mcp.WithDescription("Safe simulation playground for Kafka topologies. Actions: create, run_scenario, analyze_impact, list_scenarios, reset."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: create, run_scenario, analyze_impact, list_scenarios, reset")),
		mcp.WithString("cluster_name", mcp.Description("Cluster name for playground")),
		mcp.WithObject("topics", mcp.Description("Map of topic name -> partition count (for create)")),
		mcp.WithNumber("brokers", mcp.Description("Number of brokers. Default: 3")),
		mcp.WithString("twin_id", mcp.Description("Twin ID")),
		mcp.WithString("scenario_id", mcp.Description("Scenario ID")),
		mcp.WithObject("params", mcp.Description("Scenario parameters")),
		mcp.WithObject("operations", mcp.Description("Operations for impact analysis")),
		mcp.WithString("category", mcp.Description("Scenario category filter: scaling, resilience, migration, optimization")),
	), s.handleTwinPlayground)
}
