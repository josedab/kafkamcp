package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerChaosTools() {
	s.addTool(mcp.NewTool("kafka_chaos_inject",
		withCategory("ops"),
		withTier(TierExperimental),
		mcp.WithDescription("Inject controlled chaos into a Kafka environment. Types: broker_outage, network_partition, slow_consumer, schema_corruption, rebalance_storm, producer_flood. Actions: inject (start experiment), scenario (run pre-built scenario), list_scenarios."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: inject, scenario, list_scenarios")),
		mcp.WithString("type", mcp.Description("Experiment type (for inject action)")),
		mcp.WithString("scenario", mcp.Description("Pre-built scenario name (for scenario action)")),
		mcp.WithString("cluster", mcp.Required(), mcp.Description("Target cluster name")),
		mcp.WithObject("topics", mcp.Description("Target topics array")),
		mcp.WithObject("groups", mcp.Description("Target consumer groups array")),
		mcp.WithObject("parameters", mcp.Description("Experiment parameters")),
		mcp.WithNumber("max_duration_minutes", mcp.Description("Max duration in minutes. Default: 10")),
		mcp.WithString("category", mcp.Description("Scenario category filter: network, infrastructure, data, consumer, producer")),
	), s.handleChaosInject)

	s.addTool(mcp.NewTool("kafka_chaos_status",
		withCategory("ops"),
		withTier(TierExperimental),
		mcp.WithDescription("Get the status of chaos experiments. Provide experiment_id for a specific experiment, or omit for all experiments."),
		mcp.WithString("experiment_id", mcp.Description("Specific experiment ID")),
		mcp.WithString("status_filter", mcp.Description("Filter by status: running, completed, rolled_back, stopped")),
	), s.handleChaosStatus)

	s.addTool(mcp.NewTool("kafka_chaos_stop",
		withCategory("ops"),
		withTier(TierExperimental),
		mcp.WithDescription("Stop a running chaos experiment and trigger rollback."),
		mcp.WithString("experiment_id", mcp.Required(), mcp.Description("Experiment ID to stop")),
	), s.handleChaosStop)
}
