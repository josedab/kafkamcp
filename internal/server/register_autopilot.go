package server

import (
	mcp "github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerAutopilotTools() {
	s.addTool(mcp.NewTool("kafka_autopilot_assess",
		withCategory("migration"),
		mcp.WithDescription("Assess source and target Kafka clusters for migration readiness. Analyzes topics, consumer groups, data sizes, and risks to produce a migration readiness report."),
		mcp.WithString("source_cluster", mcp.Required(), mcp.Description("Source Kafka cluster name")),
		mcp.WithString("target_cluster", mcp.Required(), mcp.Description("Target Kafka cluster name")),
	), s.handleAutopilotAssess)

	s.addTool(mcp.NewTool("kafka_autopilot_plan",
		withCategory("migration"),
		mcp.WithDescription("Generate a migration plan for an assessed autopilot session. Creates a detailed plan with topic, consumer group, and schema migration steps."),
		mcp.WithString("session_id", mcp.Required(), mcp.Description("Autopilot session ID from assessment")),
	), s.handleAutopilotPlan)

	s.addTool(mcp.NewTool("kafka_autopilot_dryrun",
		withCategory("migration"),
		mcp.WithDescription("Simulate the migration on a digital twin to validate the plan without affecting production. Reports simulation results, warnings, and diffs."),
		mcp.WithString("session_id", mcp.Required(), mcp.Description("Autopilot session ID")),
	), s.handleAutopilotDryRun)

	s.addTool(mcp.NewTool("kafka_autopilot_execute",
		withCategory("migration"),
		withTier(TierExperimental),
		mcp.WithDescription("Execute the migration plan with orchestrated rollback support. Supports dry-run mode for final validation before actual execution."),
		mcp.WithString("session_id", mcp.Required(), mcp.Description("Autopilot session ID")),
		mcp.WithBoolean("dry_run", mcp.Description("If true, validate without making changes. Default: false")),
	), s.handleAutopilotExecute)

	s.addTool(mcp.NewTool("kafka_autopilot_rollback",
		withCategory("migration"),
		withTier(TierExperimental),
		mcp.WithDescription("Rollback a completed migration, reverting all changes made during execution."),
		mcp.WithString("session_id", mcp.Required(), mcp.Description("Autopilot session ID")),
	), s.handleAutopilotRollback)

	s.addTool(mcp.NewTool("kafka_autopilot_status",
		withCategory("migration"),
		mcp.WithDescription("Get the current status and details of an autopilot migration session, or list all sessions."),
		mcp.WithString("session_id", mcp.Description("Autopilot session ID. If empty, lists all sessions.")),
	), s.handleAutopilotStatus)
}
