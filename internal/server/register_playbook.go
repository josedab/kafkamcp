package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerPlaybookTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_playbook_list",
		withCategory("ops"),
		mcp.WithDescription("List anomaly response playbooks. Playbooks define automated remediation steps triggered by specific anomaly patterns like high consumer lag, broker failures, or throughput anomalies."),
		mcp.WithString("trigger_type", mcp.Description("Filter by trigger type: high_consumer_lag, broker_down, partition_skew, throughput_drop, throughput_spike, under_replicated, disk_usage_high, schema_error")),
	), s.handlePlaybookList)

	s.mcpServer.AddTool(mcp.NewTool("kafka_playbook_execute",
		withCategory("ops"),
		mcp.WithDescription("Execute an anomaly response playbook. Triggers automated remediation steps for a detected anomaly."),
		mcp.WithString("playbook_id", mcp.Required(), mcp.Description("Playbook ID to execute")),
		mcp.WithString("trigger_info", mcp.Description("JSON string with trigger context information")),
	), s.handlePlaybookExecute)

	s.mcpServer.AddTool(mcp.NewTool("kafka_playbook_match",
		withCategory("ops"),
		mcp.WithDescription("Find playbooks matching a specific anomaly trigger type and value."),
		mcp.WithString("trigger_type", mcp.Required(), mcp.Description("Anomaly trigger type")),
		mcp.WithNumber("value", mcp.Description("Anomaly value to match against thresholds")),
	), s.handlePlaybookMatch)

	s.mcpServer.AddTool(mcp.NewTool("kafka_playbook_manage",
		withCategory("ops"),
		mcp.WithDescription("Manage playbooks and executions: get status, complete steps, enable/disable, list executions."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: get, enable, disable, delete, executions, complete_step")),
		mcp.WithString("id", mcp.Required(), mcp.Description("Playbook ID or execution ID")),
	), s.handlePlaybookManage)
}
