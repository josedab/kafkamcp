package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerDigitalTwinTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_twin_snapshot",
		withCategory("ops"),
		mcp.WithDescription("Capture a snapshot of the current cluster state (topics, consumer groups, brokers) for digital twin simulation. Creates an immutable baseline."),
		mcp.WithString("cluster", mcp.Description("Cluster name. Default: default cluster")),
	), s.handleTwinSnapshot)

	s.mcpServer.AddTool(mcp.NewTool("kafka_twin_simulate",
		withCategory("ops"),
		mcp.WithDescription("Simulate operations against a cluster digital twin without affecting production. Supports: create_topic, delete_topic, alter_partitions, alter_config, rebalance."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: create_twin, simulate, diff, migrate_plan, get, list, history")),
		mcp.WithString("snapshot_id", mcp.Description("Snapshot ID (for create_twin)")),
		mcp.WithString("twin_id", mcp.Description("Twin ID (for simulate/diff/migrate_plan/get/history)")),
		mcp.WithString("op_type", mcp.Description("Operation type (for simulate): create_topic, delete_topic, alter_partitions, alter_config, rebalance")),
		mcp.WithString("topic", mcp.Description("Topic name (for simulate)")),
		mcp.WithNumber("partitions", mcp.Description("Partition count (for simulate)")),
		mcp.WithNumber("replication_factor", mcp.Description("Replication factor (for simulate)")),
	), s.handleTwinSimulate)
}
