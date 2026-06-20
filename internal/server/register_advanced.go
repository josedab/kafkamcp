package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerAdvancedTools() {
	// Kafka Connect management (handlers in tools_connect.go).
	s.addTool(mcp.NewTool("kafka_create_connector",
		withCategory("streaming"),
		mcp.WithDescription("Create a new Kafka Connect connector with the specified configuration."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Connector name")),
		mcp.WithObject("config", mcp.Required(), mcp.Description("Connector configuration map")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleCreateConnector)

	s.addTool(mcp.NewTool("kafka_update_connector",
		withCategory("streaming"),
		mcp.WithDescription("Update an existing Kafka Connect connector configuration."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Connector name")),
		mcp.WithObject("config", mcp.Required(), mcp.Description("New connector configuration map")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleUpdateConnector)

	s.addTool(mcp.NewTool("kafka_delete_connector",
		withCategory("streaming"),
		mcp.WithDescription("Delete a Kafka Connect connector."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Connector name")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleDeleteConnector)

	s.addTool(mcp.NewTool("kafka_restart_connector",
		withCategory("streaming"),
		mcp.WithDescription("Restart a Kafka Connect connector or specific tasks. Supports bulk restart of failed tasks."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Connector name")),
		mcp.WithNumber("task_id", mcp.Description("Specific task ID to restart (-1 for all). Default: -1")),
		mcp.WithBoolean("failed_only", mcp.Description("Only restart failed tasks. Default: false")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleRestartConnector)
}
