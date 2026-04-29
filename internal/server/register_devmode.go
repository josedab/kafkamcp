package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerDevModeTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_dev_environment",
		withCategory("platform"),
		mcp.WithDescription("Get the local development environment status, sample topics, quick start guide, and setup instructions for offline-first development."),
	), s.handleDevEnvironment)

	s.mcpServer.AddTool(mcp.NewTool("kafka_dev_sample_data",
		withCategory("platform"),
		mcp.WithDescription("Generate sample data for local development. Available generators: ecommerce-orders, user-events, iot-sensors, click-stream, payment-events."),
		mcp.WithString("generator", mcp.Required(), mcp.Description("Data generator: ecommerce-orders, user-events, iot-sensors, click-stream, payment-events")),
		mcp.WithString("count", mcp.Description("Number of messages to generate (1-100). Default: 10")),
	), s.handleDevSampleData)

	s.mcpServer.AddTool(mcp.NewTool("kafka_dev_tutorial",
		withCategory("platform"),
		mcp.WithDescription("Interactive guided tutorials for learning KafkaMCP. List tutorials, start one, or get a specific tutorial's steps."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: list, get")),
		mcp.WithString("tutorial_id", mcp.Description("Tutorial ID (for get): getting-started, anomaly-detection, schema-management, smart-investigation, digital-twin")),
		mcp.WithString("difficulty", mcp.Description("Filter by difficulty: beginner, intermediate, advanced (for list)")),
	), s.handleDevTutorial)
}
