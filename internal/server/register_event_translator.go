package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerEventTranslatorTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_event_translate",
		withCategory("gateway"),
		mcp.WithDescription("Cross-platform event translator between Kafka and other messaging systems (Pulsar, EventHubs, Kinesis, NATS, RabbitMQ). Actions: create_rule, list_rules, execute, pause, resume, delete, stats, dry_run, protocol_map."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action to perform")),
		mcp.WithString("rule_id", mcp.Description("Rule ID")),
		mcp.WithString("name", mcp.Description("Rule name")),
		mcp.WithString("source_backend", mcp.Description("Source backend name")),
		mcp.WithString("source_topic", mcp.Description("Source topic")),
		mcp.WithString("target_backend", mcp.Description("Target backend name")),
		mcp.WithString("target_topic", mcp.Description("Target topic")),
		mcp.WithObject("transform", mcp.Description("Transform config")),
		mcp.WithNumber("limit", mcp.Description("Max messages per execution")),
		mcp.WithString("source_protocol", mcp.Description("Source protocol for mapping")),
		mcp.WithString("target_protocol", mcp.Description("Target protocol for mapping")),
	), s.handleEventTranslate)
}
