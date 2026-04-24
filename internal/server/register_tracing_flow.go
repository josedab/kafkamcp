package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerTracingFlowTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_trace_flow",
		mcp.WithDescription("Distributed tracing for Kafka messages: inject W3C Trace Context headers, correlate across topic chains, and visualize message flow graphs. Actions: trace (get trace by ID), record (record a span), create_context (generate trace context), list (recent traces)."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: trace, record, create_context, list")),
		mcp.WithString("trace_id", mcp.Description("Trace ID (required for trace action)")),
		mcp.WithString("operation", mcp.Description("Operation: produce, consume, transform (for record action)")),
		mcp.WithString("topic", mcp.Description("Topic name (for record action)")),
		mcp.WithString("producer", mcp.Description("Producer ID (for record action)")),
		mcp.WithString("consumer", mcp.Description("Consumer ID (for record action)")),
		mcp.WithString("parent_span_id", mcp.Description("Parent span ID for child context")),
		mcp.WithNumber("partition", mcp.Description("Partition number")),
		mcp.WithNumber("offset", mcp.Description("Message offset")),
		mcp.WithNumber("duration_ms", mcp.Description("Operation duration in ms")),
		mcp.WithNumber("limit", mcp.Description("Max traces to list. Default: 50")),
	), s.handleTraceFlow)
}
