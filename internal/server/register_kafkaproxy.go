package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerKafkaProxyTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_proxy",
		withCategory("ops"),
		mcp.WithDescription("Kafka MCP Proxy Protocol. Transparent proxy that translates Kafka wire protocol to MCP tool calls, allowing existing Kafka clients to use KafkaMCP features (auth, audit, masking) without code changes."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: start, stop, status, connections, stats, configure")),
		mcp.WithString("listen_addr", mcp.Description("TCP listen address for proxy (default: :19092)")),
		mcp.WithString("broker_addr", mcp.Description("Upstream Kafka broker address (default: localhost:9092)")),
		mcp.WithNumber("max_connections", mcp.Description("Maximum concurrent connections (default: 1000)")),
		mcp.WithBoolean("enable_audit", mcp.Description("Enable audit logging for proxied requests (default: true)")),
		mcp.WithBoolean("enable_masking", mcp.Description("Enable PII masking on proxied responses (default: false)")),
	), s.handleKafkaProxy)
}
