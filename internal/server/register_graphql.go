package server

import (
	mcp "github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerGraphQLTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_graphql_schema",
		withCategory("api"),
		mcp.WithDescription("Get the GraphQL schema for KafkaMCP tools. Returns types, queries, mutations, and subscriptions in JSON or SDL format."),
		mcp.WithString("format", mcp.Description("Output format: json, sdl. Default: json")),
	), s.handleGraphQLSchema)

	s.mcpServer.AddTool(mcp.NewTool("kafka_graphql_query",
		withCategory("api"),
		mcp.WithDescription("Execute a GraphQL query against KafkaMCP tools. Supports introspection and tool resolution."),
		mcp.WithString("query", mcp.Required(), mcp.Description("GraphQL query string")),
	), s.handleGraphQLQuery)

	s.mcpServer.AddTool(mcp.NewTool("kafka_graphql_subscribe",
		withCategory("api"),
		mcp.WithDescription("Manage GraphQL subscriptions for real-time Kafka updates."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: subscribe, unsubscribe, list")),
		mcp.WithString("subscription", mcp.Description("Subscription name: onMessage, onConsumerLag, onClusterHealth, onComplianceAlert")),
		mcp.WithString("topic", mcp.Description("Topic to subscribe to (for onMessage)")),
		mcp.WithString("subscriber_id", mcp.Description("Subscriber ID (for unsubscribe)")),
	), s.handleGraphQLSubscribe)
}
