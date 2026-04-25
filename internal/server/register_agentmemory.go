package server

import (
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerAgentMemoryTools() {
	if s.agentMemoryEngine == nil {
		return
	}

	s.mcpServer.AddTool(mcp.NewTool("kafka_memory_store",
		mcp.WithDescription("Store a memory entry for the current agent. Supports categories (preference, schema, query_history, context, note), tags, TTL expiration, and arbitrary key-value data."),
		mcp.WithString("key", mcp.Required(), mcp.Description("Memory key")),
		mcp.WithObject("value", mcp.Required(), mcp.Description("Value to store (any JSON)")),
		mcp.WithString("category", mcp.Description("Category: preference, schema, query_history, context, note")),
		mcp.WithObject("tags", mcp.Description("Tags for categorization")),
		mcp.WithNumber("ttl_sec", mcp.Description("Time-to-live in seconds (0 = no expiry)")),
		mcp.WithObject("metadata", mcp.Description("Additional metadata key-value pairs")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_memory_store",
		resourceType: "topic",
		action:       "write",
	}, s.handleMemoryStore))

	s.mcpServer.AddTool(mcp.NewTool("kafka_memory_recall",
		mcp.WithDescription("Recall a specific memory entry by key, or list all entries. Actions: get, list, delete."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: get, list, delete")),
		mcp.WithString("key", mcp.Description("Memory key (for get/delete)")),
		mcp.WithString("category", mcp.Description("Filter by category (for list)")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_memory_recall",
		resourceType: "topic",
		action:       "read",
	}, s.handleMemoryRecall))

	s.mcpServer.AddTool(mcp.NewTool("kafka_memory_search",
		mcp.WithDescription("Search agent memory using TF-IDF semantic search. Returns ranked results by relevance."),
		mcp.WithString("query", mcp.Required(), mcp.Description("Search query")),
		mcp.WithString("category", mcp.Description("Filter by category")),
		mcp.WithNumber("limit", mcp.Description("Max results (default: 10)")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_memory_search",
		resourceType: "topic",
		action:       "read",
	}, s.handleMemorySearch))
}
