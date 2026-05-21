package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerSemanticTopicSearchTools() {
	if s.semanticEngine == nil {
		return
	}

	s.addTool(mcp.NewTool("kafka_topic_search",
		withCategory("discovery"),
		mcp.WithDescription("Find Kafka topics using natural language queries. Searches topic names, descriptions, tags, and metadata using semantic similarity instead of exact name matching. Actions: search, index_topic, index_all, list_indexed."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: search, index_topic, index_all, list_indexed")),
		mcp.WithString("query", mcp.Description("Natural language search query (for search action)")),
		mcp.WithString("topic", mcp.Description("Topic name (for index_topic action)")),
		mcp.WithString("description", mcp.Description("Topic description (for index_topic action)")),
		mcp.WithObject("tags", mcp.Description("Topic tags array (for index_topic action)")),
		mcp.WithString("cluster", mcp.Description("Cluster name")),
		mcp.WithNumber("limit", mcp.Description("Max results. Default: 10")),
	), s.handleTopicSearch)
}
