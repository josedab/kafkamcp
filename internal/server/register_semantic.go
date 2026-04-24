package server

import (
	"context"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerSemanticTools() {
	if s.semanticEngine == nil {
		return
	}

	s.mcpServer.AddTool(mcp.NewTool("kafka_semantic_index",
		mcp.WithDescription("Index Kafka messages for semantic search. Consumes recent messages from a topic and adds them to the vector index for meaning-based retrieval."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic to index messages from")),
		mcp.WithNumber("limit", mcp.Description("Max messages to index. Default: 100")),
		mcp.WithString("cluster", mcp.Description("Cluster name")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_semantic_index",
		resourceType: "topic",
		action:       "read",
		resourceName: func(r mcp.CallToolRequest) string {
			if t, ok := r.GetArguments()["topic"].(string); ok {
				return t
			}
			return "*"
		},
	}, s.handleSemanticIndex))

	s.mcpServer.AddTool(mcp.NewTool("kafka_semantic_search",
		mcp.WithDescription("Search indexed Kafka messages by meaning using semantic similarity. Returns the most relevant messages matching the query intent."),
		mcp.WithString("query", mcp.Required(), mcp.Description("Natural language search query")),
		mcp.WithString("topic", mcp.Description("Filter results to a specific topic")),
		mcp.WithNumber("limit", mcp.Description("Max results. Default: 10")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_semantic_search",
		resourceType: "topic",
		action:       "read",
	}, s.handleSemanticSearch))

	s.mcpServer.AddTool(mcp.NewTool("kafka_rag_context",
		mcp.WithDescription("Build RAG (Retrieval-Augmented Generation) context from indexed Kafka messages. Returns relevant message fragments optimized for LLM context windows."),
		mcp.WithString("query", mcp.Required(), mcp.Description("Query to build context for")),
		mcp.WithString("topic", mcp.Description("Filter to specific topic")),
		mcp.WithNumber("max_tokens", mcp.Description("Max context tokens. Default: 4000")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_rag_context",
		resourceType: "topic",
		action:       "read",
	}, s.handleRAGContext))

	s.mcpServer.AddTool(mcp.NewTool("kafka_semantic_stats",
		mcp.WithDescription("Get statistics about the semantic search index including document counts, topic distribution, and vocabulary size."),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_semantic_stats",
		resourceType: "cluster",
		action:       "read",
	}, s.handleSemanticStats))
}

func (s *Server) handleSemanticIndex(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	topic, _ := args["topic"].(string)
	if topic == "" {
		return mcp.NewToolResultError("topic is required"), nil
	}

	cluster, _ := args["cluster"].(string)
	cluster = s.resolveClusterName(cluster)

	limit := 100
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
		if limit > 1000 {
			limit = 1000
		}
	}

	messages, err := s.kafkaManager.Consume(ctx, cluster, topic, -1, "latest", limit, 10000, "")
	if err != nil {
		s.logAudit(agentID, "kafka_semantic_index", args, cluster, "error", 0, err, start)
		return s.toolError("failed to consume messages for indexing", err), nil
	}

	indexed := 0
	for _, m := range messages {
		content := fmt.Sprintf("%v", m.Value)
		if content == "" || content == "<nil>" {
			continue
		}
		key := fmt.Sprintf("%v", m.Key)
		s.semanticEngine.IndexMessage(topic, m.Partition, m.Offset, key, content, nil)
		indexed++
	}

	s.logAudit(agentID, "kafka_semantic_index", args, cluster, "success", indexed, nil, start)
	return marshalResult(map[string]any{
		"indexed":    indexed,
		"topic":      topic,
		"stats":      s.semanticEngine.GetStats(),
	})
}

func (s *Server) handleSemanticSearch(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	query, _ := args["query"].(string)
	if query == "" {
		return mcp.NewToolResultError("query is required"), nil
	}

	topicFilter, _ := args["topic"].(string)
	limit := 10
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}

	results := s.semanticEngine.Search(query, topicFilter, limit)
	s.logAudit(agentID, "kafka_semantic_search", args, "", "success", len(results), nil, start)

	return marshalResult(map[string]any{
		"query":   query,
		"results": results,
		"total":   len(results),
	})
}

func (s *Server) handleRAGContext(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	query, _ := args["query"].(string)
	if query == "" {
		return mcp.NewToolResultError("query is required"), nil
	}

	topicFilter, _ := args["topic"].(string)
	maxTokens := 4000
	if t, ok := args["max_tokens"].(float64); ok && t > 0 {
		maxTokens = int(t)
	}

	ragCtx := s.semanticEngine.BuildRAGContext(query, topicFilter, maxTokens)
	s.logAudit(agentID, "kafka_rag_context", args, "", "success", len(ragCtx.Results), nil, start)

	return marshalResult(ragCtx)
}

func (s *Server) handleSemanticStats(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	stats := s.semanticEngine.GetStats()
	s.logAudit(agentID, "kafka_semantic_stats", args, "", "success", 1, nil, start)
	return marshalResult(stats)
}
