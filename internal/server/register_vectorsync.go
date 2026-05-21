package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerVectorSyncTools() {
	s.addTool(mcp.NewTool("kafka_vector_sync_create",
		withCategory("streaming"),
		mcp.WithDescription("Create a sync bridge that streams Kafka topic data into a vector database (Pinecone, Weaviate, Qdrant, Chroma, Milvus) with automatic embedding generation."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Bridge name")),
		mcp.WithString("source_topic", mcp.Required(), mcp.Description("Source Kafka topic")),
		mcp.WithString("vector_db", mcp.Required(), mcp.Description("Vector DB type: pinecone, weaviate, qdrant, chroma, milvus")),
		mcp.WithString("collection", mcp.Required(), mcp.Description("Target collection/index name")),
		mcp.WithString("embedding_model", mcp.Description("Embedding model: simple-hash, tfidf, openai, cohere. Default: simple-hash")),
		mcp.WithNumber("dimension", mcp.Description("Embedding dimension. Default: 384")),
	), s.handleVectorSyncCreate)

	s.addTool(mcp.NewTool("kafka_vector_sync_search",
		withCategory("analytics"),
		mcp.WithDescription("Perform semantic similarity search against synced vector data from Kafka topics."),
		mcp.WithString("bridge_id", mcp.Required(), mcp.Description("Vector sync bridge ID")),
		mcp.WithString("query", mcp.Required(), mcp.Description("Search query text")),
		mcp.WithNumber("top_k", mcp.Description("Number of results to return. Default: 10")),
	), s.handleVectorSyncSearch)

	s.addTool(mcp.NewTool("kafka_vector_sync_manage",
		withCategory("streaming"),
		mcp.WithDescription("Manage vector sync bridges: list, get, pause, resume, delete."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: list, get, pause, resume, stop, delete")),
		mcp.WithString("bridge_id", mcp.Description("Bridge ID (required for get/pause/resume/stop/delete)")),
	), s.handleVectorSyncManage)
}
