package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerMsgSamplingTools() {
	s.addTool(mcp.NewTool("kafka_message_sample",
		withCategory("analytics"),
		mcp.WithDescription("Statistical sampling and profiling of messages on a topic. Provides schema distribution, message size histograms, key cardinality analysis, and throughput profiling without consuming entire topics."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: sample, cached")),
		mcp.WithString("topic", mcp.Description("Topic name to sample (required for sample action)")),
		mcp.WithNumber("sample_size", mcp.Description("Number of messages to sample (default: 100, max: 1000)")),
		mcp.WithString("cluster", mcp.Description("Cluster name for multi-cluster setups.")),
	), s.handleMessageSample)
}
