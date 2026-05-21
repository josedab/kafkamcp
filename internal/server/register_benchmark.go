package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerBenchmarkTools() {
	s.addTool(mcp.NewTool("kafka_benchmark",
		withCategory("ops"),
		mcp.WithDescription("Embedded benchmarking and load testing. Measure producer/consumer throughput, latency percentiles, and topic performance under configurable synthetic workloads."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: run, status, list")),
		mcp.WithString("topic", mcp.Description("Topic to benchmark (required for run)")),
		mcp.WithString("type", mcp.Description("Benchmark type: producer, consumer, both (default: producer)")),
		mcp.WithNumber("message_count", mcp.Description("Number of messages to produce (default: 1000, max: 100000)")),
		mcp.WithNumber("message_size", mcp.Description("Message size in bytes (default: 100, max: 1MB)")),
		mcp.WithNumber("concurrency", mcp.Description("Number of concurrent workers (default: 1, max: 20)")),
		mcp.WithString("bench_id", mcp.Description("Benchmark ID (for status action)")),
		mcp.WithString("cluster", mcp.Description("Cluster name for multi-cluster setups.")),
	), s.handleBenchmark)
}
