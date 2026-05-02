package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/benchmark"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleBenchmark(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_benchmark", resourceType: auth.ResourceCluster, action: auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		if action == "" {
			return mcp.NewToolResultError("action is required"), nil
		}

		switch action {
		case "run":
			topic := request.GetString("topic", "")
			cluster := request.GetString("cluster", "")
			if topic == "" {
				return mcp.NewToolResultError("topic is required"), nil
			}
			if deny := validateStringLength("topic", topic); deny != nil {
				return deny, nil
			}

			cfg := benchmark.BenchmarkConfig{
				Topic:            topic,
				MessageCount:     int(clampInt64(int64(request.GetInt("message_count", 1000)), 1, 100000)),
				MessageSizeBytes: int(clampInt64(int64(request.GetInt("message_size", 100)), 1, 1048576)),
				Concurrency:      int(clampInt64(int64(request.GetInt("concurrency", 1)), 1, 100)),
				Type:             request.GetString("type", "producer"),
			}

			bench := s.benchmarkEngine.CreateBenchmark(cfg)

			// Run the producer benchmark
			if cfg.Type == "producer" || cfg.Type == "both" {
				payload := s.benchmarkEngine.GeneratePayload(cfg.MessageSizeBytes)
				var latencies []float64
				var totalBytes int64
				var errorCount int

				for i := 0; i < cfg.MessageCount; i++ {
					msgStart := time.Now()
					_, err := s.kafkaManager.Produce(ctx, cluster, topic, []kafka.ProduceMessage{
						{Value: string(payload)},
					}, kafka.ProduceOptions{})
					latency := time.Since(msgStart).Seconds() * 1000
					latencies = append(latencies, latency)
					totalBytes += int64(len(payload))
					if err != nil {
						errorCount++
					}
				}

				if err := s.benchmarkEngine.RecordProducerMetrics(bench.ID, latencies, totalBytes, errorCount); err != nil {
					return s.toolError("Failed to record metrics", err), nil
				}
			}

			// Run the consumer benchmark
			if cfg.Type == "consumer" || cfg.Type == "both" {
				msgStart := time.Now()
				messages, err := s.kafkaManager.Consume(ctx, cluster, topic, -1, "earliest", cfg.MessageCount, 10000, "auto")
				latency := time.Since(msgStart).Seconds() * 1000

				var latencies []float64
				var totalBytes int64
				var errorCount int

				if err != nil {
					errorCount = 1
					latencies = append(latencies, latency)
				} else {
					perMsg := latency / float64(len(messages))
					for range messages {
						latencies = append(latencies, perMsg)
						totalBytes += int64(cfg.MessageSizeBytes)
					}
				}

				if err := s.benchmarkEngine.RecordConsumerMetrics(bench.ID, latencies, totalBytes, errorCount); err != nil {
					return s.toolError("Failed to record consumer metrics", err), nil
				}
			}

			result, _ := s.benchmarkEngine.GetResult(bench.ID)
			s.logAudit(agentID, "kafka_benchmark", args, cluster, "success", cfg.MessageCount, nil, start)
			return marshalResult(result)

		case "status":
			benchID := request.GetString("bench_id", "")
			if benchID == "" {
				return mcp.NewToolResultError("bench_id is required"), nil
			}
			result, err := s.benchmarkEngine.GetResult(benchID)
			if err != nil {
				return s.toolError("Benchmark not found", err), nil
			}
			return marshalResult(result)

		case "list":
			results := s.benchmarkEngine.ListResults(20)
			return marshalResult(map[string]any{"benchmarks": results, "total": len(results)})

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q, use: run, status, list", action)), nil
		}
	})(ctx, request)
}
