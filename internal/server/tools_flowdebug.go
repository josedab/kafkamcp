package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/flowdebug"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleFlowDebug(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_flow_debug", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		cluster := request.GetString("cluster", "")

		if deny := validateStringLength("action", action); deny != nil {
			return deny, nil
		}

		switch action {
		case "graph":
			// Collect latest metrics from cluster
			if err := s.collectFlowMetrics(ctx, cluster); err != nil {
				s.logger.Warn("failed to collect flow metrics", "error", err)
			}

			graph := s.flowDebugger.GetCurrentGraph()
			s.logAudit(ctx, agentID, "kafka_flow_debug", args, cluster, "success", len(graph.Nodes), nil, start)
			return marshalResult(graph)

		case "snapshots":
			sinceMinutes := 60.0
			if m, ok := args["since_minutes"].(float64); ok && m > 0 {
				sinceMinutes = m
			}
			limit := 30
			if l, ok := args["limit"].(float64); ok && l > 0 {
				limit = int(l)
			}
			since := time.Now().Add(-time.Duration(sinceMinutes) * time.Minute)
			snapshots := s.flowDebugger.GetSnapshots(since, time.Time{}, limit)
			s.logAudit(ctx, agentID, "kafka_flow_debug", args, cluster, "success", len(snapshots), nil, start)
			return marshalResult(map[string]any{"snapshots": snapshots, "total": len(snapshots)})

		case "config":
			if thresholds, ok := args["thresholds"].(map[string]any); ok && len(thresholds) > 0 {
				cfg := s.flowDebugger.GetBottleneckConfig()
				if v, ok := thresholds["lag_warning"].(float64); ok {
					cfg.LagWarning = int64(v)
				}
				if v, ok := thresholds["lag_critical"].(float64); ok {
					cfg.LagCritical = int64(v)
				}
				if v, ok := thresholds["error_rate_warning"].(float64); ok {
					cfg.ErrorRateWarning = v
				}
				if v, ok := thresholds["error_rate_critical"].(float64); ok {
					cfg.ErrorRateCritical = v
				}
				if v, ok := thresholds["latency_warning_ms"].(float64); ok {
					cfg.LatencyWarningMs = v
				}
				if v, ok := thresholds["latency_critical_ms"].(float64); ok {
					cfg.LatencyCriticalMs = v
				}
				if v, ok := thresholds["throughput_min_msg_sec"].(float64); ok {
					cfg.ThroughputMinMsg = v
				}
				s.flowDebugger.SetBottleneckConfig(cfg)
				s.logAudit(ctx, agentID, "kafka_flow_debug", args, cluster, "success", 1, nil, start)
				return marshalResult(map[string]any{"status": "updated", "config": cfg})
			}
			cfg := s.flowDebugger.GetBottleneckConfig()
			return marshalResult(map[string]any{"config": cfg})

		case "collect":
			if err := s.collectFlowMetrics(ctx, cluster); err != nil {
				s.logAudit(ctx, agentID, "kafka_flow_debug", args, cluster, "error", 0, err, start)
				return s.toolError("failed to collect flow metrics", err), nil
			}
			graph := s.flowDebugger.GetCurrentGraph()
			s.logAudit(ctx, agentID, "kafka_flow_debug", args, cluster, "success", len(graph.Nodes), nil, start)
			return marshalResult(map[string]any{"status": "collected", "nodes": len(graph.Nodes), "edges": len(graph.Edges)})

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; must be: graph, snapshots, config, collect", action)), nil
		}
	})(ctx, request)
}

func (s *Server) handleFlowBottlenecks(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_flow_bottlenecks", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		cluster := request.GetString("cluster", "")
		severityFilter := request.GetString("severity", "")
		typeFilter := request.GetString("type", "")

		// Collect latest metrics
		if err := s.collectFlowMetrics(ctx, cluster); err != nil {
			s.logger.Warn("failed to collect flow metrics", "error", err)
		}

		bottlenecks := s.flowDebugger.GetBottlenecks()

		// Apply filters
		var filtered []flowdebug.Bottleneck
		for _, b := range bottlenecks {
			if severityFilter != "" && b.Severity != severityFilter {
				continue
			}
			if typeFilter != "" && b.Type != typeFilter {
				continue
			}
			filtered = append(filtered, b)
		}

		s.logAudit(ctx, agentID, "kafka_flow_bottlenecks", args, cluster, "success", len(filtered), nil, start)
		return marshalResult(map[string]any{
			"bottlenecks": filtered,
			"total":       len(filtered),
			"thresholds":  s.flowDebugger.GetBottleneckConfig(),
		})
	})(ctx, request)
}

// collectFlowMetrics builds a flow graph from live cluster data.
func (s *Server) collectFlowMetrics(ctx context.Context, cluster string) error {
	topics, err := s.kafkaManager.ListTopics(ctx, cluster, false)
	if err != nil {
		return fmt.Errorf("listing topics: %w", err)
	}

	var nodes []flowdebug.FlowNode
	var edges []flowdebug.FlowEdge

	for _, t := range topics {
		node := flowdebug.FlowNode{
			ID:           "topic:" + t.Name,
			Type:         "topic",
			Name:         t.Name,
			Partitions:   t.Partitions,
			HealthStatus: "healthy",
		}
		if t.MessageCount > 0 {
			node.Throughput = float64(t.MessageCount) / 3600.0
		}
		nodes = append(nodes, node)
	}

	groups, err := s.kafkaManager.ListConsumerGroups(ctx, cluster)
	if err == nil {
		for _, g := range groups {
			detail, err := s.kafkaManager.DescribeConsumerGroup(ctx, cluster, g.GroupID)
			if err != nil {
				continue
			}

			node := flowdebug.FlowNode{
				ID:           "group:" + g.GroupID,
				Type:         "consumer_group",
				Name:         g.GroupID,
				ConsumerLag:  detail.TotalLag,
				HealthStatus: "healthy",
			}
			if detail.TotalLag > 100000 {
				node.HealthStatus = "critical"
			} else if detail.TotalLag > 10000 {
				node.HealthStatus = "warning"
			}
			nodes = append(nodes, node)

			// Create edges from topics to consumer groups
			topicSet := make(map[string]bool)
			for _, offset := range detail.Offsets {
				topicSet[offset.Topic] = true
			}
			for topic := range topicSet {
				edges = append(edges, flowdebug.FlowEdge{
					Source: "topic:" + topic,
					Target: "group:" + g.GroupID,
					Status: "active",
				})
			}
		}
	}

	graph := &flowdebug.FlowGraph{
		Nodes:     nodes,
		Edges:     edges,
		ClusterID: s.resolveClusterName(cluster),
	}
	s.flowDebugger.UpdateGraph(graph)
	return nil
}
