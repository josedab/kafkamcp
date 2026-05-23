package server

import (
	"context"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/crosslineage"
)

func (s *Server) handleCrossLineage(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_cross_lineage", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		if action == "" {
			return mcp.NewToolResultError("action is required"), nil
		}

		switch action {
		case "register_node":
			name := request.GetString("name", "")
			nodeType := request.GetString("node_type", "topic")
			if name == "" {
				return mcp.NewToolResultError("name is required"), nil
			}
			if deny := validateStringLength("name", name); deny != nil {
				return deny, nil
			}
			node := s.crossLineageEngine.RegisterNode(name, crosslineage.NodeType(nodeType), nil)
			s.logAudit(ctx, agentID, "kafka_cross_lineage", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "registered", "node": node})

		case "add_edge":
			sourceID := request.GetString("source_id", "")
			targetID := request.GetString("target_id", "")
			flowType := request.GetString("flow_type", "produces")
			label := request.GetString("label", "")

			if sourceID == "" || targetID == "" {
				return mcp.NewToolResultError("source_id and target_id are required"), nil
			}

			edge, err := s.crossLineageEngine.AddEdge(sourceID, targetID, label, flowType, nil)
			if err != nil {
				return s.toolError("Failed to add edge", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_cross_lineage", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "added", "edge": edge})

		case "graph":
			graph := s.crossLineageEngine.GetGraph()
			s.logAudit(ctx, agentID, "kafka_cross_lineage", args, "", "success", graph.TotalNodes, nil, start)
			return marshalResult(graph)

		case "trace":
			name := request.GetString("name", "")
			if name == "" {
				return mcp.NewToolResultError("name (topic name) is required"), nil
			}
			maxDepth := int(clampInt64(int64(request.GetInt("max_depth", 5)), 1, 50))
			lineage := s.crossLineageEngine.TraceLineage(name, maxDepth)
			s.logAudit(ctx, agentID, "kafka_cross_lineage", args, "", "success", 1, nil, start)
			return marshalResult(lineage)

		case "remove_node":
			id := request.GetString("id", "")
			if id == "" {
				return mcp.NewToolResultError("id is required"), nil
			}
			if err := s.crossLineageEngine.RemoveNode(id); err != nil {
				return s.toolError("Failed to remove node", err), nil
			}
			return marshalResult(map[string]any{"status": "removed", "id": id})

		case "remove_edge":
			id := request.GetString("id", "")
			if id == "" {
				return mcp.NewToolResultError("id is required"), nil
			}
			if err := s.crossLineageEngine.RemoveEdge(id); err != nil {
				return s.toolError("Failed to remove edge", err), nil
			}
			return marshalResult(map[string]any{"status": "removed", "id": id})

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q", action)), nil
		}
	})(ctx, request)
}

func (s *Server) handleLineageImpact(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_lineage_impact", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		nodeID := request.GetString("node_id", "")
		if nodeID == "" {
			return mcp.NewToolResultError("node_id is required"), nil
		}

		analysis, err := s.crossLineageEngine.AnalyzeImpact(nodeID)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_lineage_impact", args, "", "error", 0, err, start)
			return s.toolError("Failed to analyze impact", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_lineage_impact", args, "", "success", 1, nil, start)
		return marshalResult(analysis)
	})(ctx, request)
}

func (s *Server) handleLineageFindPath(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_lineage_find_path", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		sourceID := request.GetString("source_id", "")
		targetID := request.GetString("target_id", "")
		if sourceID == "" || targetID == "" {
			return mcp.NewToolResultError("source_id and target_id are required"), nil
		}

		maxDepth := 10
		if d, ok := args["max_depth"].(float64); ok && d > 0 {
			maxDepth = int(d)
		}

		result, err := s.crossLineageEngine.FindPaths(sourceID, targetID, maxDepth)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_lineage_find_path", args, "", "error", 0, err, start)
			return s.toolError("Failed to find paths", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_lineage_find_path", args, "", "success", len(result.Paths), nil, start)
		return marshalResult(result)
	})(ctx, request)
}

func (s *Server) handleLineageCrossCluster(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_lineage_cross_cluster", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		flows := s.crossLineageEngine.GetCrossClusterFlows()
		s.logAudit(ctx, agentID, "kafka_lineage_cross_cluster", args, "", "success", len(flows), nil, start)
		return marshalResult(map[string]any{"cross_cluster_flows": flows, "total": len(flows)})
	})(ctx, request)
}
