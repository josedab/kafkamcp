package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/crosslineage"
	"github.com/mark3labs/mcp-go/mcp"
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
			s.logAudit(agentID, "kafka_cross_lineage", args, "", "success", 1, nil, start)
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
			s.logAudit(agentID, "kafka_cross_lineage", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "added", "edge": edge})

		case "graph":
			graph := s.crossLineageEngine.GetGraph()
			s.logAudit(agentID, "kafka_cross_lineage", args, "", "success", graph.TotalNodes, nil, start)
			return marshalResult(graph)

		case "trace":
			name := request.GetString("name", "")
			if name == "" {
				return mcp.NewToolResultError("name (topic name) is required"), nil
			}
			maxDepth := request.GetInt("max_depth", 5)
			lineage := s.crossLineageEngine.TraceLineage(name, maxDepth)
			s.logAudit(agentID, "kafka_cross_lineage", args, "", "success", 1, nil, start)
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
