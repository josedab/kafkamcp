package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/viewstore"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleViewCreate(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	name, _ := args["name"].(string)
	sourceTopic, _ := args["source_topic"].(string)
	keyField, _ := args["key_field"].(string)
	aggType, _ := args["aggregation_type"].(string)
	windowType, _ := args["window_type"].(string)
	filterExpr, _ := args["filter_expr"].(string)
	description, _ := args["description"].(string)

	if name == "" || sourceTopic == "" {
		return mcp.NewToolResultError("name and source_topic are required"), nil
	}

	var windowSizeMs, retentionMs int64
	if v, ok := args["window_size_ms"].(float64); ok {
		windowSizeMs = int64(v)
	}
	if v, ok := args["retention_ms"].(float64); ok {
		retentionMs = int64(v)
	}

	var groupByFields, valueFields []string
	if raw, ok := args["group_by_fields"]; ok {
		data, err := json.Marshal(raw)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("marshaling group_by_fields: %v", err)), nil
		}
		json.Unmarshal(data, &groupByFields)
	}
	if raw, ok := args["value_fields"]; ok {
		data, err := json.Marshal(raw)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("marshaling value_fields: %v", err)), nil
		}
		json.Unmarshal(data, &valueFields)
	}

	view, err := s.viewStoreEngine.CreateView(name, sourceTopic, keyField, aggType, windowType, windowSizeMs, retentionMs, groupByFields, valueFields, filterExpr, description, agentID)
	if err != nil {
		return s.toolError("failed to create view", err), nil
	}

	s.logAudit(agentID, "kafka_view_create", args, "", "success", 1, nil, start)
	return marshalResult(view)
}

func (s *Server) handleViewQuery(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	viewID, _ := args["view_id"].(string)
	if viewID == "" {
		return mcp.NewToolResultError("view_id is required"), nil
	}

	opts := viewstore.QueryOptions{
		KeyExact:  request.GetString("key_exact", ""),
		KeyPrefix: request.GetString("key_prefix", ""),
	}
	if v, ok := args["limit"].(float64); ok {
		opts.Limit = int(v)
	}
	if opts.Limit < 0 {
		opts.Limit = 0
	}
	if opts.Limit > 10000 {
		opts.Limit = 10000
	}
	if v, ok := args["offset"].(float64); ok {
		opts.Offset = int(v)
	}
	if opts.Offset < 0 {
		opts.Offset = 0
	}
	if v, ok := args["descending"].(bool); ok {
		opts.Descending = v
	}

	result, err := s.viewStoreEngine.Query(viewID, opts)
	if err != nil {
		return s.toolError("view query failed", err), nil
	}

	s.logAudit(agentID, "kafka_view_query", args, "", "success", result.TotalCount, nil, start)
	return marshalResult(result)
}

func (s *Server) handleViewStatus(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	action, _ := args["action"].(string)
	viewID, _ := args["view_id"].(string)

	switch action {
	case "list":
		views := s.viewStoreEngine.ListViews()
		s.logAudit(agentID, "kafka_view_status", args, "", "success", len(views), nil, start)
		return marshalResult(map[string]any{"views": views, "total": len(views)})

	case "get":
		if viewID == "" {
			return mcp.NewToolResultError("view_id is required"), nil
		}
		view, err := s.viewStoreEngine.GetView(viewID)
		if err != nil {
			return s.toolError("view not found", err), nil
		}
		s.logAudit(agentID, "kafka_view_status", args, "", "success", 1, nil, start)
		return marshalResult(view)

	case "delete":
		if viewID == "" {
			return mcp.NewToolResultError("view_id is required"), nil
		}
		if err := s.viewStoreEngine.DeleteView(viewID); err != nil {
			return s.toolError("failed to delete view", err), nil
		}
		s.logAudit(agentID, "kafka_view_status", args, "", "success", 1, nil, start)
		return marshalResult(map[string]string{"status": "deleted"})

	case "stats":
		if viewID == "" {
			return mcp.NewToolResultError("view_id is required"), nil
		}
		stats, err := s.viewStoreEngine.Stats(viewID)
		if err != nil {
			return s.toolError("failed to retrieve view stats", err), nil
		}
		s.logAudit(agentID, "kafka_view_status", args, "", "success", 1, nil, start)
		return marshalResult(stats)

	default:
		return mcp.NewToolResultError("action must be: list, get, delete, or stats"), nil
	}
}
