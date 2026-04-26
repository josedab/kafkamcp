package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleMemoryStore(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	key, _ := args["key"].(string)
	value := args["value"]
	category, _ := args["category"].(string)

	if key == "" {
		return mcp.NewToolResultError("key is required"), nil
	}

	var tags []string
	if raw, ok := args["tags"]; ok {
		data, err := json.Marshal(raw)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("marshaling tags: %v", err)), nil
		}
		json.Unmarshal(data, &tags)
	}

	var ttlSec int64
	if v, ok := args["ttl_sec"].(float64); ok {
		ttlSec = int64(v)
	}

	var metadata map[string]string
	if raw, ok := args["metadata"].(map[string]any); ok {
		metadata = make(map[string]string)
		for k, v := range raw {
			if s, ok := v.(string); ok {
				metadata[k] = s
			}
		}
	}

	entry, err := s.agentMemoryEngine.Store(agentID, key, value, category, tags, ttlSec, metadata)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	s.logAudit(agentID, "kafka_memory_store", args, "", "success", 1, nil, start)
	return marshalResult(entry)
}

func (s *Server) handleMemoryRecall(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	action, _ := args["action"].(string)
	key, _ := args["key"].(string)
	category, _ := args["category"].(string)

	switch action {
	case "get":
		if key == "" {
			return mcp.NewToolResultError("key is required"), nil
		}
		entry, err := s.agentMemoryEngine.Recall(agentID, key)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		s.logAudit(agentID, "kafka_memory_recall", args, "", "success", 1, nil, start)
		return marshalResult(entry)

	case "list":
		entries := s.agentMemoryEngine.ListEntries(agentID, category)
		stats := s.agentMemoryEngine.Stats(agentID)
		s.logAudit(agentID, "kafka_memory_recall", args, "", "success", len(entries), nil, start)
		return marshalResult(map[string]any{
			"entries": entries,
			"stats":   stats,
		})

	case "delete":
		if key == "" {
			return mcp.NewToolResultError("key is required"), nil
		}
		if err := s.agentMemoryEngine.Delete(agentID, key); err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		s.logAudit(agentID, "kafka_memory_recall", args, "", "success", 1, nil, start)
		return marshalResult(map[string]string{"status": "deleted"})

	default:
		return mcp.NewToolResultError("action must be: get, list, or delete"), nil
	}
}

func (s *Server) handleMemorySearch(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	query, _ := args["query"].(string)
	category, _ := args["category"].(string)

	if query == "" {
		return mcp.NewToolResultError("query is required"), nil
	}

	limit := 10
	if v, ok := args["limit"].(float64); ok && v > 0 {
		limit = int(v)
	}

	results := s.agentMemoryEngine.Search(agentID, query, category, limit)

	s.logAudit(agentID, "kafka_memory_search", args, "", "success", len(results), nil, start)
	return marshalResult(map[string]any{
		"results": results,
		"total":   len(results),
		"query":   query,
	})
}
