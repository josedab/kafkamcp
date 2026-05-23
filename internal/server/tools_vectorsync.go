package server

import (
	"context"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/vectorsync"
)

func (s *Server) handleVectorSyncCreate(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_vector_sync_create",
		resourceType: auth.ResourceTopic,
		action:       auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		name := request.GetString("name", "")
		sourceTopic := request.GetString("source_topic", "")
		vectorDB := request.GetString("vector_db", "")
		collection := request.GetString("collection", "")

		if name == "" || sourceTopic == "" || vectorDB == "" || collection == "" {
			return mcp.NewToolResultError("name, source_topic, vector_db, and collection are required"), nil
		}
		if deny := validateStringLength("name", name); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("source_topic", sourceTopic); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("vector_db", vectorDB); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("collection", collection); deny != nil {
			return deny, nil
		}

		embModel := request.GetString("embedding_model", "simple-hash")
		dimension := int(request.GetFloat("dimension", 384))

		vdb := vectorsync.VectorDBConfig{
			Type:       vectorsync.VectorDBType(vectorDB),
			Collection: collection,
		}
		emb := vectorsync.EmbeddingConfig{
			Model:     vectorsync.EmbeddingModel(embModel),
			Dimension: dimension,
		}

		bridge, err := s.vectorSyncEngine.CreateBridge(name, sourceTopic, agentID, vdb, emb)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_vector_sync_create", args, "", "error", 0, err, start)
			return s.toolError("Failed to create bridge", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_vector_sync_create", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{
			"status":  "created",
			"bridge":  bridge,
			"message": fmt.Sprintf("Vector sync bridge %q created: %s → %s/%s", name, sourceTopic, vectorDB, collection),
		})
	})(ctx, request)
}

func (s *Server) handleVectorSyncSearch(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_vector_sync_search",
		resourceType: auth.ResourceTopic,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		bridgeID := request.GetString("bridge_id", "")
		query := request.GetString("query", "")

		if bridgeID == "" || query == "" {
			return mcp.NewToolResultError("bridge_id and query are required"), nil
		}
		if deny := validateStringLength("bridge_id", bridgeID); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("query", query); deny != nil {
			return deny, nil
		}

		topK := int(request.GetFloat("top_k", 10))

		results, err := s.vectorSyncEngine.SearchSimilar(bridgeID, query, topK)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_vector_sync_search", args, "", "error", 0, err, start)
			return s.toolError("Search failed", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_vector_sync_search", args, "", "success", len(results), nil, start)
		return marshalResult(map[string]any{
			"results": results,
			"total":   len(results),
			"query":   query,
		})
	})(ctx, request)
}

func (s *Server) handleVectorSyncManage(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_vector_sync_manage",
		resourceType: auth.ResourceTopic,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "list")
		bridgeID := request.GetString("bridge_id", "")

		if deny := validateStringLength("bridge_id", bridgeID); deny != nil {
			return deny, nil
		}

		switch action {
		case "list":
			bridges := s.vectorSyncEngine.ListBridges()
			s.logAudit(ctx, agentID, "kafka_vector_sync_manage", args, "", "success", len(bridges), nil, start)
			return marshalResult(map[string]any{"bridges": bridges, "total": len(bridges)})

		case "get":
			if bridgeID == "" {
				return mcp.NewToolResultError("bridge_id is required"), nil
			}
			bridge, err := s.vectorSyncEngine.GetBridge(bridgeID)
			if err != nil {
				return s.toolError("Bridge not found", err), nil
			}
			return marshalResult(map[string]any{"bridge": bridge})

		case "pause":
			if err := s.vectorSyncEngine.UpdateBridgeStatus(bridgeID, "paused"); err != nil {
				return s.toolError("Failed to pause", err), nil
			}
			return marshalResult(map[string]any{"status": "paused", "bridge_id": bridgeID})

		case "resume":
			if err := s.vectorSyncEngine.UpdateBridgeStatus(bridgeID, "active"); err != nil {
				return s.toolError("Failed to resume", err), nil
			}
			return marshalResult(map[string]any{"status": "active", "bridge_id": bridgeID})

		case "stop":
			if err := s.vectorSyncEngine.UpdateBridgeStatus(bridgeID, "stopped"); err != nil {
				return s.toolError("Failed to stop", err), nil
			}
			return marshalResult(map[string]any{"status": "stopped", "bridge_id": bridgeID})

		case "delete":
			if err := s.vectorSyncEngine.DeleteBridge(bridgeID); err != nil {
				return s.toolError("Failed to delete", err), nil
			}
			return marshalResult(map[string]any{"status": "deleted", "bridge_id": bridgeID})

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q", action)), nil
		}
	})(ctx, request)
}
