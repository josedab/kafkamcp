package server

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/josedab/kafkamcp/internal/approval"
	"github.com/josedab/kafkamcp/internal/idempotency"
)

const idempotencyKeyArg = "idempotency_key"

type cachedToolResult struct {
	Text              []string        `json:"text,omitempty"`
	StructuredContent json.RawMessage `json:"structured_content,omitempty"`
}

func (s *Server) beginIdempotentMutation(ctx context.Context, tool, agentID string, args map[string]any) (idempotency.Receipt, *mcp.CallToolResult, error) {
	if s.idempotencyManager == nil || !isIdempotentWriteTool(tool) {
		return idempotency.Receipt{}, nil, nil
	}
	key, _ := args[idempotencyKeyArg].(string)
	if key == "" {
		return idempotency.Receipt{}, nil, nil
	}
	if len(key) < 8 || len(key) > 128 {
		return idempotency.Receipt{}, nil, fmt.Errorf("%s must be between 8 and 128 characters", idempotencyKeyArg)
	}
	scope, err := s.approvalScopeFor(ctx, tool, agentID, args)
	if err != nil {
		return idempotency.Receipt{}, nil, err
	}
	scopeDigest := approval.DigestArgs(map[string]any{
		"agent_id":        scope.AgentID,
		"identity_method": scope.IdentityMethod,
		"tool":            scope.Tool,
		"cluster":         scope.Cluster,
		"resource":        scope.Resource,
		"args_digest":     scope.ArgsDigest,
	})
	receipt, replay, err := s.idempotencyManager.Begin(ctx, key, scopeDigest)
	if err != nil {
		return idempotency.Receipt{}, nil, err
	}
	if !replay {
		return receipt, nil, nil
	}
	result, err := decodeCachedToolResult(receipt.Result)
	if err != nil {
		return idempotency.Receipt{}, nil, err
	}
	return idempotency.Receipt{}, result, nil
}

func (s *Server) completeIdempotentMutation(ctx context.Context, receipt idempotency.Receipt, result *mcp.CallToolResult) error {
	if receipt.KeyHash == "" || s.idempotencyManager == nil {
		return nil
	}
	cached, err := encodeCachedToolResult(result)
	if err != nil {
		return err
	}
	return s.idempotencyManager.Complete(ctx, receipt, cached)
}

func (s *Server) abortIdempotentMutation(ctx context.Context, receipt idempotency.Receipt) {
	if receipt.KeyHash == "" || s.idempotencyManager == nil {
		return
	}
	if err := s.idempotencyManager.Abort(ctx, receipt); err != nil && s.logger != nil {
		s.logger.Error("failed to abort idempotency reservation", "error", err)
	}
}

func encodeCachedToolResult(result *mcp.CallToolResult) (cachedToolResult, error) {
	if result == nil {
		return cachedToolResult{}, nil
	}
	cached := cachedToolResult{}
	for _, content := range result.Content {
		switch text := content.(type) {
		case mcp.TextContent:
			cached.Text = append(cached.Text, text.Text)
		case *mcp.TextContent:
			cached.Text = append(cached.Text, text.Text)
		default:
			return cachedToolResult{}, fmt.Errorf("idempotency: unsupported result content type %T", content)
		}
	}
	if result.StructuredContent != nil {
		raw, err := json.Marshal(result.StructuredContent)
		if err != nil {
			return cachedToolResult{}, fmt.Errorf("idempotency: marshal structured result: %w", err)
		}
		cached.StructuredContent = raw
	}
	return cached, nil
}

func decodeCachedToolResult(raw json.RawMessage) (*mcp.CallToolResult, error) {
	var cached cachedToolResult
	if err := json.Unmarshal(raw, &cached); err != nil {
		return nil, fmt.Errorf("idempotency: decode cached result: %w", err)
	}
	result := &mcp.CallToolResult{}
	for _, text := range cached.Text {
		result.Content = append(result.Content, mcp.TextContent{Type: "text", Text: text})
	}
	if len(cached.StructuredContent) > 0 {
		var structured any
		if err := json.Unmarshal(cached.StructuredContent, &structured); err != nil {
			return nil, fmt.Errorf("idempotency: decode structured result: %w", err)
		}
		result.StructuredContent = structured
	}
	return result, nil
}
