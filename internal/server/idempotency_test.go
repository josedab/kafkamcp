package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/josedab/kafkamcp/internal/engine/backend"
	"github.com/josedab/kafkamcp/internal/idempotency"
)

func TestEncodeDecodeCachedToolResult_RoundTrip(t *testing.T) {
	t.Parallel()
	orig := &mcp.CallToolResult{
		Content:           []mcp.Content{mcp.TextContent{Type: "text", Text: "hello"}},
		StructuredContent: map[string]any{"status": "ok"},
	}
	cached, err := encodeCachedToolResult(orig)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	raw, err := json.Marshal(cached)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	decoded, err := decodeCachedToolResult(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded.Content) != 1 {
		t.Fatalf("expected 1 content, got %d", len(decoded.Content))
	}
	if tc, ok := decoded.Content[0].(mcp.TextContent); !ok || tc.Text != "hello" {
		t.Errorf("expected text 'hello', got %#v", decoded.Content[0])
	}
}

func TestEncodeCachedToolResult_NilResult(t *testing.T) {
	t.Parallel()
	cached, err := encodeCachedToolResult(nil)
	if err != nil {
		t.Fatalf("encode nil: %v", err)
	}
	if len(cached.Text) != 0 || cached.StructuredContent != nil {
		t.Errorf("expected empty cached result, got %#v", cached)
	}
}

func TestIdempotencyManager_ReplayReturnsCachedResult(t *testing.T) {
	t.Parallel()
	mgr := idempotency.NewManager(backend.NewInMemory(), time.Hour)
	ctx := context.Background()

	receipt, replay, err := mgr.Begin(ctx, "key-abcdef12", "scope-1")
	if err != nil {
		t.Fatalf("first begin: %v", err)
	}
	if replay {
		t.Fatal("first begin should not be a replay")
	}
	if err := mgr.Complete(ctx, receipt, map[string]any{"ok": true}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	_, replay2, err := mgr.Begin(ctx, "key-abcdef12", "scope-1")
	if err != nil {
		t.Fatalf("second begin: %v", err)
	}
	if !replay2 {
		t.Error("second begin with same key+scope should replay")
	}
}

func TestIdempotencyManager_Abort(t *testing.T) {
	t.Parallel()
	mgr := idempotency.NewManager(backend.NewInMemory(), time.Hour)
	ctx := context.Background()

	receipt, _, err := mgr.Begin(ctx, "key-abcdef12", "scope-1")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := mgr.Abort(ctx, receipt); err != nil {
		t.Fatalf("abort: %v", err)
	}
	_, replay, err := mgr.Begin(ctx, "key-abcdef12", "scope-1")
	if err != nil {
		t.Fatalf("begin after abort: %v", err)
	}
	if replay {
		t.Error("begin after abort should not replay")
	}
}
