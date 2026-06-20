package server

import (
	"context"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
)

// ===================== Helper =====================

func makeResourceRequest(uri string) mcp.ReadResourceRequest {
	req := mcp.ReadResourceRequest{}
	req.Params.URI = uri
	return req
}

// ===================== handleAuditLogResource =====================
// handleAuditLogResource does not call logAudit on the success path.

func TestHandleAuditLogResource_NoDeny(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	contents, err := s.handleAuditLogResource(ctx, makeResourceRequest("kafka://audit/log"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(contents) == 0 {
		t.Error("expected non-empty contents")
	}
}

// ===================== handleReplicationStatus =====================
// replicationMon is nil in test server so handler returns early.

// ===================== handleTopicResource =====================
// URI validation errors are returned before logAudit is called.

func TestHandleTopicResource_EmptyName(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	_, err := s.handleTopicResource(ctx, makeResourceRequest("kafka://topics/"))
	if err == nil {
		t.Error("expected error for empty topic name")
	}
}

func TestHandleTopicResource_InvalidURI(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	_, err := s.handleTopicResource(ctx, makeResourceRequest("bad-uri"))
	if err == nil {
		t.Error("expected error for invalid URI")
	}
}

func TestHandleTopicResource_NameTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	longName := strings.Repeat("a", maxInputLength+1)
	_, err := s.handleTopicResource(ctx, makeResourceRequest("kafka://topics/"+longName))
	if err == nil {
		t.Error("expected error for topic name exceeding max length")
	}
}

// ===================== handleGroupResource =====================
// URI validation errors are returned before logAudit is called.

func TestHandleGroupResource_InvalidURI(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	_, err := s.handleGroupResource(ctx, makeResourceRequest("bad-uri"))
	if err == nil {
		t.Error("expected error for invalid group URI")
	}
}

func TestHandleGroupResource_EmptyGroupID(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	_, err := s.handleGroupResource(ctx, makeResourceRequest("kafka://groups/"))
	if err == nil {
		t.Error("expected error for empty group ID")
	}
}

func TestHandleGroupResource_GroupIDTooLong(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	longID := strings.Repeat("g", maxInputLength+1)
	_, err := s.handleGroupResource(ctx, makeResourceRequest("kafka://groups/"+longID))
	if err == nil {
		t.Error("expected error for group ID exceeding max length")
	}
}

// ===================== WatchManager / MaskingEngine accessors =====================

func TestMaskingEngine_ReturnsNonNil(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	if s.MaskingEngine() == nil {
		t.Error("expected non-nil MaskingEngine")
	}
}

// ===================== Server accessor tests =====================

func TestMetrics_ReturnsNonNil(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	if s.Metrics() == nil {
		t.Error("expected non-nil Metrics")
	}
}

func TestMCPServer_ReturnsNil(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	// mcpServer is nil in test server (not initialized with mcp-go)
	if s.MCPServer() != nil {
		t.Error("expected nil MCPServer in test server")
	}
}

func TestSetOTelProvider(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	s.SetOTelProvider(nil)
	// Should not panic
}
