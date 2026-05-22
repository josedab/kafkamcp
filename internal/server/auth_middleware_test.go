package server

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

// ===================== sanitizeAgentID tests =====================

func TestSanitizeAgentID_ValidIDs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    string
		expected string
	}{
		{"agent-1", "agent-1"},
		{"Agent_2", "Agent_2"},
		{"my.agent.id", "my.agent.id"},
		{"abc123", "abc123"},
		{"A", "A"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := sanitizeAgentID(tt.input)
			if got != tt.expected {
				t.Errorf("sanitizeAgentID(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestSanitizeAgentID_Truncation(t *testing.T) {
	t.Parallel()
	longID := strings.Repeat("a", 200)
	got := sanitizeAgentID(longID)
	if len(got) > 128 {
		t.Errorf("expected truncation to 128, got length %d", len(got))
	}
}

func TestSanitizeAgentID_SpecialChars(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    string
		expected string
	}{
		{"agent@evil", "anonymous"},
		{"agent<script>", "anonymous"},
		{"agent with spaces", "anonymous"},
		{"agent/path", "anonymous"},
		{"agent;drop", "anonymous"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := sanitizeAgentID(tt.input)
			if got != tt.expected {
				t.Errorf("sanitizeAgentID(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestSanitizeAgentID_EmptyString(t *testing.T) {
	t.Parallel()
	// Empty string has no invalid chars, returns empty
	got := sanitizeAgentID("")
	if got != "" {
		t.Errorf("sanitizeAgentID(\"\") = %q, want \"\"", got)
	}
}

func TestSanitizeAgentID_AllInvalidChars(t *testing.T) {
	t.Parallel()
	got := sanitizeAgentID("@@@")
	if got != "anonymous" {
		t.Errorf("sanitizeAgentID(\"@@@\") = %q, want \"anonymous\"", got)
	}
}

// ===================== extractAgentID tests =====================

func TestExtractAgentID_FromHeader(t *testing.T) {
	t.Parallel()
	req := mcp.CallToolRequest{}
	req.Header = http.Header{}
	req.Header.Set("X-Agent-ID", "my-agent")

	got := extractAgentID(req)
	if got != "my-agent" {
		t.Errorf("extractAgentID = %q, want \"my-agent\"", got)
	}
}

func TestExtractAgentID_FromClientID(t *testing.T) {
	t.Parallel()
	req := mcp.CallToolRequest{}
	req.Header = http.Header{}
	req.Header.Set("X-Client-ID", "client-1")

	got := extractAgentID(req)
	if got != "client-1" {
		t.Errorf("extractAgentID = %q, want \"client-1\"", got)
	}
}

func TestExtractAgentID_NoHeader(t *testing.T) {
	t.Parallel()
	req := mcp.CallToolRequest{}

	got := extractAgentID(req)
	if got != "anonymous" {
		t.Errorf("extractAgentID = %q, want \"anonymous\"", got)
	}
}

func TestExtractAgentID_EmptyHeaders(t *testing.T) {
	t.Parallel()
	req := mcp.CallToolRequest{}
	req.Header = http.Header{}

	got := extractAgentID(req)
	if got != "anonymous" {
		t.Errorf("extractAgentID = %q, want \"anonymous\"", got)
	}
}

// ===================== extractAgentIDFromResource tests =====================

func TestExtractAgentIDFromResource_FromHeader(t *testing.T) {
	t.Parallel()
	req := mcp.ReadResourceRequest{}
	req.Header = http.Header{}
	req.Header.Set("X-Agent-ID", "res-agent")

	got := extractAgentIDFromResource(req)
	if got != "res-agent" {
		t.Errorf("extractAgentIDFromResource = %q, want \"res-agent\"", got)
	}
}

func TestExtractAgentIDFromResource_NoHeader(t *testing.T) {
	t.Parallel()
	req := mcp.ReadResourceRequest{}

	got := extractAgentIDFromResource(req)
	if got != "anonymous" {
		t.Errorf("extractAgentIDFromResource = %q, want \"anonymous\"", got)
	}
}

// ===================== checkRateLimit tests =====================

func TestCheckRateLimit_NotExceeded(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)

	deny := s.checkRateLimit("agent-1", "some_tool")
	if deny != nil {
		t.Error("expected nil (not rate limited)")
	}
}

func TestCheckRateLimit_Exceeded(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withRateLimit)

	// First call should succeed
	deny := s.checkRateLimit("rate-limited-agent", "some_tool")
	if deny != nil {
		t.Error("expected first call to succeed")
	}

	// Second call should be rate limited (1 rpm)
	deny = s.checkRateLimit("rate-limited-agent", "some_tool")
	if deny == nil {
		t.Error("expected rate limit denial")
	}
}

// ===================== checkResourceRateLimit tests =====================

func TestCheckResourceRateLimit_NotExceeded(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)

	err := s.checkResourceRateLimit("agent-1", "resource")
	if err != nil {
		t.Error("expected nil (not rate limited)")
	}
}

func TestCheckResourceRateLimit_Exceeded(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withRateLimit)

	_ = s.checkResourceRateLimit("rate-limited-agent", "res")
	err := s.checkResourceRateLimit("rate-limited-agent", "res")
	if err == nil {
		t.Error("expected rate limit error")
	}
}

// ===================== withToolAuth tests =====================

func TestWithToolAuth_FullFlow(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)

	var calledAgentID string
	handler := s.withToolAuth(toolAuthConfig{
		tool:         "test_tool",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		calledAgentID = agentID
		return marshalResult(map[string]string{"status": "ok"})
	})

	result, err := handler(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	if calledAgentID != "anonymous" {
		t.Errorf("expected anonymous agent, got %q", calledAgentID)
	}
}

func TestWithToolAuth_AuthDenied(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	s.kafkaManager = &kafka.MockKafkaManager{}

	called := false
	handler := s.withToolAuth(toolAuthConfig{
		tool:         "test_tool",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		called = true
		return nil, nil
	})

	result, err := handler(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if called {
		t.Error("handler should not have been called when auth denied")
	}
}

func TestWithToolAuth_RateLimitRejection(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withRateLimit)
	s.kafkaManager = &kafka.MockKafkaManager{}

	handler := s.withToolAuth(toolAuthConfig{
		tool:         "test_tool",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		return marshalResult(map[string]string{"status": "ok"})
	})

	// Build request with rate-limited agent header
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{}
	req.Header = http.Header{}
	req.Header.Set("X-Agent-ID", "rate-limited-agent")

	// First call should succeed
	result, _ := handler(context.Background(), req)
	assertNotError(t, result)

	// Second call should be rate limited
	result, _ = handler(context.Background(), req)
	assertIsError(t, result)
}

// ===================== validateAgentID tests =====================

func TestValidateAgentID_DefaultDenyDisabled(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)

	deny := s.validateAgentID("any-agent")
	if deny != nil {
		t.Error("expected nil when default deny is disabled")
	}
}

func TestValidateAgentID_DefaultDenyEnabled_UnknownAgent(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)

	deny := s.validateAgentID("unknown-agent")
	if deny == nil {
		t.Error("expected denial for unknown agent with default deny")
	}
}

func TestValidateAgentID_DefaultDenyEnabled_KnownAgent(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)

	deny := s.validateAgentID("allowed-agent")
	if deny != nil {
		t.Error("expected nil for known agent")
	}
}

func TestValidateAgentID_Anonymous(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)

	deny := s.validateAgentID("anonymous")
	if deny == nil {
		t.Error("expected denial for anonymous with default deny")
	}
}
