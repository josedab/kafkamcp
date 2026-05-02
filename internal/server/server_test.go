package server

import (
	"net/http"
	"runtime"
	"testing"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/chaos"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/mark3labs/mcp-go/mcp"
)

func TestMatchTopicFilter(t *testing.T) {
	t.Parallel()
	tests := []struct {
		pattern string
		name    string
		want    bool
	}{
		{"*", "anything", true},
		{"*", "dotted.name", true},
		{"orders.*", "orders.created", true},
		{"orders.*", "orders.deleted", true},
		{"orders.*", "payments.created", false},
		{"*.dlq", "orders.dlq", true},
		{"*.dlq", "orders.created", false},
		{"orders.created", "orders.created", true},
		{"orders.created", "orders.deleted", false},
		{"test", "test", true},
		{"test", "other", false},
		{"*.*", "a.b", true},
		{"prefix.*", "prefix.suffix.extra", true}, // prefix.* matches any topic starting with "prefix."
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.name, func(t *testing.T) {
			got := matchTopicFilter(tt.pattern, tt.name)
			if got != tt.want {
				t.Errorf("matchTopicFilter(%q, %q) = %v, want %v", tt.pattern, tt.name, got, tt.want)
			}
		})
	}
}

func TestExtractAgentID(t *testing.T) {
	t.Parallel()
	// extractAgentID with no headers returns "anonymous"
	req := mcp.CallToolRequest{}
	id := extractAgentID(req)
	if id != "anonymous" {
		t.Errorf("expected 'anonymous', got %q", id)
	}
}

func TestExtractAgentIDWithHeader(t *testing.T) {
	t.Parallel()
	req := mcp.CallToolRequest{}
	req.Header = http.Header{"X-Agent-Id": []string{"my-agent"}}
	id := extractAgentID(req)
	if id != "my-agent" {
		t.Errorf("expected 'my-agent', got %q", id)
	}
}

func TestMarshalResult(t *testing.T) {
	t.Parallel()
	result, err := marshalResult(map[string]any{"key": "value"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestMarshalResourceJSON(t *testing.T) {
	t.Parallel()
	contents, err := marshalResourceJSON("kafka://test", map[string]any{"foo": "bar"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(contents) != 1 {
		t.Fatalf("expected 1 content, got %d", len(contents))
	}
}

func TestExtractAgentIDFromResource(t *testing.T) {
	t.Parallel()
	req := mcp.ReadResourceRequest{}
	id := extractAgentIDFromResource(req)
	if id != "anonymous" {
		t.Errorf("expected 'anonymous', got %q", id)
	}
}

func TestExtractAgentIDFromResourceWithHeader(t *testing.T) {
	t.Parallel()
	req := mcp.ReadResourceRequest{}
	req.Header = http.Header{"X-Agent-Id": []string{"resource-agent"}}
	id := extractAgentIDFromResource(req)
	if id != "resource-agent" {
		t.Errorf("expected 'resource-agent', got %q", id)
	}
}

func TestServerCloseStopsSubsystems(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)

	// Replace nil kafkaManager with a mock so Close() doesn't panic.
	closeCalled := false
	s.kafkaManager = &kafka.MockKafkaManager{
		CloseFn: func() { closeCalled = true },
	}

	// Initialize a chaos engine with an active experiment to verify
	// that Close() cancels its goroutines.
	s.chaosEngine = chaos.NewEngine(s.logger)
	_, err := s.chaosEngine.Inject(
		chaos.TypeSlowConsumer, "test-cluster",
		[]string{"test-topic"}, nil, nil, nil,
	)
	if err != nil {
		t.Fatalf("failed to inject chaos experiment: %v", err)
	}

	goroutinesBefore := runtime.NumGoroutine()

	if err := s.Close(); err != nil {
		t.Fatalf("Server.Close() returned error: %v", err)
	}

	if !closeCalled {
		t.Error("expected kafkaManager.Close() to be called")
	}

	// Allow goroutines time to exit.
	time.Sleep(50 * time.Millisecond)
	goroutinesAfter := runtime.NumGoroutine()

	// The chaos experiment goroutine should have been cleaned up.
	if goroutinesAfter > goroutinesBefore {
		t.Errorf("possible goroutine leak: before=%d after=%d", goroutinesBefore, goroutinesAfter)
	}

	// Verify chaos engine state was cleared.
	exps := s.chaosEngine.ListExperiments("")
	if len(exps) != 0 {
		t.Errorf("expected 0 experiments after Close(), got %d", len(exps))
	}
}
