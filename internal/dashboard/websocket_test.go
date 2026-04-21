package dashboard

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/audit"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/kafkamcp/kafkamcp/internal/ratelimit"
)

func testDashboardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func setupDashboard(t *testing.T) *Dashboard {
	t.Helper()
	logger := testDashboardLogger()
	al, _ := audit.NewLogger(true, 100, "", logger)
	rl := ratelimit.New(0)
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{{Name: "test-topic", Partitions: 3, MessageCount: 100}}, nil
		},
		ListConsumerGroupsFn: func(ctx context.Context, cluster string) ([]kafka.ConsumerGroupSummary, error) {
			return []kafka.ConsumerGroupSummary{
				{GroupID: "group-1", State: "Stable", TotalLag: 500},
			}, nil
		},
		PingFn: func(ctx context.Context) error { return nil },
	}
	return New(mock, al, rl, logger)
}

// --- WebSocketHub tests ---

func TestNewWebSocketHub(t *testing.T) {
	hub := NewWebSocketHub()
	if hub == nil {
		t.Fatal("expected non-nil hub")
	}
	if hub.ClientCount() != 0 {
		t.Errorf("expected 0 clients, got %d", hub.ClientCount())
	}
}

func TestWebSocketHub_Broadcast(t *testing.T) {
	hub := NewWebSocketHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	// Register a client
	client := &WSClient{ID: "c1", Ch: make(chan []byte, 10), JoinedAt: time.Now()}
	hub.register <- client
	time.Sleep(50 * time.Millisecond)

	hub.Broadcast("test_event", map[string]string{"key": "value"})
	time.Sleep(50 * time.Millisecond)

	select {
	case msg := <-client.Ch:
		if len(msg) == 0 {
			t.Error("expected non-empty broadcast message")
		}
	default:
		t.Error("expected broadcast message on client channel")
	}
}

func TestWebSocketHub_ClientCount(t *testing.T) {
	hub := NewWebSocketHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	hub.register <- &WSClient{ID: "c1", Ch: make(chan []byte, 1), JoinedAt: time.Now()}
	hub.register <- &WSClient{ID: "c2", Ch: make(chan []byte, 1), JoinedAt: time.Now()}
	time.Sleep(50 * time.Millisecond)

	if hub.ClientCount() != 2 {
		t.Errorf("expected 2 clients, got %d", hub.ClientCount())
	}

	hub.unregister <- &WSClient{ID: "c1"}
	time.Sleep(50 * time.Millisecond)

	if hub.ClientCount() != 1 {
		t.Errorf("expected 1 client after unregister, got %d", hub.ClientCount())
	}
}

func TestWebSocketHub_RunContextCancel(t *testing.T) {
	hub := NewWebSocketHub()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		hub.Run(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("expected Run to exit on context cancel")
	}
}

// --- DashboardMetrics tests ---

func TestDashboardMetrics_RecordToolCall(t *testing.T) {
	dm := NewDashboardMetrics()
	dm.RecordToolCall("agent-1", "list_topics", 50, false)
	dm.RecordToolCall("agent-1", "list_topics", 30, false)
	dm.RecordToolCall("agent-1", "consume", 100, true)

	entries := dm.GetAgentActivity()
	if len(entries) != 1 {
		t.Fatalf("expected 1 agent, got %d", len(entries))
	}

	entry := entries[0]
	if entry.AgentID != "agent-1" || entry.TotalCalls != 3 {
		t.Errorf("unexpected entry: %+v", entry)
	}
	if entry.ErrorRate < 0.33 || entry.ErrorRate > 0.34 {
		t.Errorf("expected ~0.33 error rate, got %f", entry.ErrorRate)
	}
}

func TestDashboardMetrics_RecordToolCall_ConcurrentSafety(t *testing.T) {
	dm := NewDashboardMetrics()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			dm.RecordToolCall("agent-1", "tool", int64(n), n%10 == 0)
		}(i)
	}
	wg.Wait()

	entries := dm.GetAgentActivity()
	if len(entries) != 1 {
		t.Fatalf("expected 1 agent, got %d", len(entries))
	}
	if entries[0].TotalCalls != 100 {
		t.Errorf("expected 100 calls, got %d", entries[0].TotalCalls)
	}
}

func TestDashboardMetrics_GetAgentActivity_Sorting(t *testing.T) {
	dm := NewDashboardMetrics()
	dm.RecordToolCall("agent-low", "tool", 10, false)
	dm.RecordToolCall("agent-high", "tool", 10, false)
	dm.RecordToolCall("agent-high", "tool", 10, false)
	dm.RecordToolCall("agent-high", "tool", 10, false)

	entries := dm.GetAgentActivity()
	if len(entries) != 2 {
		t.Fatalf("expected 2 agents, got %d", len(entries))
	}
	if entries[0].AgentID != "agent-high" {
		t.Error("expected agent-high first (most calls)")
	}
}

func TestDashboardMetrics_GetAgentActivity_TopTools(t *testing.T) {
	dm := NewDashboardMetrics()
	for i := 0; i < 10; i++ {
		dm.RecordToolCall("a1", "list_topics", 50, false)
	}
	for i := 0; i < 5; i++ {
		dm.RecordToolCall("a1", "consume", 100, false)
	}
	dm.RecordToolCall("a1", "produce", 200, false)

	entries := dm.GetAgentActivity()
	if len(entries[0].TopTools) == 0 {
		t.Error("expected top tools")
	}
	if entries[0].TopTools[0].Tool != "list_topics" {
		t.Errorf("expected list_topics as top tool, got %s", entries[0].TopTools[0].Tool)
	}
}

func TestDashboardMetrics_GetAgentActivity_TopToolsMaxFive(t *testing.T) {
	dm := NewDashboardMetrics()
	for i := 0; i < 7; i++ {
		tool := "tool_" + string(rune('a'+i))
		for j := 0; j <= i; j++ {
			dm.RecordToolCall("a1", tool, 10, false)
		}
	}

	entries := dm.GetAgentActivity()
	if len(entries[0].TopTools) > 5 {
		t.Errorf("expected max 5 top tools, got %d", len(entries[0].TopTools))
	}
}

func TestDashboardMetrics_GetAgentActivity_AvgLatency(t *testing.T) {
	dm := NewDashboardMetrics()
	dm.RecordToolCall("a1", "tool", 100, false)
	dm.RecordToolCall("a1", "tool", 200, false)

	entries := dm.GetAgentActivity()
	if len(entries[0].TopTools) > 0 {
		if entries[0].TopTools[0].AvgMs != 150 {
			t.Errorf("expected avg latency 150, got %d", entries[0].TopTools[0].AvgMs)
		}
	}
}

func TestDashboardMetrics_GetAgentActivity_Empty(t *testing.T) {
	dm := NewDashboardMetrics()
	entries := dm.GetAgentActivity()
	if len(entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(entries))
	}
}

// --- HTTP handler tests ---

func TestHandleConsumerLag(t *testing.T) {
	d := setupDashboard(t)
	mux := http.NewServeMux()
	d.RegisterDashboardRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/consumer-lag", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandleConsumerLag_MethodNotAllowed(t *testing.T) {
	d := setupDashboard(t)
	mux := http.NewServeMux()
	d.RegisterDashboardRoutes(mux)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/consumer-lag", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestHandleAgentActivity(t *testing.T) {
	d := setupDashboard(t)
	mux := http.NewServeMux()
	d.RegisterDashboardRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/agent-activity", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandleFlowRates(t *testing.T) {
	d := setupDashboard(t)
	mux := http.NewServeMux()
	d.RegisterDashboardRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/flow-rates", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandleDashboardSummary(t *testing.T) {
	d := setupDashboard(t)
	mux := http.NewServeMux()
	d.RegisterDashboardRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard-summary", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandleDashboardSummary_MethodNotAllowed(t *testing.T) {
	d := setupDashboard(t)
	mux := http.NewServeMux()
	d.RegisterDashboardRoutes(mux)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/dashboard-summary", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestHandleConsumerLag_EmbedMode(t *testing.T) {
	d := setupDashboard(t)
	mux := http.NewServeMux()
	d.RegisterDashboardRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/consumer-lag?embed=true", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if w.Header().Get("X-Frame-Options") != "DENY" {
		t.Error("expected X-Frame-Options DENY (embed override removed for security)")
	}
}
