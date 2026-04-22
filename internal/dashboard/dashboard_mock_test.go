package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/audit"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/kafkamcp/kafkamcp/internal/ratelimit"
)

func newMockDashboard(t *testing.T, mock *kafka.MockKafkaManager) *Dashboard {
	t.Helper()
	rl := ratelimit.New(0)
	al, err := audit.NewLogger(true, 100, "", slog.Default())
	if err != nil {
		t.Fatalf("failed to create audit logger: %v", err)
	}
	return New(mock, al, rl, slog.Default())
}

// --- handleTopics tests ---

func TestHandleTopics_Success(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{
				{Name: "orders", Partitions: 3, ReplicationFactor: 2},
				{Name: "users", Partitions: 1, ReplicationFactor: 1},
			}, nil
		},
	}
	d := newMockDashboard(t, mock)
	handler := d.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/topics", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	topics, ok := result["topics"].([]any)
	if !ok {
		t.Fatal("expected 'topics' array in response")
	}
	if len(topics) != 2 {
		t.Errorf("expected 2 topics, got %d", len(topics))
	}
	if result["total"].(float64) != 2 {
		t.Errorf("expected total=2, got %v", result["total"])
	}
}

func TestHandleTopics_KafkaError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return nil, errors.New("broker unavailable")
		},
	}
	d := newMockDashboard(t, mock)
	handler := d.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/topics", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

func TestHandleTopics_MethodNotAllowed(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	d := newMockDashboard(t, mock)
	handler := d.Handler()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/topics", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

// --- handleTopology tests ---

func TestHandleTopology_Success(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		GetTopicGraphFn: func(ctx context.Context, cluster string) (*kafka.TopicGraph, error) {
			return &kafka.TopicGraph{
				Nodes: []kafka.TopicGraphNode{
					{ID: "orders", Name: "orders", Type: "topic"},
					{ID: "cg-1", Name: "order-processor", Type: "consumer_group"},
				},
				Edges: []kafka.TopicGraphEdge{
					{Source: "orders", Target: "cg-1", Type: "consumes"},
				},
			}, nil
		},
	}
	d := newMockDashboard(t, mock)
	handler := d.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/topology", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	nodes, ok := result["nodes"].([]any)
	if !ok {
		t.Fatal("expected 'nodes' array in response")
	}
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(nodes))
	}

	edges, ok := result["edges"].([]any)
	if !ok {
		t.Fatal("expected 'edges' array in response")
	}
	if len(edges) != 1 {
		t.Errorf("expected 1 edge, got %d", len(edges))
	}
}

func TestHandleTopology_KafkaError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		GetTopicGraphFn: func(ctx context.Context, cluster string) (*kafka.TopicGraph, error) {
			return nil, errors.New("metadata fetch failed")
		},
	}
	d := newMockDashboard(t, mock)
	handler := d.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/topology", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

func TestHandleTopology_MethodNotAllowed(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	d := newMockDashboard(t, mock)
	handler := d.Handler()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/topology", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

// --- handleHealth tests ---

func TestHandleHealth_Healthy(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		PingFn: func(ctx context.Context) error {
			return nil
		},
	}
	d := newMockDashboard(t, mock)
	handler := d.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if result["status"] != "healthy" {
		t.Errorf("expected status 'healthy', got %v", result["status"])
	}
	if _, hasError := result["error"]; hasError {
		t.Error("healthy response should not contain 'error' key")
	}
}

func TestHandleHealth_Unhealthy(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		PingFn: func(ctx context.Context) error {
			return errors.New("connection refused")
		},
	}
	d := newMockDashboard(t, mock)
	handler := d.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if result["status"] != "unhealthy" {
		t.Errorf("expected status 'unhealthy', got %v", result["status"])
	}
	if _, hasError := result["error"]; hasError {
		t.Error("unhealthy response should not expose error details")
	}
}

// --- writeJSON tests ---

func TestWriteJSON_ContentType(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSON(w, map[string]string{"msg": "hello"})

	ct := w.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("expected Content-Type 'application/json', got %q", ct)
	}
}

func TestWriteJSON_EncodesCorrectly(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSON(w, map[string]any{
		"topics": []string{"orders", "users"},
		"count":  2,
	})

	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if result["count"].(float64) != 2 {
		t.Errorf("expected count=2, got %v", result["count"])
	}
	topics := result["topics"].([]any)
	if len(topics) != 2 {
		t.Errorf("expected 2 topics, got %d", len(topics))
	}
}

// --- Mock call verification ---

func TestHandleTopics_CallsListTopics(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{}, nil
		},
	}
	d := newMockDashboard(t, mock)
	handler := d.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/topics", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if mock.CallCount("ListTopics") != 1 {
		t.Errorf("expected ListTopics called once, got %d", mock.CallCount("ListTopics"))
	}
}

func TestHandleHealth_CallsPing(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		PingFn: func(ctx context.Context) error { return nil },
	}
	d := newMockDashboard(t, mock)
	handler := d.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if mock.CallCount("Ping") != 1 {
		t.Errorf("expected Ping called once, got %d", mock.CallCount("Ping"))
	}
}

func TestHandleTopology_CallsGetTopicGraph(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		GetTopicGraphFn: func(ctx context.Context, cluster string) (*kafka.TopicGraph, error) {
			return &kafka.TopicGraph{}, nil
		},
	}
	d := newMockDashboard(t, mock)
	handler := d.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/topology", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if mock.CallCount("GetTopicGraph") != 1 {
		t.Errorf("expected GetTopicGraph called once, got %d", mock.CallCount("GetTopicGraph"))
	}
}
