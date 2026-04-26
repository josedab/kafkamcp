package ide

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/kafkamcp/kafkamcp/internal/schema"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func newTestServer(km kafka.KafkaManager, sc map[string]*schema.Client) *Server {
	return NewServer(km, sc, testLogger())
}

func testPassthroughAuth() func(http.HandlerFunc) http.HandlerFunc {
	return func(h http.HandlerFunc) http.HandlerFunc { return h }
}

func TestNewServer(t *testing.T) {
	s := newTestServer(nil, nil)
	if s == nil {
		t.Fatal("expected non-nil server")
	}
	if len(s.sessions) != 0 {
		t.Errorf("expected empty sessions, got %d", len(s.sessions))
	}
}

func TestRegisterRoutes(t *testing.T) {
	s := newTestServer(&kafka.MockKafkaManager{}, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	// Verify health endpoint registered
	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/health", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestRegisterRoutes_NilMiddlewareDenies(t *testing.T) {
	s := newTestServer(&kafka.MockKafkaManager{}, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/health", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Errorf("expected 403 when no auth middleware, got %d", w.Code)
	}
}

func TestHandleConnect(t *testing.T) {
	s := newTestServer(&kafka.MockKafkaManager{}, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	body := strings.NewReader(`{"ide":"vscode","version":"1.90.0"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/ide/v1/connect", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "connected" {
		t.Errorf("expected connected status, got %v", resp["status"])
	}
	if resp["session_id"] == nil {
		t.Error("expected session_id in response")
	}
}

func TestHandleConnectMethodNotAllowed(t *testing.T) {
	s := newTestServer(&kafka.MockKafkaManager{}, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/connect", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestHandleConnectInvalidBody(t *testing.T) {
	s := newTestServer(&kafka.MockKafkaManager{}, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodPost, "/api/ide/v1/connect", strings.NewReader("invalid"))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestHandleTopicBrowse(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{
				{Name: "orders", Partitions: 3, ReplicationFactor: 2, MessageCount: 100},
				{Name: "payments", Partitions: 6, ReplicationFactor: 3, MessageCount: 500},
			}, nil
		},
	}
	s := newTestServer(mock, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/topics?cluster=default", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp TopicBrowseResult
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Total != 2 {
		t.Errorf("expected 2 topics, got %d", resp.Total)
	}
}

func TestHandleTopicBrowseInternalFilter(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			if !includeInternal {
				return []kafka.TopicSummary{{Name: "orders", Partitions: 1}}, nil
			}
			return []kafka.TopicSummary{{Name: "orders"}, {Name: "__consumer_offsets", IsInternal: true}}, nil
		},
	}
	s := newTestServer(mock, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/topics?internal=true", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleTopicBrowseError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return nil, errors.New("kafka unavailable")
		},
	}
	s := newTestServer(mock, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/topics", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}
}

func TestHandleTopicBrowseMethodNotAllowed(t *testing.T) {
	s := newTestServer(&kafka.MockKafkaManager{}, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodPost, "/api/ide/v1/topics", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestHandleTopicDetail(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		DescribeTopicFn: func(ctx context.Context, cluster, topicName string) (*kafka.TopicDetail, error) {
			// Server passes (ctx, topicName, cluster) so cluster param receives the topic name
			return &kafka.TopicDetail{
				Name:              cluster,
				ReplicationFactor: 3,
				IsInternal:        false,
				MessageCount:      1000,
				Partitions: []kafka.PartitionDetail{
					{ID: 0, Leader: 1, EarliestOffset: 0, LatestOffset: 500},
					{ID: 1, Leader: 2, EarliestOffset: 0, LatestOffset: 500},
				},
			}, nil
		},
	}
	s := newTestServer(mock, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/topics/orders", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp TopicNode
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Name != "orders" {
		t.Errorf("expected orders, got %s", resp.Name)
	}
	if len(resp.Children) != 2 {
		t.Errorf("expected 2 partitions, got %d", len(resp.Children))
	}
	if resp.Children[0].Messages != 500 {
		t.Errorf("expected 500 messages, got %d", resp.Children[0].Messages)
	}
}

func TestHandleTopicDetailNotFound(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		DescribeTopicFn: func(ctx context.Context, cluster, topicName string) (*kafka.TopicDetail, error) {
			return nil, errors.New("not found")
		},
	}
	s := newTestServer(mock, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/topics/nonexistent", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestHandleTopicDetailEmptyName(t *testing.T) {
	s := newTestServer(&kafka.MockKafkaManager{}, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/topics/", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestHandleTopicDetailUnicodeName(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		DescribeTopicFn: func(ctx context.Context, cluster, topicName string) (*kafka.TopicDetail, error) {
			return &kafka.TopicDetail{Name: cluster}, nil
		},
	}
	s := newTestServer(mock, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/topics/日本語トピック", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for unicode topic, got %d", w.Code)
	}
}

func TestHandleSchemas(t *testing.T) {
	s := newTestServer(&kafka.MockKafkaManager{}, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/schemas", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["schema_registry"] != false {
		t.Errorf("expected no schema registry, got %v", resp["schema_registry"])
	}
}

func TestHandleSchemasWithClients(t *testing.T) {
	clients := map[string]*schema.Client{"default": {}}
	s := newTestServer(&kafka.MockKafkaManager{}, clients)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/schemas", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["schema_registry"] != true {
		t.Errorf("expected schema registry true, got %v", resp["schema_registry"])
	}
}

func TestHandleSchemasMethodNotAllowed(t *testing.T) {
	s := newTestServer(&kafka.MockKafkaManager{}, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodPost, "/api/ide/v1/schemas", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestHandleSchemaDetail(t *testing.T) {
	s := newTestServer(&kafka.MockKafkaManager{}, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/schemas/orders-value", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp SchemaInfo
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Subject != "orders-value" {
		t.Errorf("expected orders-value, got %s", resp.Subject)
	}
	if resp.SchemaType != "AVRO" {
		t.Errorf("expected AVRO, got %s", resp.SchemaType)
	}
}

func TestHandleSchemaDetailEmpty(t *testing.T) {
	s := newTestServer(&kafka.MockKafkaManager{}, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/schemas/", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestHandleMessagePreview(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{
				{Partition: 0, Offset: 100, Key: "key1", Value: `{"id":1}`, Timestamp: "2025-01-01T00:00:00Z"},
			}, nil
		},
	}
	s := newTestServer(mock, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/messages?topic=orders", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["count"].(float64) != 1 {
		t.Errorf("expected 1 message, got %v", resp["count"])
	}
}

func TestHandleMessagePreviewNoTopic(t *testing.T) {
	s := newTestServer(&kafka.MockKafkaManager{}, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/messages", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestHandleMessagePreviewError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return nil, errors.New("timeout")
		},
	}
	s := newTestServer(mock, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/messages?topic=orders", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}
}

func TestHandleMessagePreviewMethodNotAllowed(t *testing.T) {
	s := newTestServer(&kafka.MockKafkaManager{}, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodPost, "/api/ide/v1/messages?topic=orders", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestHandleMessagePreviewLargeMessage(t *testing.T) {
	largeValue := strings.Repeat("x", 5000)
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{
				{Partition: 0, Offset: 1, Key: "k1", Value: largeValue, Timestamp: "2025-01-01T00:00:00Z"},
			}, nil
		},
	}
	s := newTestServer(mock, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/messages?topic=orders", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleCompletions(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{
				{Name: "orders", Partitions: 3, MessageCount: 100},
				{Name: "payments", Partitions: 6, MessageCount: 500},
			}, nil
		},
		ListConsumerGroupsFn: func(ctx context.Context, cluster string) ([]kafka.ConsumerGroupSummary, error) {
			return []kafka.ConsumerGroupSummary{
				{GroupID: "order-processor", State: "Stable", TotalLag: 10},
			}, nil
		},
	}
	s := newTestServer(mock, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/completions", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	total := int(resp["total"].(float64))
	if total != 3 {
		t.Errorf("expected 3 completions (2 topics + 1 group), got %d", total)
	}
}

func TestHandleCompletionsWithPrefix(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{
				{Name: "orders"},
				{Name: "payments"},
			}, nil
		},
		ListConsumerGroupsFn: func(ctx context.Context, cluster string) ([]kafka.ConsumerGroupSummary, error) {
			return nil, nil
		},
	}
	s := newTestServer(mock, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/completions?prefix=ord&kind=topic", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if int(resp["total"].(float64)) != 1 {
		t.Errorf("expected 1 filtered completion, got %v", resp["total"])
	}
}

func TestHandleCompletionsMethodNotAllowed(t *testing.T) {
	s := newTestServer(&kafka.MockKafkaManager{}, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodPost, "/api/ide/v1/completions", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestHandleHealth(t *testing.T) {
	s := newTestServer(&kafka.MockKafkaManager{}, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/health", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "healthy" {
		t.Errorf("expected healthy, got %v", resp["status"])
	}
	if resp["sessions"].(float64) != 0 {
		t.Errorf("expected 0 sessions, got %v", resp["sessions"])
	}
}

func TestHandleHealthWithSessions(t *testing.T) {
	s := newTestServer(&kafka.MockKafkaManager{}, nil)
	s.sessions["s1"] = &IDESession{ID: "s1", IDE: "vscode"}

	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/health", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["sessions"].(float64) != 1 {
		t.Errorf("expected 1 session, got %v", resp["sessions"])
	}
}

func TestWriteIDEJSON(t *testing.T) {
	s := newTestServer(&kafka.MockKafkaManager{}, nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Origin", "http://localhost:3000")
	s.writeIDEJSON(w, r, http.StatusCreated, map[string]string{"key": "value"})

	if w.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d", w.Code)
	}
	if w.Header().Get("Content-Type") != "application/json" {
		t.Errorf("expected application/json, got %s", w.Header().Get("Content-Type"))
	}
	if w.Header().Get("Access-Control-Allow-Origin") != "http://localhost:3000" {
		t.Errorf("expected CORS header for localhost origin, got %s", w.Header().Get("Access-Control-Allow-Origin"))
	}
	if w.Header().Get("Access-Control-Allow-Credentials") != "false" {
		t.Error("expected Access-Control-Allow-Credentials: false")
	}
}

func TestConcurrentConnect(t *testing.T) {
	s := newTestServer(&kafka.MockKafkaManager{}, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			body := strings.NewReader(fmt.Sprintf(`{"ide":"vscode","version":"%d"}`, idx))
			req := httptest.NewRequest(http.MethodPost, "/api/ide/v1/connect", body)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("concurrent connect failed: %d", w.Code)
			}
		}(i)
	}
	wg.Wait()

	s.mu.RLock()
	count := len(s.sessions)
	s.mu.RUnlock()
	if count < 15 {
		t.Errorf("expected at least 15 sessions from concurrent connects, got %d", count)
	}
}
