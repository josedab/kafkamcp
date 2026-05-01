package ide

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

func TestIsAllowedOrigin_Whitelisted(t *testing.T) {
	t.Parallel()
	s := newTestServer(&kafka.MockKafkaManager{}, nil)
	tests := []struct {
		origin string
		want   bool
	}{
		{"http://localhost", true},
		{"http://127.0.0.1", true},
		{"https://localhost", true},
		{"https://127.0.0.1", true},
		{"vscode-webview://", true},
	}
	for _, tt := range tests {
		t.Run(tt.origin, func(t *testing.T) {
			got := s.isAllowedOrigin(tt.origin)
			if got != tt.want {
				t.Errorf("isAllowedOrigin(%q) = %v, want %v", tt.origin, got, tt.want)
			}
		})
	}
}

func TestIsAllowedOrigin_LocalhostWithPort(t *testing.T) {
	t.Parallel()
	s := newTestServer(&kafka.MockKafkaManager{}, nil)
	tests := []struct {
		origin string
		want   bool
	}{
		{"http://localhost:3000", true},
		{"http://127.0.0.1:8080", true},
		{"https://localhost:443", true},
		{"https://127.0.0.1:9090", true},
	}
	for _, tt := range tests {
		t.Run(tt.origin, func(t *testing.T) {
			got := s.isAllowedOrigin(tt.origin)
			if got != tt.want {
				t.Errorf("isAllowedOrigin(%q) = %v, want %v", tt.origin, got, tt.want)
			}
		})
	}
}

func TestIsAllowedOrigin_Rejected(t *testing.T) {
	t.Parallel()
	s := newTestServer(&kafka.MockKafkaManager{}, nil)
	tests := []struct {
		origin string
	}{
		{"http://evil.com"},
		{"https://attacker.example.com"},
		{"http://192.168.1.1"},
		{"ftp://localhost"},
	}
	for _, tt := range tests {
		t.Run(tt.origin, func(t *testing.T) {
			if s.isAllowedOrigin(tt.origin) {
				t.Errorf("expected origin %q to be rejected", tt.origin)
			}
		})
	}
}

func TestValidateIDEParam_Valid(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	ok := validateIDEParam(w, "test", "valid-value")
	if !ok {
		t.Error("expected validation to pass for normal value")
	}
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestValidateIDEParam_TooLong(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	longValue := strings.Repeat("x", maxIDEInputLength+1)
	ok := validateIDEParam(w, "test_field", longValue)
	if ok {
		t.Error("expected validation to fail for oversized value")
	}
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestValidateIDEParam_Empty(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	ok := validateIDEParam(w, "test", "")
	if !ok {
		t.Error("expected validation to pass for empty value")
	}
}

func TestValidateIDEParam_ExactLimit(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	ok := validateIDEParam(w, "test", strings.Repeat("a", maxIDEInputLength))
	if !ok {
		t.Error("expected validation to pass at exact limit")
	}
}

func TestGenerateIDESessionID_Uniqueness(t *testing.T) {
	t.Parallel()
	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id, err := generateIDESessionID()
		if err != nil {
			t.Fatalf("generateIDESessionID failed: %v", err)
		}
		if seen[id] {
			t.Fatalf("duplicate session ID generated: %s", id)
		}
		seen[id] = true
	}
}

func TestGenerateIDESessionID_Prefix(t *testing.T) {
	t.Parallel()
	id, err := generateIDESessionID()
	if err != nil {
		t.Fatalf("generateIDESessionID failed: %v", err)
	}
	if !strings.HasPrefix(id, "ide_") {
		t.Errorf("expected prefix 'ide_', got %q", id)
	}
}

func TestHandleCompletions_GroupFiltering(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return nil, nil
		},
		ListConsumerGroupsFn: func(ctx context.Context, cluster string) ([]kafka.ConsumerGroupSummary, error) {
			return []kafka.ConsumerGroupSummary{
				{GroupID: "order-processor", State: "Stable", TotalLag: 10},
				{GroupID: "payment-handler", State: "Stable", TotalLag: 5},
			}, nil
		},
	}
	s := newTestServer(mock, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/completions?kind=group&prefix=order", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	total := int(resp["total"].(float64))
	if total != 1 {
		t.Errorf("expected 1 group completion with prefix 'order', got %d", total)
	}
}

func TestHandleHealth_ResponseStructure(t *testing.T) {
	t.Parallel()
	s := newTestServer(&kafka.MockKafkaManager{}, nil)
	s.sessions["s1"] = &IDESession{ID: "s1", IDE: "vscode"}
	s.sessions["s2"] = &IDESession{ID: "s2", IDE: "intellij"}

	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/health", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp["status"] != "healthy" {
		t.Errorf("expected 'healthy', got %v", resp["status"])
	}
	if int(resp["sessions"].(float64)) != 2 {
		t.Errorf("expected 2 sessions, got %v", resp["sessions"])
	}
	caps, ok := resp["capabilities"].([]any)
	if !ok || len(caps) != 4 {
		t.Errorf("expected 4 capabilities, got %v", resp["capabilities"])
	}
}

func TestWriteIDEJSON_CORSHeaders(t *testing.T) {
	t.Parallel()
	s := newTestServer(&kafka.MockKafkaManager{}, nil)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Origin", "http://localhost:5173")
	s.writeIDEJSON(w, r, http.StatusOK, map[string]string{"k": "v"})

	if w.Header().Get("Content-Type") != "application/json" {
		t.Errorf("expected application/json, got %s", w.Header().Get("Content-Type"))
	}
	if w.Header().Get("Access-Control-Allow-Origin") != "http://localhost:5173" {
		t.Errorf("expected CORS header, got %s", w.Header().Get("Access-Control-Allow-Origin"))
	}
	if w.Header().Get("Access-Control-Allow-Credentials") != "false" {
		t.Error("expected Access-Control-Allow-Credentials: false")
	}
}

func TestWriteIDEJSON_NoCORSForRejectedOrigin(t *testing.T) {
	t.Parallel()
	s := newTestServer(&kafka.MockKafkaManager{}, nil)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Origin", "http://evil.com")
	s.writeIDEJSON(w, r, http.StatusOK, map[string]string{"k": "v"})

	if w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Errorf("expected no CORS header for rejected origin, got %s", w.Header().Get("Access-Control-Allow-Origin"))
	}
}

func TestWriteIDEJSON_NoOriginHeader(t *testing.T) {
	t.Parallel()
	s := newTestServer(&kafka.MockKafkaManager{}, nil)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	s.writeIDEJSON(w, r, http.StatusOK, map[string]string{"k": "v"})

	if w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Errorf("expected no CORS header when no Origin, got %s", w.Header().Get("Access-Control-Allow-Origin"))
	}
}
