package ide

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

// --- SetAllowedOrigins ---

func TestSetAllowedOrigins_ValidOrigins(t *testing.T) {
	t.Parallel()
	s := newTestServer(&kafka.MockKafkaManager{}, nil)

	s.SetAllowedOrigins([]string{"https://example.com", "https://dev.example.com"})

	if !s.allowedOrigins["https://example.com"] {
		t.Error("expected example.com to be allowed")
	}
	if !s.allowedOrigins["https://dev.example.com"] {
		t.Error("expected dev.example.com to be allowed")
	}
	// Default origins should be overridden
	if s.allowedOrigins["http://localhost"] {
		t.Error("expected default localhost to be removed after SetAllowedOrigins")
	}
}

func TestSetAllowedOrigins_EmptyList(t *testing.T) {
	t.Parallel()
	s := newTestServer(&kafka.MockKafkaManager{}, nil)

	s.SetAllowedOrigins([]string{})

	if len(s.allowedOrigins) != 0 {
		t.Errorf("expected empty origins, got %d", len(s.allowedOrigins))
	}
}

func TestSetAllowedOrigins_CORSHeaders(t *testing.T) {
	t.Parallel()
	s := newTestServer(&kafka.MockKafkaManager{}, nil)
	s.SetAllowedOrigins([]string{"https://my-ide.com"})
	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	// Request with allowed origin
	req := httptest.NewRequest(http.MethodGet, "/api/ide/v1/health", nil)
	req.Header.Set("Origin", "https://my-ide.com")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Header().Get("Access-Control-Allow-Origin") != "https://my-ide.com" {
		t.Errorf("expected CORS header for allowed origin, got %q", w.Header().Get("Access-Control-Allow-Origin"))
	}

	// Request with disallowed origin
	req = httptest.NewRequest(http.MethodGet, "/api/ide/v1/health", nil)
	req.Header.Set("Origin", "https://evil.com")
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Errorf("expected no CORS header for disallowed origin, got %q", w.Header().Get("Access-Control-Allow-Origin"))
	}
}

// --- StartSessionCleanup ---

func TestStartSessionCleanup_ContextCancellation(t *testing.T) {
	t.Parallel()
	s := newTestServer(&kafka.MockKafkaManager{}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	s.StartSessionCleanup(ctx)

	// Cancel should stop the goroutine
	cancel()
	// Give goroutine time to notice
	time.Sleep(50 * time.Millisecond)
	// No way to directly check goroutine stopped, but this verifies no panic
}

// --- cleanupExpiredSessions ---

func TestCleanupExpiredSessions_RemovesExpired(t *testing.T) {
	t.Parallel()
	s := newTestServer(&kafka.MockKafkaManager{}, nil)

	// Add an expired session
	s.sessions["expired"] = &IDESession{
		ID:       "expired",
		IDE:      "vscode",
		LastPing: time.Now().Add(-1 * time.Hour), // well past TTL
	}
	// Add an active session
	s.sessions["active"] = &IDESession{
		ID:       "active",
		IDE:      "intellij",
		LastPing: time.Now(),
	}

	s.cleanupExpiredSessions()

	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.sessions["expired"]; ok {
		t.Error("expected expired session to be removed")
	}
	if _, ok := s.sessions["active"]; !ok {
		t.Error("expected active session to be preserved")
	}
}

func TestCleanupExpiredSessions_EmptyStore(t *testing.T) {
	t.Parallel()
	s := newTestServer(&kafka.MockKafkaManager{}, nil)

	// Should not panic
	s.cleanupExpiredSessions()

	s.mu.RLock()
	count := len(s.sessions)
	s.mu.RUnlock()
	if count != 0 {
		t.Errorf("expected 0 sessions, got %d", count)
	}
}

func TestCleanupExpiredSessions_AllExpired(t *testing.T) {
	t.Parallel()
	s := newTestServer(&kafka.MockKafkaManager{}, nil)

	for i := 0; i < 5; i++ {
		s.sessions[string(rune('a'+i))] = &IDESession{
			ID:       string(rune('a' + i)),
			LastPing: time.Now().Add(-2 * time.Hour),
		}
	}

	s.cleanupExpiredSessions()

	s.mu.RLock()
	count := len(s.sessions)
	s.mu.RUnlock()
	if count != 0 {
		t.Errorf("expected all sessions removed, got %d", count)
	}
}

// --- isAllowedOrigin ---

func TestIsAllowedOrigin_LocalhostPort(t *testing.T) {
	t.Parallel()
	s := newTestServer(&kafka.MockKafkaManager{}, nil)

	if !s.isAllowedOrigin("http://localhost:3000") {
		t.Error("expected localhost:3000 to be allowed")
	}
	if !s.isAllowedOrigin("http://127.0.0.1:8080") {
		t.Error("expected 127.0.0.1:8080 to be allowed")
	}
	if !s.isAllowedOrigin("https://localhost:443") {
		t.Error("expected https localhost to be allowed")
	}
}

func TestIsAllowedOrigin_Disallowed(t *testing.T) {
	t.Parallel()
	s := newTestServer(&kafka.MockKafkaManager{}, nil)

	if s.isAllowedOrigin("https://evil.com") {
		t.Error("expected evil.com to be disallowed")
	}
}

func TestIsAllowedOrigin_VscodeWebview(t *testing.T) {
	t.Parallel()
	s := newTestServer(&kafka.MockKafkaManager{}, nil)

	if !s.isAllowedOrigin("vscode-webview://") {
		t.Error("expected vscode-webview:// to be allowed by default")
	}
}

// --- validateIDEParam ---

func TestValidateIDEParam_TooLongCov(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	longValue := make([]byte, maxIDEInputLength+1)
	for i := range longValue {
		longValue[i] = 'a'
	}
	ok := validateIDEParam(w, "test", string(longValue))
	if ok {
		t.Error("expected false for too-long param")
	}
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestValidateIDEParam_ValidCov(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	ok := validateIDEParam(w, "test", "normal-value")
	if !ok {
		t.Error("expected true for normal param")
	}
}

// --- handleConnect session limit ---

func TestHandleConnect_MaxSessions(t *testing.T) {
	t.Parallel()
	s := newTestServer(&kafka.MockKafkaManager{}, nil)

	// Fill sessions to max
	for i := 0; i < maxIDESessions; i++ {
		s.sessions[string(rune(i))] = &IDESession{ID: string(rune(i))}
	}

	mux := http.NewServeMux()
	s.RegisterRoutes(mux, testPassthroughAuth())

	body := `{"ide":"vscode","version":"1.90.0"}`
	req := httptest.NewRequest(http.MethodPost, "/api/ide/v1/connect", nil)
	req.Body = http.NoBody
	req = httptest.NewRequest(http.MethodPost, "/api/ide/v1/connect", strings.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 at max sessions, got %d", w.Code)
	}
}

// --- generateIDESessionID ---

func TestGenerateIDESessionID_FormatCov(t *testing.T) {
	t.Parallel()
	id, err := generateIDESessionID()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(id) < 10 {
		t.Errorf("expected longer session ID, got %q", id)
	}
	if id[:4] != "ide_" {
		t.Errorf("expected ide_ prefix, got %q", id[:4])
	}
}

func TestGenerateIDESessionID_UniqueCov(t *testing.T) {
	t.Parallel()
	seen := make(map[string]bool)
	for i := 0; i < 50; i++ {
		id, _ := generateIDESessionID()
		if seen[id] {
			t.Fatalf("duplicate ID: %s", id)
		}
		seen[id] = true
	}
}
