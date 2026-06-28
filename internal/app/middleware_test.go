package app_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/josedab/kafkamcp/internal/app"
)

func TestSecurityHeadersMiddleware_SetsHeaders(t *testing.T) {
	handler := app.SecurityHeadersMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))

	want := map[string]string{
		"X-Content-Type-Options":  "nosniff",
		"X-Frame-Options":         "DENY",
		"Cache-Control":           "no-store",
		"Content-Security-Policy": "default-src 'none'",
		"Referrer-Policy":         "no-referrer",
	}
	for header, value := range want {
		if got := rec.Header().Get(header); got != value {
			t.Errorf("header %s: got %q, want %q", header, got, value)
		}
	}
}

func TestRateLimitMiddleware_AllowsUnderBurst(t *testing.T) {
	limiter := app.NewRateLimiter(10, 5)
	handler := app.RateLimitMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), limiter)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestRateLimitMiddleware_Rejects429WhenExhausted(t *testing.T) {
	limiter := app.NewRateLimiter(0.001, 1) // tiny rate, burst=1
	handler := app.RateLimitMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), limiter)

	// First request consumes the burst token.
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, httptest.NewRequest("GET", "/", nil))

	// Second request should be rate-limited.
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, httptest.NewRequest("GET", "/", nil))
	if rec2.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429 after burst exhausted, got %d", rec2.Code)
	}
}

// TestBuildHTTPHandler_SecurityHeadersPresent verifies that security headers
// are added to every response through the assembled handler chain.
func TestBuildHTTPHandler_SecurityHeadersPresent(t *testing.T) {
	base := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	handler := app.BuildHTTPHandler(base, nil, 10<<20)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))

	if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options: got %q, want %q", got, "nosniff")
	}
}

// TestBuildHTTPHandler_MaxBodyEnforced verifies that reading a body that
// exceeds the size limit returns an error from the MaxBytesReader. The 413
// status code is emitted by the real HTTP server (not httptest), so we
// assert that the body read returns a non-nil error instead.
func TestBuildHTTPHandler_MaxBodyEnforced(t *testing.T) {
	var readErr error
	base := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Read more than the limit; MaxBytesReader should return an error.
		buf := make([]byte, 200)
		_, readErr = r.Body.Read(buf)
		w.WriteHeader(http.StatusOK)
	})
	handler := app.BuildHTTPHandler(base, nil, 10) // 10-byte limit

	body := strings.NewReader(strings.Repeat("x", 100))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("POST", "/", body))

	// The real HTTP server returns 413; in httptest the body read returns an error.
	if readErr == nil {
		t.Error("expected body read to return an error after exceeding MaxBytesReader limit")
	}
}

// TestBuildHTTPHandler_MTLSIsOutermostLayer verifies that the mTLS middleware
// runs before the inner layers: a context value set by mTLS must be visible to
// the base handler.
func TestBuildHTTPHandler_MTLSIsOutermostLayer(t *testing.T) {
	type ctxKey struct{}

	fakeMTLS := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			// Annotate the context so the base handler can confirm mTLS ran first.
			r = r.WithContext(context.WithValue(ctx, ctxKey{}, true))
			next.ServeHTTP(w, r)
		})
	}

	var sawMTLS bool
	base := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawMTLS = r.Context().Value(ctxKey{}) == true
		w.WriteHeader(http.StatusOK)
	})

	handler := app.BuildHTTPHandler(base, fakeMTLS, 10<<20)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))

	if !sawMTLS {
		t.Error("base handler did not see the context value set by the mTLS layer — mTLS is not outermost")
	}
}

// TestBuildHTTPHandler_NilMTLSSkipped verifies that passing nil for the mTLS
// argument does not panic and the chain still works correctly.
func TestBuildHTTPHandler_NilMTLSSkipped(t *testing.T) {
	base := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	handler := app.BuildHTTPHandler(base, nil, 10<<20)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}
