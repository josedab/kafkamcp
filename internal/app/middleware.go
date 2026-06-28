package app

import (
	"net/http"
	"sync"
	"time"
)

// SecurityHeadersMiddleware adds defense-in-depth HTTP security headers to
// every response. It must be applied to every HTTP transport handler.
func SecurityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Security-Policy", "default-src 'none'")
		w.Header().Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
		w.Header().Set("Access-Control-Allow-Origin", "")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Agent-ID, X-Client-ID")
		next.ServeHTTP(w, r)
	})
}

// RateLimiter is a token-bucket rate limiter.
type RateLimiter struct {
	mu       sync.Mutex
	tokens   float64
	max      float64
	rate     float64 // tokens per second
	lastTime time.Time
}

// NewRateLimiter creates a RateLimiter that refills at ratePerSec tokens/second
// with a maximum burst of burst tokens.
func NewRateLimiter(ratePerSec, burst float64) *RateLimiter {
	return &RateLimiter{
		tokens:   burst,
		max:      burst,
		rate:     ratePerSec,
		lastTime: time.Now(),
	}
}

// Allow consumes one token and returns whether the request is permitted.
func (rl *RateLimiter) Allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(rl.lastTime).Seconds()
	rl.lastTime = now
	rl.tokens += elapsed * rl.rate
	if rl.tokens > rl.max {
		rl.tokens = rl.max
	}
	if rl.tokens < 1 {
		return false
	}
	rl.tokens--
	return true
}

// RateLimitMiddleware rejects requests with HTTP 429 when the limiter is
// exhausted.
func RateLimitMiddleware(next http.Handler, limiter *RateLimiter) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !limiter.Allow() {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// BuildHTTPHandler assembles the standard middleware chain for MCP HTTP
// transports (SSE and streamable-HTTP). Execution order on an incoming
// request (outermost → innermost):
//
//  1. mtls  – extracts the verified mTLS peer identity (no-op when nil)
//  2. MaxBytesHandler – caps the request body at maxBodyBytes
//  3. SecurityHeadersMiddleware – sets defense-in-depth response headers
//  4. base  – the MCP transport handler
//
// Passing nil for mtls skips the mTLS layer.
func BuildHTTPHandler(base http.Handler, mtls func(http.Handler) http.Handler, maxBodyBytes int64) http.Handler {
	h := SecurityHeadersMiddleware(base)
	h = http.MaxBytesHandler(h, maxBodyBytes)
	if mtls != nil {
		h = mtls(h)
	}
	return h
}
