// Package ratelimit implements per-agent token-bucket rate limiting.
//
// The Limiter optionally persists token-bucket state to an
// [engine.Store] so that quotas survive process restarts: an attacker
// cannot bypass a tight quota by crashing the process. Persistence is
// opt-in via [NewWithBackend]; the legacy [New] constructor stays
// in-memory and matches the historical behavior bit-for-bit.
package ratelimit

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/josedab/kafkamcp/internal/engine"
	"github.com/josedab/kafkamcp/internal/engine/backend"
)

// Limiter implements a per-agent token bucket rate limiter.
type Limiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	global  *bucket

	// store persists bucket state across restarts. When nil the limiter
	// is purely in-memory (legacy behavior).
	store  *engine.Store[bucketRecord]
	logger *slog.Logger
}

type bucket struct {
	tokens     float64
	maxTokens  float64
	refillRate float64 // tokens per second
	lastRefill time.Time
}

// bucketRecord is the persisted form of a bucket. Only the dynamic
// state is stored — the static limits (maxTokens, refillRate) come from
// configuration and are re-applied on load so changing the configured
// quota correctly overrides any previously-persisted ceiling.
type bucketRecord struct {
	Tokens     float64   `json:"tokens"`
	LastRefill time.Time `json:"last_refill"`
}

// New creates a new in-memory rate limiter with a global limit.
//
// The returned Limiter does not persist state; restarting the process
// resets every bucket to full. For deployments that need quotas to
// survive restarts use [NewWithBackend].
func New(globalRequestsPerMinute int) *Limiter {
	l := &Limiter{
		buckets: make(map[string]*bucket),
	}
	if globalRequestsPerMinute > 0 {
		l.global = newBucket(globalRequestsPerMinute)
	}
	return l
}

// NewWithBackend constructs a rate limiter that persists per-agent
// bucket state to be. Existing bucket records are loaded eagerly during
// construction; subsequent writes happen on every Allow() call that
// mutates a bucket (rate-limiting calls are themselves rare relative to
// regular tool calls so the write amplification is acceptable).
//
// When be is nil this is equivalent to [New].
func NewWithBackend(ctx context.Context, globalRequestsPerMinute int, be backend.Backend, logger *slog.Logger) (*Limiter, error) {
	l := New(globalRequestsPerMinute)
	if be == nil {
		l.logger = logger
		return l, nil
	}
	l.store = engine.NewStore[bucketRecord](be, "ratelimit.buckets")
	l.logger = logger
	if err := l.loadGlobal(ctx); err != nil {
		return nil, err
	}
	return l, nil
}

// loadGlobal restores the global bucket if it was previously persisted.
// Per-agent buckets are loaded lazily by SetAgentLimit so they pick up
// any configuration changes.
func (l *Limiter) loadGlobal(ctx context.Context) error {
	if l.global == nil || l.store == nil {
		return nil
	}
	rec, ok, err := l.store.Get(ctx, "__global__")
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.global.tokens = clampTokens(rec.Tokens, l.global.maxTokens)
	l.global.lastRefill = rec.LastRefill
	return nil
}

func newBucket(requestsPerMinute int) *bucket {
	rate := float64(requestsPerMinute) / 60.0
	return &bucket{
		tokens:     float64(requestsPerMinute),
		maxTokens:  float64(requestsPerMinute),
		refillRate: rate,
		lastRefill: time.Now(),
	}
}

func (b *bucket) allow() bool {
	now := time.Now()
	elapsed := now.Sub(b.lastRefill).Seconds()
	b.tokens += elapsed * b.refillRate
	if b.tokens > b.maxTokens {
		b.tokens = b.maxTokens
	}
	b.lastRefill = now

	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

func clampTokens(v, max float64) float64 {
	if v < 0 {
		return 0
	}
	if v > max {
		return max
	}
	return v
}

// SetAgentLimit configures a per-agent rate limit. When the limiter is
// backed by a persistent store, any previously-persisted state for the
// agent is restored and clamped to the new ceiling.
func (l *Limiter) SetAgentLimit(agentID string, requestsPerMinute int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if requestsPerMinute <= 0 {
		return
	}
	b := newBucket(requestsPerMinute)
	if l.store != nil {
		if rec, ok, err := l.store.Get(context.Background(), agentID); err == nil && ok {
			b.tokens = clampTokens(rec.Tokens, b.maxTokens)
			b.lastRefill = rec.LastRefill
		} else if err != nil && l.logger != nil {
			l.logger.Warn("ratelimit: failed to load persisted bucket", "agent", agentID, "error", err)
		}
	}
	l.buckets[agentID] = b
}

// Allow checks if a request from the given agent should be allowed.
func (l *Limiter) Allow(agentID string) bool {
	l.mu.Lock()

	// Check global limit
	if l.global != nil && !l.global.allow() {
		l.mu.Unlock()
		l.persistGlobal()
		return false
	}

	// Check per-agent limit
	if b, ok := l.buckets[agentID]; ok {
		ok := b.allow()
		l.mu.Unlock()
		l.persistAgent(agentID, b)
		l.persistGlobal()
		return ok
	}

	l.mu.Unlock()
	// Default-allow requests from agents without configured rate limits.
	// Persist the global drain so attackers cannot reset by restarting.
	l.persistGlobal()
	return true
}

func (l *Limiter) persistAgent(agentID string, b *bucket) {
	if l.store == nil {
		return
	}
	rec := bucketRecord{Tokens: b.tokens, LastRefill: b.lastRefill}
	if err := l.store.Set(context.Background(), agentID, rec); err != nil && l.logger != nil {
		l.logger.Warn("ratelimit: persist bucket failed", "agent", agentID, "error", err)
	}
}

func (l *Limiter) persistGlobal() {
	if l.store == nil || l.global == nil {
		return
	}
	rec := bucketRecord{Tokens: l.global.tokens, LastRefill: l.global.lastRefill}
	if err := l.store.Set(context.Background(), "__global__", rec); err != nil && l.logger != nil {
		l.logger.Warn("ratelimit: persist global bucket failed", "error", err)
	}
}

// AgentStatus describes the current rate limit state for an agent.
type AgentStatus struct {
	AgentID          string  `json:"agent_id"`
	TokensRemaining  float64 `json:"tokens_remaining"`
	MaxTokens        float64 `json:"max_tokens"`
	RefillRatePerSec float64 `json:"refill_rate_per_sec"`
}

// Status returns rate limit status for all configured agents.
func (l *Limiter) Status() []AgentStatus {
	l.mu.Lock()
	defer l.mu.Unlock()

	var result []AgentStatus
	for agentID, b := range l.buckets {
		now := time.Now()
		elapsed := now.Sub(b.lastRefill).Seconds()
		tokens := b.tokens + elapsed*b.refillRate
		if tokens > b.maxTokens {
			tokens = b.maxTokens
		}
		result = append(result, AgentStatus{
			AgentID:          agentID,
			TokensRemaining:  tokens,
			MaxTokens:        b.maxTokens,
			RefillRatePerSec: b.refillRate,
		})
	}
	return result
}
