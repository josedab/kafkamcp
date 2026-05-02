package ratelimit

import (
	"sync"
	"time"
)

// Limiter implements a per-agent token bucket rate limiter.
type Limiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	global  *bucket
}

type bucket struct {
	tokens     float64
	maxTokens  float64
	refillRate float64 // tokens per second
	lastRefill time.Time
}

// New creates a new rate limiter with a global limit.
func New(globalRequestsPerMinute int) *Limiter {
	l := &Limiter{
		buckets: make(map[string]*bucket),
	}
	if globalRequestsPerMinute > 0 {
		l.global = newBucket(globalRequestsPerMinute)
	}
	return l
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

// SetAgentLimit configures a per-agent rate limit.
func (l *Limiter) SetAgentLimit(agentID string, requestsPerMinute int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if requestsPerMinute > 0 {
		l.buckets[agentID] = newBucket(requestsPerMinute)
	}
}

// Allow checks if a request from the given agent should be allowed.
func (l *Limiter) Allow(agentID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check global limit
	if l.global != nil && !l.global.allow() {
		return false
	}

	// Check per-agent limit
	if b, ok := l.buckets[agentID]; ok {
		return b.allow()
	}

	// Default-allow requests from agents without configured rate limits.
	// The policy engine handles authorization separately; rate limiting
	// should not block agents that simply have no explicit quota.
	return true
}

// AgentStatus describes the current rate limit state for an agent.
type AgentStatus struct {
	AgentID           string  `json:"agent_id"`
	TokensRemaining   float64 `json:"tokens_remaining"`
	MaxTokens         float64 `json:"max_tokens"`
	RefillRatePerSec  float64 `json:"refill_rate_per_sec"`
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
