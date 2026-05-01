package nlquery

import (
	"strings"
	"time"
)

// CacheResult stores a query result in the cache.
func (e *Engine) CacheResult(query string, result ExecutionResult) {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()

	// Evict expired entries
	for k, v := range e.queryCache {
		if now.Sub(v.CachedAt) > v.TTL {
			delete(e.queryCache, k)
		}
	}

	// If still at capacity, evict the oldest entry
	if len(e.queryCache) >= e.maxCacheEntries {
		var oldestKey string
		var oldestTime time.Time
		for k, v := range e.queryCache {
			if oldestKey == "" || v.CachedAt.Before(oldestTime) {
				oldestKey = k
				oldestTime = v.CachedAt
			}
		}
		if oldestKey != "" {
			delete(e.queryCache, oldestKey)
		}
	}

	e.queryCache[strings.ToLower(strings.TrimSpace(query))] = &CachedResult{
		Result:   result,
		CachedAt: now,
		TTL:      e.cacheTTL,
	}
}

// GetCached retrieves a cached result if available and not expired.
func (e *Engine) GetCached(query string) (*ExecutionResult, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	key := strings.ToLower(strings.TrimSpace(query))
	cached, ok := e.queryCache[key]
	if !ok {
		return nil, false
	}
	if time.Since(cached.CachedAt) > cached.TTL {
		delete(e.queryCache, key)
		return nil, false
	}
	result := cached.Result
	result.FromCache = true
	return &result, true
}
