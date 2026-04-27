package nlquery

import (
	"strings"
	"time"
)

// CacheResult stores a query result in the cache.
func (e *Engine) CacheResult(query string, result ExecutionResult) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.queryCache[strings.ToLower(strings.TrimSpace(query))] = &CachedResult{
		Result:   result,
		CachedAt: time.Now(),
		TTL:      e.cacheTTL,
	}
}

// GetCached retrieves a cached result if available and not expired.
func (e *Engine) GetCached(query string) (*ExecutionResult, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	key := strings.ToLower(strings.TrimSpace(query))
	cached, ok := e.queryCache[key]
	if !ok {
		return nil, false
	}
	if time.Since(cached.CachedAt) > cached.TTL {
		return nil, false
	}
	result := cached.Result
	result.FromCache = true
	return &result, true
}
