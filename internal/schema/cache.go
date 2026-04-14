package schema

import (
	"sync"
	"time"
)

// Cache is a simple LRU cache with TTL for schema entries.
type Cache struct {
	mu         sync.RWMutex
	entries    map[string]*cacheEntry
	maxEntries int
	ttl        time.Duration
}

type cacheEntry struct {
	value     *SchemaInfo
	expiresAt time.Time
}

// NewCache creates a new schema cache.
func NewCache(maxEntries int, ttl time.Duration) *Cache {
	if maxEntries <= 0 {
		maxEntries = 10000
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &Cache{
		entries:    make(map[string]*cacheEntry),
		maxEntries: maxEntries,
		ttl:        ttl,
	}
}

// Get retrieves a cached schema entry.
func (c *Cache) Get(key string) (*SchemaInfo, bool) {
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok {
		return nil, false
	}
	if time.Now().After(entry.expiresAt) {
		c.mu.Lock()
		delete(c.entries, key)
		c.mu.Unlock()
		return nil, false
	}
	return entry.value, true
}

// Set stores a schema entry in the cache.
func (c *Cache) Set(key string, value *SchemaInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Evict oldest entries if at capacity
	if len(c.entries) >= c.maxEntries {
		c.evictOldest()
	}

	c.entries[key] = &cacheEntry{
		value:     value,
		expiresAt: time.Now().Add(c.ttl),
	}
}

func (c *Cache) evictOldest() {
	var oldestKey string
	var oldestTime time.Time

	for k, v := range c.entries {
		if oldestKey == "" || v.expiresAt.Before(oldestTime) {
			oldestKey = k
			oldestTime = v.expiresAt
		}
	}
	if oldestKey != "" {
		delete(c.entries, oldestKey)
	}
}

// Stats returns cache statistics.
type CacheStats struct {
	Size       int `json:"size"`
	MaxEntries int `json:"max_entries"`
}

// Stats returns current cache statistics.
func (c *Cache) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return CacheStats{
		Size:       len(c.entries),
		MaxEntries: c.maxEntries,
	}
}
