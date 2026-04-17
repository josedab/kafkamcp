package schema

import (
	"container/list"
	"sync"
	"time"
)

// Cache is a simple LRU cache with TTL for schema entries.
type Cache struct {
	mu         sync.RWMutex
	entries    map[string]*list.Element
	order      *list.List // insertion order for O(1) eviction
	maxEntries int
	ttl        time.Duration
}

type cacheEntry struct {
	key       string
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
		entries:    make(map[string]*list.Element),
		order:      list.New(),
		maxEntries: maxEntries,
		ttl:        ttl,
	}
}

// Get retrieves a cached schema entry.
func (c *Cache) Get(key string) (*SchemaInfo, bool) {
	c.mu.RLock()
	elem, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok {
		return nil, false
	}
	entry := elem.Value.(*cacheEntry)
	if time.Now().After(entry.expiresAt) {
		c.mu.Lock()
		c.removeElement(elem)
		c.mu.Unlock()
		return nil, false
	}
	return entry.value, true
}

// Set stores a schema entry in the cache.
func (c *Cache) Set(key string, value *SchemaInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Update existing entry
	if elem, ok := c.entries[key]; ok {
		c.order.Remove(elem)
		delete(c.entries, key)
	}

	// Evict oldest entry if at capacity
	if len(c.entries) >= c.maxEntries {
		c.evictOldest()
	}

	entry := &cacheEntry{
		key:       key,
		value:     value,
		expiresAt: time.Now().Add(c.ttl),
	}
	elem := c.order.PushBack(entry)
	c.entries[key] = elem
}

func (c *Cache) evictOldest() {
	front := c.order.Front()
	if front != nil {
		c.removeElement(front)
	}
}

func (c *Cache) removeElement(elem *list.Element) {
	entry := c.order.Remove(elem).(*cacheEntry)
	delete(c.entries, entry.key)
}

// CacheStats holds size and capacity metrics for the schema cache.
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
