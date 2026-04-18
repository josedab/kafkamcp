package schema

import (
	"testing"
	"time"
)

func TestNewCacheDefaults(t *testing.T) {
	c := NewCache(0, 0)
	if c.maxEntries != 10000 {
		t.Errorf("expected maxEntries=10000, got %d", c.maxEntries)
	}
	if c.ttl != 5*time.Minute {
		t.Errorf("expected ttl=5m, got %v", c.ttl)
	}
}

func TestNewCacheNegative(t *testing.T) {
	c := NewCache(-1, -1)
	if c.maxEntries != 10000 {
		t.Errorf("expected maxEntries=10000, got %d", c.maxEntries)
	}
	if c.ttl != 5*time.Minute {
		t.Errorf("expected ttl=5m, got %v", c.ttl)
	}
}

func TestCacheSetUpdateExisting(t *testing.T) {
	c := NewCache(100, time.Hour)
	info1 := &SchemaInfo{Subject: "test", Version: 1}
	info2 := &SchemaInfo{Subject: "test", Version: 2}

	c.Set("key1", info1)
	c.Set("key1", info2) // update existing

	got, ok := c.Get("key1")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if got.Version != 2 {
		t.Errorf("expected version=2, got %d", got.Version)
	}
	stats := c.Stats()
	if stats.Size != 1 {
		t.Errorf("expected size=1, got %d", stats.Size)
	}
}

func TestCacheEvictionExtended(t *testing.T) {
	c := NewCache(2, time.Hour)
	c.Set("k1", &SchemaInfo{Subject: "s1"})
	c.Set("k2", &SchemaInfo{Subject: "s2"})
	c.Set("k3", &SchemaInfo{Subject: "s3"}) // should evict k1

	_, ok := c.Get("k1")
	if ok {
		t.Error("expected k1 to be evicted")
	}
	_, ok = c.Get("k3")
	if !ok {
		t.Error("expected k3 to be in cache")
	}
}

func TestCacheExpiry(t *testing.T) {
	c := NewCache(100, time.Millisecond)
	c.Set("k1", &SchemaInfo{Subject: "s1"})
	time.Sleep(5 * time.Millisecond)

	_, ok := c.Get("k1")
	if ok {
		t.Error("expected k1 to be expired")
	}
}
