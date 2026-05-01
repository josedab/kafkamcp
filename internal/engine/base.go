// Package engine provides a generic, thread-safe in-memory store that can be
// embedded by subsystem engines to eliminate duplicated sync.RWMutex + map
// boilerplate across the codebase.
package engine

import (
	"fmt"
	"sync"
)

// Store is a generic, thread-safe in-memory key-value store.
// Subsystem engines can embed Store[T] instead of hand-rolling their own
// sync.RWMutex + map[string]*T pattern.
type Store[T any] struct {
	mu    sync.RWMutex
	items map[string]*T
}

// NewStore creates a new empty Store.
func NewStore[T any]() *Store[T] {
	return &Store[T]{
		items: make(map[string]*T),
	}
}

// Get retrieves an item by key. Returns the item and true if found.
func (s *Store[T]) Get(key string) (*T, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	item, ok := s.items[key]
	return item, ok
}

// MustGet retrieves an item by key, returning an error if not found.
func (s *Store[T]) MustGet(key string) (*T, error) {
	item, ok := s.Get(key)
	if !ok {
		return nil, fmt.Errorf("item %q not found", key)
	}
	return item, nil
}

// Set stores an item under the given key, overwriting any existing entry.
func (s *Store[T]) Set(key string, item *T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[key] = item
}

// Create stores an item only if the key does not already exist.
// Returns an error if the key is already present.
func (s *Store[T]) Create(key string, item *T) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.items[key]; exists {
		return fmt.Errorf("item %q already exists", key)
	}
	s.items[key] = item
	return nil
}

// Delete removes an item by key. Returns true if the item existed.
func (s *Store[T]) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, existed := s.items[key]
	delete(s.items, key)
	return existed
}

// MustDelete removes an item by key, returning an error if not found.
func (s *Store[T]) MustDelete(key string) error {
	if !s.Delete(key) {
		return fmt.Errorf("item %q not found", key)
	}
	return nil
}

// List returns all items as a slice. The caller receives copies of the
// pointers; the underlying store remains unchanged.
func (s *Store[T]) List() []*T {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*T, 0, len(s.items))
	for _, item := range s.items {
		result = append(result, item)
	}
	return result
}

// Keys returns all keys in the store.
func (s *Store[T]) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.items))
	for k := range s.items {
		keys = append(keys, k)
	}
	return keys
}

// Count returns the number of items in the store.
func (s *Store[T]) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.items)
}

// Has returns true if the key exists.
func (s *Store[T]) Has(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.items[key]
	return ok
}

// Update applies a mutator function to the item at key while holding the
// write lock. Returns an error if the key does not exist.
func (s *Store[T]) Update(key string, fn func(*T)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[key]
	if !ok {
		return fmt.Errorf("item %q not found", key)
	}
	fn(item)
	return nil
}

// ForEach calls fn for every key-value pair while holding a read lock.
// If fn returns false, iteration stops.
func (s *Store[T]) ForEach(fn func(key string, item *T) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k, v := range s.items {
		if !fn(k, v) {
			break
		}
	}
}

// Filter returns items matching the predicate.
func (s *Store[T]) Filter(fn func(key string, item *T) bool) []*T {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*T
	for k, v := range s.items {
		if fn(k, v) {
			result = append(result, v)
		}
	}
	return result
}

// Clear removes all items from the store.
func (s *Store[T]) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = make(map[string]*T)
}
