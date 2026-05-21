package backend

import (
	"context"
	"strings"
	"sync"
)

// InMemory is the default in-process Backend implementation. It is backed
// by a sync.RWMutex-guarded map of namespaces, and is suitable for tests
// and for ephemeral deployments where durability is not required.
type InMemory struct {
	mu     sync.RWMutex
	data   map[string]map[string][]byte
	closed bool
}

// NewInMemory returns an empty in-memory backend.
func NewInMemory() *InMemory {
	return &InMemory{data: make(map[string]map[string][]byte)}
}

// Get implements Backend.
func (b *InMemory) Get(_ context.Context, namespace, key string) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, ErrClosed
	}
	ns, ok := b.data[namespace]
	if !ok {
		return nil, ErrNotFound
	}
	v, ok := ns[key]
	if !ok {
		return nil, ErrNotFound
	}
	// Return a defensive copy so callers cannot mutate stored bytes.
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

// Set implements Backend.
func (b *InMemory) Set(_ context.Context, namespace, key string, value []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	ns, ok := b.data[namespace]
	if !ok {
		ns = make(map[string][]byte)
		b.data[namespace] = ns
	}
	v := make([]byte, len(value))
	copy(v, value)
	ns[key] = v
	return nil
}

// Delete implements Backend.
func (b *InMemory) Delete(_ context.Context, namespace, key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	if ns, ok := b.data[namespace]; ok {
		delete(ns, key)
	}
	return nil
}

// List implements Backend.
func (b *InMemory) List(_ context.Context, namespace, prefix string) ([]Entry, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, ErrClosed
	}
	ns, ok := b.data[namespace]
	if !ok {
		return nil, nil
	}
	out := make([]Entry, 0, len(ns))
	for k, v := range ns {
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			continue
		}
		buf := make([]byte, len(v))
		copy(buf, v)
		out = append(out, Entry{Namespace: namespace, Key: k, Value: buf})
	}
	return out, nil
}

// Close implements Backend.
func (b *InMemory) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	b.data = nil
	return nil
}
