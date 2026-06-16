// Package engine provides shared infrastructure for KafkaMCP "engines" —
// long-lived in-process subsystems such as agent memory, materialized
// views, audit logging, and rate limiting.
//
// Historically each engine kept its own ad-hoc map[string]T in memory.
// This package introduces:
//
//   - A pluggable storage [backend.Backend] (in-memory or embedded SQLite).
//
//   - A typed [Store] helper that marshals/unmarshals JSON values for an
//     engine namespace.
//
//   - Configuration plumbing (see [internal/config.PersistenceConfig]) so
//     operators can opt into durable state via:
//
//     server:
//     persistence:
//     backend: sqlite
//     path: /var/lib/kafkamcp/state.db
//
// The default is the in-memory backend, preserving the prior behavior
// for existing deployments.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/josedab/kafkamcp/internal/engine/backend"
)

// Store is a typed wrapper around a [backend.Backend] for a single
// namespace. Values are marshaled as JSON. It is safe for concurrent use
// by multiple goroutines.
type Store[T any] struct {
	be        backend.Backend
	namespace string
}

// VersionedValue contains a decoded value and the exact persisted bytes used
// as its compare-and-swap version. Callers must pass Raw back unchanged.
type VersionedValue[T any] struct {
	Value T
	Raw   []byte
}

// NewStore returns a typed Store rooted at namespace.
func NewStore[T any](be backend.Backend, namespace string) *Store[T] {
	return &Store[T]{be: be, namespace: namespace}
}

// Backend returns the underlying backend (useful for adapters that need
// to share a backend across multiple stores).
func (s *Store[T]) Backend() backend.Backend { return s.be }

// Namespace returns the namespace passed to NewStore.
func (s *Store[T]) Namespace() string { return s.namespace }

// Get loads a value by key. It returns ([T zero], false, nil) when the
// key does not exist and a non-nil error for any other failure.
func (s *Store[T]) Get(ctx context.Context, key string) (T, bool, error) {
	versioned, ok, err := s.GetVersioned(ctx, key)
	return versioned.Value, ok, err
}

// GetVersioned loads a value plus the exact raw bytes stored by the backend.
func (s *Store[T]) GetVersioned(ctx context.Context, key string) (VersionedValue[T], bool, error) {
	var zero T
	raw, err := s.be.Get(ctx, s.namespace, key)
	if errors.Is(err, backend.ErrNotFound) {
		return VersionedValue[T]{Value: zero}, false, nil
	}
	if err != nil {
		return VersionedValue[T]{Value: zero}, false, err
	}
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		return VersionedValue[T]{Value: zero}, false, fmt.Errorf("kafkamcp/engine: unmarshal %s/%s: %w", s.namespace, key, err)
	}
	return VersionedValue[T]{Value: v, Raw: append([]byte(nil), raw...)}, true, nil
}

// Set stores value at key, replacing any previous value.
func (s *Store[T]) Set(ctx context.Context, key string, value T) error {
	raw, err := s.marshal(key, value)
	if err != nil {
		return err
	}
	return s.be.Set(ctx, s.namespace, key, raw)
}

// Create stores value only when key is absent.
func (s *Store[T]) Create(ctx context.Context, key string, value T) (bool, error) {
	raw, err := s.marshal(key, value)
	if err != nil {
		return false, err
	}
	return s.be.CompareAndSwap(ctx, s.namespace, key, nil, raw)
}

// CompareAndSwap replaces a value only when the persisted bytes still match
// expectedRaw from GetVersioned or ListVersioned.
func (s *Store[T]) CompareAndSwap(ctx context.Context, key string, expectedRaw []byte, replacement T) (bool, error) {
	raw, err := s.marshal(key, replacement)
	if err != nil {
		return false, err
	}
	return s.be.CompareAndSwap(ctx, s.namespace, key, expectedRaw, raw)
}

// DeleteIfUnchanged removes a key only when its persisted bytes still match.
func (s *Store[T]) DeleteIfUnchanged(ctx context.Context, key string, expectedRaw []byte) (bool, error) {
	return s.be.CompareAndSwap(ctx, s.namespace, key, expectedRaw, nil)
}

// Delete removes key from the namespace; missing keys are not an error.
func (s *Store[T]) Delete(ctx context.Context, key string) error {
	return s.be.Delete(ctx, s.namespace, key)
}

// List returns every (key, value) pair in the namespace whose key starts
// with prefix. Pass an empty prefix to list everything.
func (s *Store[T]) List(ctx context.Context, prefix string) (map[string]T, error) {
	versioned, err := s.ListVersioned(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := make(map[string]T, len(versioned))
	for key, value := range versioned {
		out[key] = value.Value
	}
	return out, nil
}

// ListVersioned returns decoded values with their exact persisted bytes.
func (s *Store[T]) ListVersioned(ctx context.Context, prefix string) (map[string]VersionedValue[T], error) {
	entries, err := s.be.List(ctx, s.namespace, prefix)
	if err != nil {
		return nil, err
	}
	out := make(map[string]VersionedValue[T], len(entries))
	for _, e := range entries {
		var v T
		if err := json.Unmarshal(e.Value, &v); err != nil {
			return nil, fmt.Errorf("kafkamcp/engine: unmarshal %s/%s: %w", s.namespace, e.Key, err)
		}
		out[e.Key] = VersionedValue[T]{
			Value: v,
			Raw:   append([]byte(nil), e.Value...),
		}
	}
	return out, nil
}

func (s *Store[T]) marshal(key string, value T) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("kafkamcp/engine: marshal %s/%s: %w", s.namespace, key, err)
	}
	return raw, nil
}
