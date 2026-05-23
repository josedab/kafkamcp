// Package backend defines the KV abstraction used by KafkaMCP engines that
// need to persist state across server restarts (agent memory, materialized
// views, rate-limit budgets, audit ring buffer, ...).
//
// The interface is deliberately narrow:
//
//   - A namespace partitions keys (e.g. "agentmemory", "viewstore").
//   - Keys and values are opaque to the backend; the caller is responsible
//     for marshaling (typically JSON via the generic [Store] helper in the
//     parent engine package).
//
// Two implementations ship with KafkaMCP:
//
//   - "inmem" — a sync.Map-backed driver used by tests and by deployments
//     that intentionally opt out of durable state (the historical default).
//   - "sqlite" — an embedded SQLite driver (modernc.org/sqlite, no cgo)
//     using WAL mode, suitable for single-node production deployments.
//
// Backends are safe for concurrent use by multiple goroutines. Closing a
// backend that is still being used returns [ErrClosed] from subsequent
// operations.
package backend

import (
	"context"
	"errors"
)

// Entry is a single key/value pair returned by [Backend.List].
type Entry struct {
	Namespace string
	Key       string
	Value     []byte
}

// Backend is the storage abstraction used by persistent KafkaMCP engines.
//
// Implementations must be safe for concurrent use.
type Backend interface {
	// Get returns the value stored at (namespace, key). It returns
	// [ErrNotFound] if the key does not exist.
	Get(ctx context.Context, namespace, key string) ([]byte, error)

	// Set stores value at (namespace, key), overwriting any previous
	// value.
	Set(ctx context.Context, namespace, key string, value []byte) error

	// Delete removes (namespace, key). It is not an error to delete a
	// key that does not exist.
	Delete(ctx context.Context, namespace, key string) error

	// List returns all entries in namespace whose key has the given
	// prefix. When prefix is empty, all entries in the namespace are
	// returned. The order of the returned slice is unspecified.
	List(ctx context.Context, namespace, prefix string) ([]Entry, error)

	// Close releases backend resources. After Close, all other methods
	// return [ErrClosed].
	Close() error
}

// Sentinel errors returned by Backend implementations.
var (
	// ErrNotFound is returned by Get when the key does not exist.
	ErrNotFound = errors.New("kafkamcp/engine/backend: key not found")

	// ErrClosed is returned by any operation invoked on a closed
	// backend.
	ErrClosed = errors.New("kafkamcp/engine/backend: backend closed")
)
