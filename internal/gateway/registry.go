package gateway

import (
	"log/slog"
	"sync"
)

// AdapterFactory creates a StreamBackend from a config and logger.
type AdapterFactory func(cfg BackendConfig, logger *slog.Logger) StreamBackend

var (
	adaptersMu sync.RWMutex
	adapters   = make(map[Protocol]AdapterFactory)
)

// RegisterAdapterFactory registers an adapter factory for a protocol.
// Adapter files call this from init() so the factory is available when
// the corresponding build tag is enabled.
func RegisterAdapterFactory(proto Protocol, factory AdapterFactory) {
	adaptersMu.Lock()
	defer adaptersMu.Unlock()
	adapters[proto] = factory
}

// lookupAdapterFactory returns the factory for a protocol, if registered.
func lookupAdapterFactory(proto Protocol) (AdapterFactory, bool) {
	adaptersMu.RLock()
	defer adaptersMu.RUnlock()
	f, ok := adapters[proto]
	return f, ok
}
