// Package engine provides a generic, thread-safe in-memory store that can be
// embedded by subsystem engines to eliminate duplicated sync.RWMutex + map
// boilerplate across the codebase.
package engine
