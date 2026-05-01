# ADR-005: Shared Engine Store Pattern

## Status

Accepted

## Context

KafkaMCP has 100+ subsystem engines (contracts, dagworkflow, digitaltwin, chaos, sandbox, etc.) that independently implement the same in-memory storage pattern:

```go
type Engine struct {
    mu    sync.RWMutex
    items map[string]*Item
    // ...
}
```

Each engine duplicates the same boilerplate: `RLock/RUnlock` for reads, `Lock/Unlock` for writes, nil-map checks, existence checks on create/delete, and iteration helpers. This leads to:

1. **~3000+ lines of duplicated locking and map operations** across the codebase.
2. **Inconsistent error messages** — some engines say "not found", others say "does not exist".
3. **Subtle concurrency bugs** — some engines forget `defer Unlock()`, or hold locks longer than needed.
4. **Cognitive overhead** — every new subsystem re-implements the same pattern from scratch.

### Options considered

1. **Status quo** — Each engine manages its own `sync.RWMutex` + `map`. Simple but unmaintainable at scale.
2. **Interface-based registry** — Define a `Store` interface. Loses type safety and requires type assertions.
3. **Generic store via Go generics** — Provide `engine.Store[T]` that engines embed or compose. Preserves type safety, eliminates boilerplate, and enables future persistence swaps.

## Decision

We introduce `internal/engine.Store[T]`, a generic, thread-safe in-memory store parameterized on the value type. Subsystem engines can embed or compose `engine.Store[T]` to gain standard CRUD operations without hand-rolling mutex/map code.

### API surface

```go
// Core CRUD
func NewStore[T any]() *Store[T]
func (s *Store[T]) Get(key string) (*T, bool)
func (s *Store[T]) MustGet(key string) (*T, error)
func (s *Store[T]) Set(key string, item *T)
func (s *Store[T]) Create(key string, item *T) error   // fails if key exists
func (s *Store[T]) Delete(key string) bool
func (s *Store[T]) MustDelete(key string) error

// Query
func (s *Store[T]) List() []*T
func (s *Store[T]) Keys() []string
func (s *Store[T]) Count() int
func (s *Store[T]) Has(key string) bool
func (s *Store[T]) Filter(fn func(string, *T) bool) []*T
func (s *Store[T]) ForEach(fn func(string, *T) bool)

// Mutation
func (s *Store[T]) Update(key string, fn func(*T)) error
func (s *Store[T]) Clear()
```

### Why `sync.RWMutex` + `map`

- **Read-heavy workload** — MCP tool calls are overwhelmingly reads (list, get, describe). `RWMutex` allows concurrent readers with no contention.
- **Simplicity** — No external dependencies, no garbage collection pressure from channels, straightforward deadlock debugging.
- **Sufficient scale** — In-memory engines serve a single MCP server process; the dataset fits in memory and does not require distributed coordination.

### Future persistence path

The `Store[T]` abstraction deliberately hides the storage backend. When persistence is needed (e.g., SQLite, BadgerDB, or remote state), we can:

1. Introduce a `Backend` interface (`Get`, `Set`, `Delete`, `List`).
2. Swap the in-memory map for a persistent backend without changing engine call sites.
3. Add optional write-ahead logging for crash recovery.

This is explicitly deferred until a concrete persistence requirement emerges.

## Consequences

### Positive

- New engines can be built in minutes by embedding `engine.Store[T]` and adding only domain-specific logic.
- Consistent error messages and locking semantics across all engines.
- Single place to add instrumentation (metrics, tracing) for store operations.
- Clear upgrade path to persistent storage.

### Negative

- Existing engines are not automatically migrated — adoption is incremental.
- Engines with multiple maps (e.g., `contracts` has `contracts`, `stats`, `modes`) need multiple `Store` instances or continue managing some maps manually.
- Generic type parameters add a small amount of syntactic complexity.

### Neutral

- This is a purely internal decision with no impact on the MCP protocol surface.
- The `sync.RWMutex` approach is well-understood in the Go ecosystem and does not introduce novel concurrency primitives.
