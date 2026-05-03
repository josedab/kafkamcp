package timetravel

import (
	"fmt"
	"log/slog"
	"reflect"
	"sort"
	"sync"
	"time"
)

const defaultMaxSnapshots = 10000

// StateSnapshot captures entity state at a point in time.
type StateSnapshot struct {
	Key       string         `json:"key"`
	Type      string         `json:"type"`
	Timestamp time.Time      `json:"timestamp"`
	State     map[string]any `json:"state"`
}

// TimeTravelQuery describes a point-in-time state lookup.
type TimeTravelQuery struct {
	Key       string    `json:"key"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
}

// TimeTravelResult is returned by QueryAt.
type TimeTravelResult struct {
	Key          string         `json:"key"`
	Type         string         `json:"type"`
	QueryTime    time.Time      `json:"query_time"`
	SnapshotTime time.Time      `json:"snapshot_time"`
	State        map[string]any `json:"state"`
	Exact        bool           `json:"exact_match"`
	History      []StateChange  `json:"history,omitempty"`
}

// StateChange records what changed between consecutive snapshots.
type StateChange struct {
	Timestamp time.Time     `json:"timestamp"`
	Changes   []FieldChange `json:"changes"`
}

// FieldChange describes a single field difference.
type FieldChange struct {
	Field    string `json:"field"`
	OldValue any    `json:"old_value"`
	NewValue any    `json:"new_value"`
}

// Engine manages time-sorted state snapshots for point-in-time queries.
type Engine struct {
	mu           sync.RWMutex
	snapshots    map[string][]StateSnapshot
	logger       *slog.Logger
	maxSnapshots int
}

// NewEngine creates a new time-travel query engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		snapshots:    make(map[string][]StateSnapshot),
		logger:       logger,
		maxSnapshots: defaultMaxSnapshots,
	}
}

// RecordSnapshot stores a state snapshot for the given key.
func (e *Engine) RecordSnapshot(key, snapshotType string, state map[string]any) {
	e.mu.Lock()
	defer e.mu.Unlock()

	snap := StateSnapshot{
		Key:       key,
		Type:      snapshotType,
		Timestamp: time.Now().UTC(),
		State:     state,
	}

	list := e.snapshots[key]
	list = append(list, snap)

	// Evict oldest when over limit.
	if len(list) > e.maxSnapshots {
		list = list[len(list)-e.maxSnapshots:]
	}

	e.snapshots[key] = list
}

// QueryAt returns the state at the closest snapshot <= the requested timestamp.
func (e *Engine) QueryAt(query TimeTravelQuery) (*TimeTravelResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	list, ok := e.snapshots[query.Key]
	if !ok || len(list) == 0 {
		return nil, fmt.Errorf("no snapshots for key %q", query.Key)
	}

	// Binary search for the latest snapshot at or before query.Timestamp.
	idx := sort.Search(len(list), func(i int) bool {
		return list[i].Timestamp.After(query.Timestamp)
	})
	idx-- // step back to the snapshot <= timestamp

	if idx < 0 {
		return nil, fmt.Errorf("no snapshot at or before %s for key %q", query.Timestamp.Format(time.RFC3339), query.Key)
	}

	snap := list[idx]
	exact := snap.Timestamp.Equal(query.Timestamp)

	return &TimeTravelResult{
		Key:          snap.Key,
		Type:         snap.Type,
		QueryTime:    query.Timestamp,
		SnapshotTime: snap.Timestamp,
		State:        snap.State,
		Exact:        exact,
	}, nil
}

// QueryRange returns all snapshots for key within [from, to], optionally filtered by type.
func (e *Engine) QueryRange(key, snapshotType string, from, to time.Time) ([]StateSnapshot, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	list, ok := e.snapshots[key]
	if !ok {
		return nil, fmt.Errorf("no snapshots for key %q", key)
	}

	const maxResults = 1000
	var result []StateSnapshot
	for _, s := range list {
		if (s.Timestamp.Equal(from) || s.Timestamp.After(from)) &&
			(s.Timestamp.Equal(to) || s.Timestamp.Before(to)) {
			if snapshotType != "" && s.Type != snapshotType {
				continue
			}
			result = append(result, s)
			if len(result) >= maxResults {
				break
			}
		}
	}
	return result, nil
}

// DiffAt compares state at two points in time for the same key.
func (e *Engine) DiffAt(key, snapshotType string, t1, t2 time.Time) ([]FieldChange, error) {
	r1, err := e.QueryAt(TimeTravelQuery{Key: key, Type: snapshotType, Timestamp: t1})
	if err != nil {
		return nil, fmt.Errorf("querying state at t1: %w", err)
	}
	r2, err := e.QueryAt(TimeTravelQuery{Key: key, Type: snapshotType, Timestamp: t2})
	if err != nil {
		return nil, fmt.Errorf("querying state at t2: %w", err)
	}

	return diffMaps(r1.State, r2.State), nil
}

// ListKeys returns all tracked keys, optionally filtered by snapshot type.
func (e *Engine) ListKeys(snapshotType string) []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var keys []string
	for k, list := range e.snapshots {
		if snapshotType == "" {
			keys = append(keys, k)
			continue
		}
		for _, s := range list {
			if s.Type == snapshotType {
				keys = append(keys, k)
				break
			}
		}
	}
	sort.Strings(keys)
	return keys
}

// GetLatest returns the most recent snapshot for a key.
func (e *Engine) GetLatest(key string) (*StateSnapshot, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	list, ok := e.snapshots[key]
	if !ok || len(list) == 0 {
		return nil, fmt.Errorf("no snapshots for key %q", key)
	}
	latest := list[len(list)-1]
	return &latest, nil
}

// diffMaps computes field-level differences between two state maps.
func diffMaps(a, b map[string]any) []FieldChange {
	var changes []FieldChange

	for k, va := range a {
		vb, ok := b[k]
		if !ok {
			changes = append(changes, FieldChange{Field: k, OldValue: va, NewValue: nil})
		} else if !reflect.DeepEqual(va, vb) {
			changes = append(changes, FieldChange{Field: k, OldValue: va, NewValue: vb})
		}
	}
	for k, vb := range b {
		if _, ok := a[k]; !ok {
			changes = append(changes, FieldChange{Field: k, OldValue: nil, NewValue: vb})
		}
	}

	sort.Slice(changes, func(i, j int) bool { return changes[i].Field < changes[j].Field })
	return changes
}
