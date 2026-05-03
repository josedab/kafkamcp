package timetravel

import (
	"log/slog"
	"testing"
	"time"
)

func TestRecordAndQueryAt(t *testing.T) {
	e := NewEngine(slog.Default())

	t1 := time.Now().Add(-10 * time.Minute)
	t2 := time.Now().Add(-5 * time.Minute)
	t3 := time.Now()

	// Manually set timestamps by recording and manipulating
	e.mu.Lock()
	e.snapshots["topic:orders"] = []StateSnapshot{
		{Key: "topic:orders", Type: "topic", Timestamp: t1, State: map[string]any{"partitions": 3}},
		{Key: "topic:orders", Type: "topic", Timestamp: t2, State: map[string]any{"partitions": 6}},
		{Key: "topic:orders", Type: "topic", Timestamp: t3, State: map[string]any{"partitions": 12}},
	}
	e.mu.Unlock()

	// Query at exact t2
	r, err := e.QueryAt(TimeTravelQuery{Key: "topic:orders", Timestamp: t2})
	if err != nil {
		t.Fatal(err)
	}
	if r.State["partitions"] != 6 {
		t.Errorf("expected partitions=6, got %v", r.State["partitions"])
	}
	if !r.Exact {
		t.Error("expected exact match")
	}

	// Query between t1 and t2 — should return t1
	mid := t1.Add(3 * time.Minute)
	r, err = e.QueryAt(TimeTravelQuery{Key: "topic:orders", Timestamp: mid})
	if err != nil {
		t.Fatal(err)
	}
	if r.State["partitions"] != 3 {
		t.Errorf("expected partitions=3, got %v", r.State["partitions"])
	}

	// Query before first snapshot — should error
	_, err = e.QueryAt(TimeTravelQuery{Key: "topic:orders", Timestamp: t1.Add(-1 * time.Hour)})
	if err == nil {
		t.Error("expected error for query before all snapshots")
	}
}

func TestQueryRange(t *testing.T) {
	e := NewEngine(slog.Default())

	now := time.Now()
	e.mu.Lock()
	e.snapshots["topic:events"] = []StateSnapshot{
		{Key: "topic:events", Type: "topic", Timestamp: now.Add(-10 * time.Minute), State: map[string]any{"a": 1}},
		{Key: "topic:events", Type: "topic", Timestamp: now.Add(-5 * time.Minute), State: map[string]any{"a": 2}},
		{Key: "topic:events", Type: "consumer_group", Timestamp: now.Add(-3 * time.Minute), State: map[string]any{"a": 3}},
		{Key: "topic:events", Type: "topic", Timestamp: now, State: map[string]any{"a": 4}},
	}
	e.mu.Unlock()

	// Query all in range
	results, err := e.QueryRange("topic:events", "", now.Add(-11*time.Minute), now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 4 {
		t.Errorf("expected 4 results, got %d", len(results))
	}

	// Filter by type
	results, err = e.QueryRange("topic:events", "topic", now.Add(-11*time.Minute), now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 topic results, got %d", len(results))
	}
}

func TestDiffAt(t *testing.T) {
	e := NewEngine(slog.Default())

	t1 := time.Now().Add(-10 * time.Minute)
	t2 := time.Now()

	e.mu.Lock()
	e.snapshots["topic:orders"] = []StateSnapshot{
		{Key: "topic:orders", Type: "topic", Timestamp: t1, State: map[string]any{"partitions": 3, "retention": "7d"}},
		{Key: "topic:orders", Type: "topic", Timestamp: t2, State: map[string]any{"partitions": 6, "compression": "lz4"}},
	}
	e.mu.Unlock()

	changes, err := e.DiffAt("topic:orders", "topic", t1, t2)
	if err != nil {
		t.Fatal(err)
	}

	if len(changes) != 3 { // partitions changed, retention removed, compression added
		t.Errorf("expected 3 changes, got %d", len(changes))
	}

	changeMap := make(map[string]FieldChange)
	for _, c := range changes {
		changeMap[c.Field] = c
	}
	if c, ok := changeMap["partitions"]; ok {
		if c.OldValue != 3 || c.NewValue != 6 {
			t.Errorf("unexpected partitions change: %v -> %v", c.OldValue, c.NewValue)
		}
	} else {
		t.Error("missing partitions change")
	}
	if c, ok := changeMap["retention"]; ok {
		if c.NewValue != nil {
			t.Errorf("expected retention removed, got %v", c.NewValue)
		}
	} else {
		t.Error("missing retention change")
	}
}

func TestListKeys(t *testing.T) {
	e := NewEngine(slog.Default())
	e.RecordSnapshot("topic:a", "topic", map[string]any{"x": 1})
	e.RecordSnapshot("group:b", "consumer_group", map[string]any{"y": 2})

	all := e.ListKeys("")
	if len(all) != 2 {
		t.Errorf("expected 2 keys, got %d", len(all))
	}

	topics := e.ListKeys("topic")
	if len(topics) != 1 || topics[0] != "topic:a" {
		t.Errorf("expected [topic:a], got %v", topics)
	}
}

func TestGetLatest(t *testing.T) {
	e := NewEngine(slog.Default())
	e.RecordSnapshot("topic:x", "topic", map[string]any{"v": 1})
	e.RecordSnapshot("topic:x", "topic", map[string]any{"v": 2})

	snap, err := e.GetLatest("topic:x")
	if err != nil {
		t.Fatal(err)
	}
	if snap.State["v"] != 2 {
		t.Errorf("expected v=2, got %v", snap.State["v"])
	}

	_, err = e.GetLatest("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent key")
	}
}

func TestSnapshotEviction(t *testing.T) {
	e := NewEngine(slog.Default())
	e.maxSnapshots = 5

	for i := 0; i < 10; i++ {
		e.RecordSnapshot("key", "topic", map[string]any{"i": i})
	}

	e.mu.RLock()
	defer e.mu.RUnlock()
	if len(e.snapshots["key"]) != 5 {
		t.Errorf("expected 5 snapshots after eviction, got %d", len(e.snapshots["key"]))
	}
}
