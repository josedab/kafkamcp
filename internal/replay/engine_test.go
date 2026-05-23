package replay

import (
	"log/slog"
	"testing"
)

func TestCreateBookmark(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	offsets := map[int32]int64{0: 100, 1: 200}
	bm := e.CreateBookmark("snap-1", "orders", "cluster-1", offsets, nil, []string{"prod"})
	if bm.Name != "snap-1" {
		t.Errorf("expected name snap-1, got %s", bm.Name)
	}
	if bm.Topic != "orders" {
		t.Errorf("expected topic orders, got %s", bm.Topic)
	}
	if bm.MessageCount != 300 {
		t.Errorf("expected 300 messages, got %d", bm.MessageCount)
	}
	if len(bm.Tags) != 1 || bm.Tags[0] != "prod" {
		t.Errorf("unexpected tags: %v", bm.Tags)
	}
}

func TestGetBookmark_Found(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	bm := e.CreateBookmark("snap-1", "orders", "c", map[int32]int64{0: 10}, nil, nil)
	got, err := e.GetBookmark(bm.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Name != "snap-1" {
		t.Errorf("expected snap-1, got %s", got.Name)
	}
}

func TestGetBookmark_NotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, err := e.GetBookmark("nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent bookmark")
	}
}

func TestListBookmarks_All(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.CreateBookmark("a", "orders", "c", map[int32]int64{0: 10}, nil, nil)
	e.CreateBookmark("b", "payments", "c", map[int32]int64{0: 20}, nil, nil)
	all := e.ListBookmarks("")
	if len(all) != 2 {
		t.Errorf("expected 2 bookmarks, got %d", len(all))
	}
}

func TestListBookmarks_TopicFilter(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.CreateBookmark("a", "orders", "c", map[int32]int64{0: 10}, nil, nil)
	e.CreateBookmark("b", "payments", "c", map[int32]int64{0: 20}, nil, nil)
	filtered := e.ListBookmarks("orders")
	if len(filtered) != 1 {
		t.Errorf("expected 1 bookmark for orders, got %d", len(filtered))
	}
}

func TestDeleteBookmark(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	bm := e.CreateBookmark("a", "orders", "c", map[int32]int64{0: 10}, nil, nil)
	err := e.DeleteBookmark(bm.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_, err = e.GetBookmark(bm.ID)
	if err == nil {
		t.Error("expected error after deletion")
	}
}

func TestDeleteBookmark_NotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	err := e.DeleteBookmark("nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent bookmark")
	}
}

func TestDiff_OverlappingOffsets(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	a := e.CreateBookmark("a", "orders", "c", map[int32]int64{0: 100, 1: 200}, nil, nil)
	b := e.CreateBookmark("b", "orders", "c", map[int32]int64{0: 150, 1: 200}, nil, nil)

	diff, err := e.Diff(a.ID, b.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if diff.Topic != "orders" {
		t.Errorf("expected topic orders, got %s", diff.Topic)
	}
	if diff.Added != 50 {
		t.Errorf("expected 50 added, got %d", diff.Added)
	}
}

func TestDiff_DisjointPartitions(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	a := e.CreateBookmark("a", "orders", "c", map[int32]int64{0: 100}, nil, nil)
	b := e.CreateBookmark("b", "orders", "c", map[int32]int64{0: 100, 1: 50}, nil, nil)

	diff, err := e.Diff(a.ID, b.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Partition 1 only in B, so 50 added
	if diff.Added != 50 {
		t.Errorf("expected 50 added for new partition, got %d", diff.Added)
	}
}

func TestDiff_DifferentTopics(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	a := e.CreateBookmark("a", "orders", "c", map[int32]int64{0: 100}, nil, nil)
	b := e.CreateBookmark("b", "payments", "c", map[int32]int64{0: 100}, nil, nil)

	_, err := e.Diff(a.ID, b.ID)
	if err == nil {
		t.Fatal("expected error for different topics")
	}
}

func TestDiff_NonExistentBookmark(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	bm := e.CreateBookmark("a", "orders", "c", map[int32]int64{0: 100}, nil, nil)

	_, err := e.Diff(bm.ID, "nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent bookmark B")
	}
	_, err = e.Diff("nonexistent", bm.ID)
	if err == nil {
		t.Fatal("expected error for non-existent bookmark A")
	}
}

func TestDiff_SampleMessageModified(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	samplesA := []SampleMessage{{Partition: 0, Offset: 10, ValueHash: "abc", Size: 100}}
	samplesB := []SampleMessage{{Partition: 0, Offset: 10, ValueHash: "def", Size: 200}}
	a := e.CreateBookmark("a", "orders", "c", map[int32]int64{0: 100}, samplesA, nil)
	b := e.CreateBookmark("b", "orders", "c", map[int32]int64{0: 100}, samplesB, nil)

	diff, err := e.Diff(a.ID, b.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if diff.Modified != 1 {
		t.Errorf("expected 1 modified, got %d", diff.Modified)
	}
	if len(diff.FieldChanges) != 1 {
		t.Errorf("expected 1 field change, got %d", len(diff.FieldChanges))
	}
}

func TestCreateSimulation(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	sim := e.CreateSimulation("replay-1", "orders", "group-1", map[int32]int64{0: 1000}, "")
	if sim.Name != "replay-1" {
		t.Errorf("expected name replay-1, got %s", sim.Name)
	}
	if sim.Status != "pending" {
		t.Errorf("expected pending status, got %s", sim.Status)
	}
	if len(sim.Predictions) == 0 {
		t.Error("expected predictions")
	}
}

func TestRunSimulation(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	sim := e.CreateSimulation("replay-1", "orders", "g", map[int32]int64{0: 500}, "")
	result, err := e.RunSimulation(sim.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != "completed" {
		t.Errorf("expected completed, got %s", result.Status)
	}
	if result.MessagesReplayed != 500 {
		t.Errorf("expected 500 messages replayed, got %d", result.MessagesReplayed)
	}
}

func TestRunSimulation_AlreadyCompleted(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	sim := e.CreateSimulation("replay-1", "orders", "g", map[int32]int64{0: 500}, "")
	_, _ = e.RunSimulation(sim.ID)

	_, err := e.RunSimulation(sim.ID)
	if err == nil {
		t.Fatal("expected error running already-completed simulation")
	}
}

func TestRunSimulation_NotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, err := e.RunSimulation("nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent simulation")
	}
}

func TestGetSimulation(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	sim := e.CreateSimulation("r", "t", "g", map[int32]int64{0: 10}, "")
	got, err := e.GetSimulation(sim.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Name != "r" {
		t.Errorf("expected name r, got %s", got.Name)
	}
}

func TestGetSimulation_NotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, err := e.GetSimulation("nope")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestListSimulations(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.CreateSimulation("a", "orders", "g", map[int32]int64{0: 10}, "")
	e.CreateSimulation("b", "payments", "g", map[int32]int64{0: 20}, "")

	all := e.ListSimulations("")
	if len(all) != 2 {
		t.Errorf("expected 2, got %d", len(all))
	}
	filtered := e.ListSimulations("orders")
	if len(filtered) != 1 {
		t.Errorf("expected 1 for orders, got %d", len(filtered))
	}
}

func TestEstimateImpact(t *testing.T) {
	t.Parallel()
	if estimateImpact(100) != "low" {
		t.Error("expected low")
	}
	if estimateImpact(50000) != "medium" {
		t.Error("expected medium")
	}
	if estimateImpact(2000000) != "high" {
		t.Error("expected high")
	}
}
