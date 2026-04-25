package catalog

import (
	"testing"
	"time"
)

// --- SetLineage ---

func TestSetLineageExisting(t *testing.T) {
	e := NewEngine(testLogger())
	entry, _ := e.Index("orders", "topic", "", "", nil, nil, nil)

	lineage := &LineageInfo{
		Upstream:   []string{"raw-events"},
		Downstream: []string{"analytics"},
		Producers:  []string{"order-service"},
		Consumers:  []string{"report-service"},
	}
	err := e.SetLineage(entry.ID, lineage)
	if err != nil {
		t.Fatalf("SetLineage failed: %v", err)
	}

	got, _ := e.GetEntry(entry.ID)
	if got.Lineage == nil {
		t.Fatal("expected lineage to be set")
	}
	if len(got.Lineage.Upstream) != 1 {
		t.Errorf("expected 1 upstream, got %d", len(got.Lineage.Upstream))
	}
}

func TestSetLineageNonExistent(t *testing.T) {
	e := NewEngine(testLogger())
	err := e.SetLineage("nonexistent", &LineageInfo{})
	if err == nil {
		t.Error("expected error for nonexistent entry")
	}
}

// --- GetEntry ---

func TestGetEntryExisting(t *testing.T) {
	e := NewEngine(testLogger())
	entry, _ := e.Index("orders", "topic", "desc", "owner", []string{"tag"}, map[string]any{"key": "val"}, map[string]string{"env": "prod"})

	got, err := e.GetEntry(entry.ID)
	if err != nil {
		t.Fatalf("GetEntry failed: %v", err)
	}
	if got.Name != "orders" {
		t.Errorf("expected orders, got %s", got.Name)
	}
	if got.Description != "desc" {
		t.Errorf("expected desc, got %s", got.Description)
	}
	if got.Owner != "owner" {
		t.Errorf("expected owner, got %s", got.Owner)
	}
}

func TestGetEntryNonExistent(t *testing.T) {
	e := NewEngine(testLogger())
	_, err := e.GetEntry("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent entry")
	}
}

// --- DeleteEntry ---

func TestDeleteEntryExisting(t *testing.T) {
	e := NewEngine(testLogger())
	entry, _ := e.Index("orders", "topic", "", "", nil, nil, nil)

	err := e.DeleteEntry(entry.ID)
	if err != nil {
		t.Fatalf("DeleteEntry failed: %v", err)
	}

	_, err = e.GetEntry(entry.ID)
	if err == nil {
		t.Error("expected error after delete")
	}
}

func TestDeleteEntryNonExistent(t *testing.T) {
	e := NewEngine(testLogger())
	err := e.DeleteEntry("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent entry")
	}
}

func TestDeleteEntryRemovedFromSearch(t *testing.T) {
	e := NewEngine(testLogger())
	entry, _ := e.Index("orders", "topic", "", "", nil, nil, nil)
	e.DeleteEntry(entry.ID)

	results := e.Search(SearchOptions{Query: "orders"})
	if len(results) != 0 {
		t.Errorf("expected 0 results after delete, got %d", len(results))
	}
}

// --- MarkCrawled ---

func TestMarkCrawled(t *testing.T) {
	e := NewEngine(testLogger())
	e.MarkCrawled()

	stats := e.Stats()
	if stats.LastCrawl.IsZero() {
		t.Error("expected non-zero LastCrawl")
	}
}

// --- hasAnyTag ---

func TestHasAnyTagMatching(t *testing.T) {
	if !hasAnyTag([]string{"prod", "pii"}, []string{"pii"}) {
		t.Error("expected match")
	}
}

func TestHasAnyTagNoMatch(t *testing.T) {
	if hasAnyTag([]string{"prod"}, []string{"dev"}) {
		t.Error("expected no match")
	}
}

func TestHasAnyTagEmptyEntryTags(t *testing.T) {
	if hasAnyTag([]string{}, []string{"prod"}) {
		t.Error("expected false for empty entry tags")
	}
}

func TestHasAnyTagEmptySearchTags(t *testing.T) {
	if hasAnyTag([]string{"prod"}, []string{}) {
		t.Error("expected false for empty search tags")
	}
}

func TestHasAnyTagCaseInsensitive(t *testing.T) {
	if !hasAnyTag([]string{"PROD"}, []string{"prod"}) {
		t.Error("expected case-insensitive match")
	}
}

// --- calculateFreshness ---

func TestCalculateFreshnessLive(t *testing.T) {
	f := calculateFreshness(time.Now())
	if f != "live" {
		t.Errorf("expected live, got %s", f)
	}
}

func TestCalculateFreshnessLiveUnder24h(t *testing.T) {
	f := calculateFreshness(time.Now().Add(-12 * time.Hour))
	if f != "live" {
		t.Errorf("expected live for 12h old, got %s", f)
	}
}

func TestCalculateFreshnessStale(t *testing.T) {
	f := calculateFreshness(time.Now().Add(-3 * 24 * time.Hour))
	if f != "stale" {
		t.Errorf("expected stale, got %s", f)
	}
}

func TestCalculateFreshnessInactive(t *testing.T) {
	f := calculateFreshness(time.Now().Add(-30 * 24 * time.Hour))
	if f != "inactive" {
		t.Errorf("expected inactive, got %s", f)
	}
}

// --- Index upsert behavior ---

func TestIndexUpsert(t *testing.T) {
	e := NewEngine(testLogger())
	entry1, _ := e.Index("orders", "topic", "desc1", "owner1", nil, nil, nil)
	entry2, _ := e.Index("orders", "topic", "desc2", "owner2", nil, nil, nil)

	if entry1.ID != entry2.ID {
		t.Error("expected same ID for upsert")
	}
	if entry2.Description != "desc2" {
		t.Errorf("expected updated desc, got %s", entry2.Description)
	}
}

func TestIndexEmptyName(t *testing.T) {
	e := NewEngine(testLogger())
	_, err := e.Index("", "topic", "", "", nil, nil, nil)
	if err == nil {
		t.Error("expected error for empty name")
	}
}

func TestIndexInvalidType(t *testing.T) {
	e := NewEngine(testLogger())
	_, err := e.Index("test", "invalid_type", "", "", nil, nil, nil)
	if err == nil {
		t.Error("expected error for invalid type")
	}
}

// --- Search ---

func TestSearchByOwner(t *testing.T) {
	e := NewEngine(testLogger())
	e.Index("t1", "topic", "", "team-a", nil, nil, nil)
	e.Index("t2", "topic", "", "team-b", nil, nil, nil)

	results := e.Search(SearchOptions{Owner: "team-a"})
	if len(results) != 1 {
		t.Errorf("expected 1, got %d", len(results))
	}
}

func TestSearchByMinQuality(t *testing.T) {
	e := NewEngine(testLogger())
	e.Index("full", "topic", "desc", "owner", []string{"tag"}, map[string]any{"k": "v"}, map[string]string{"m": "v"})
	e.Index("bare", "topic", "", "", nil, nil, nil)

	results := e.Search(SearchOptions{MinQuality: 50})
	if len(results) != 1 {
		t.Errorf("expected 1 high-quality, got %d", len(results))
	}
}

func TestSearchByTags(t *testing.T) {
	e := NewEngine(testLogger())
	e.Index("t1", "topic", "", "", []string{"prod"}, nil, nil)
	e.Index("t2", "topic", "", "", []string{"dev"}, nil, nil)

	results := e.Search(SearchOptions{Tags: []string{"prod"}})
	if len(results) != 1 {
		t.Errorf("expected 1, got %d", len(results))
	}
}

func TestSearchLimit(t *testing.T) {
	e := NewEngine(testLogger())
	for i := 0; i < 5; i++ {
		e.Index("t"+string(rune('a'+i)), "topic", "", "", nil, nil, nil)
	}

	results := e.Search(SearchOptions{Limit: 2})
	if len(results) != 2 {
		t.Errorf("expected 2, got %d", len(results))
	}
}

// --- Tag deduplication ---

func TestTagDeduplication(t *testing.T) {
	e := NewEngine(testLogger())
	entry, _ := e.Index("t1", "topic", "", "", []string{"prod"}, nil, nil)

	updated, _ := e.Tag(entry.ID, []string{"prod", "new"})
	if len(updated.Tags) != 2 {
		t.Errorf("expected 2 tags (deduped), got %d: %v", len(updated.Tags), updated.Tags)
	}
}

func TestTagNonExistent(t *testing.T) {
	e := NewEngine(testLogger())
	_, err := e.Tag("nonexistent", []string{"tag"})
	if err == nil {
		t.Error("expected error for nonexistent entry")
	}
}

// --- calculateQuality ---

func TestCalculateQualityFullScore(t *testing.T) {
	entry := &CatalogEntry{
		Description: "full",
		Owner:       "owner",
		Tags:        []string{"tag"},
		Lineage:     &LineageInfo{},
		Metadata:    map[string]string{"k": "v"},
		Properties:  map[string]any{"k": "v"},
		Freshness:   "live",
	}
	score := calculateQuality(entry)
	if score != 100 {
		t.Errorf("expected 100, got %.0f", score)
	}
}

func TestCalculateQualityBareMinimum(t *testing.T) {
	entry := &CatalogEntry{}
	score := calculateQuality(entry)
	if score != 0 {
		t.Errorf("expected 0, got %.0f", score)
	}
}
