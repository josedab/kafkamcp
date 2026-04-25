// Package catalog provides an auto-cataloging system for Kafka topics, schemas,
// and metadata with full-text search, tagging, and data quality scoring.
package catalog

import (
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// CatalogEntry represents a cataloged dataset (topic, schema, etc.).
type CatalogEntry struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Type         string            `json:"type"` // topic, schema, connector, consumer_group
	Description  string            `json:"description,omitempty"`
	Owner        string            `json:"owner,omitempty"`
	Tags         []string          `json:"tags,omitempty"`
	Properties   map[string]any    `json:"properties,omitempty"`
	QualityScore float64           `json:"quality_score"` // 0-100
	Freshness    string            `json:"freshness"`     // live, stale, inactive
	LastUpdated  time.Time         `json:"last_updated"`
	CreatedAt    time.Time         `json:"created_at"`
	UpdatedAt    time.Time         `json:"updated_at"`
	Lineage      *LineageInfo      `json:"lineage,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

// LineageInfo tracks data lineage for a catalog entry.
type LineageInfo struct {
	Upstream   []string `json:"upstream,omitempty"`
	Downstream []string `json:"downstream,omitempty"`
	Producers  []string `json:"producers,omitempty"`
	Consumers  []string `json:"consumers,omitempty"`
}

// SearchOptions defines search parameters for the catalog.
type SearchOptions struct {
	Query       string   `json:"query,omitempty"`
	Type        string   `json:"type,omitempty"`
	Tags        []string `json:"tags,omitempty"`
	Owner       string   `json:"owner,omitempty"`
	MinQuality  float64  `json:"min_quality,omitempty"`
	Freshness   string   `json:"freshness,omitempty"`
	Limit       int      `json:"limit,omitempty"`
}

// SearchResult is a catalog entry with relevance score.
type SearchResult struct {
	Entry CatalogEntry `json:"entry"`
	Score float64      `json:"score"`
}

// CatalogStats contains aggregate statistics about the catalog.
type CatalogStats struct {
	TotalEntries   int               `json:"total_entries"`
	ByType         map[string]int    `json:"by_type"`
	ByFreshness    map[string]int    `json:"by_freshness"`
	AvgQuality     float64           `json:"avg_quality_score"`
	TopTags        map[string]int    `json:"top_tags"`
	LastCrawl      time.Time         `json:"last_crawl"`
	StaleCount     int               `json:"stale_count"`
	UntaggedCount  int               `json:"untagged_count"`
}

// Engine manages the data catalog.
type Engine struct {
	mu        sync.RWMutex
	entries   map[string]*CatalogEntry
	lastCrawl time.Time
	logger    *slog.Logger
	nextID    int64
}

// NewEngine creates a new catalog engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		entries: make(map[string]*CatalogEntry),
		logger:  logger,
	}
}

// Index adds or updates a catalog entry.
func (e *Engine) Index(name, entryType, description, owner string, tags []string, properties map[string]any, metadata map[string]string) (*CatalogEntry, error) {
	if name == "" || entryType == "" {
		return nil, fmt.Errorf("name and type are required")
	}

	validTypes := map[string]bool{"topic": true, "schema": true, "connector": true, "consumer_group": true}
	if !validTypes[entryType] {
		return nil, fmt.Errorf("invalid type: %s", entryType)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Check if entry exists by name+type
	var existing *CatalogEntry
	for _, entry := range e.entries {
		if entry.Name == name && entry.Type == entryType {
			existing = entry
			break
		}
	}

	if existing != nil {
		existing.Description = description
		existing.Owner = owner
		existing.Tags = tags
		existing.Properties = properties
		existing.Metadata = metadata
		existing.UpdatedAt = time.Now()
		existing.LastUpdated = time.Now()
		existing.QualityScore = calculateQuality(existing)
		existing.Freshness = calculateFreshness(existing.LastUpdated)
		return existing, nil
	}

	e.nextID++
	id := fmt.Sprintf("cat_%d", e.nextID)

	entry := &CatalogEntry{
		ID:          id,
		Name:        name,
		Type:        entryType,
		Description: description,
		Owner:       owner,
		Tags:        tags,
		Properties:  properties,
		Metadata:    metadata,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		LastUpdated: time.Now(),
	}
	entry.QualityScore = calculateQuality(entry)
	entry.Freshness = calculateFreshness(entry.LastUpdated)

	e.entries[id] = entry
	e.logger.Info("catalog entry indexed", "id", id, "name", name, "type", entryType)
	return entry, nil
}

// Search searches the catalog with full-text and faceted filtering.
func (e *Engine) Search(opts SearchOptions) []SearchResult {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if opts.Limit <= 0 {
		opts.Limit = 20
	}

	var results []SearchResult

	for _, entry := range e.entries {
		if opts.Type != "" && entry.Type != opts.Type {
			continue
		}
		if opts.Owner != "" && entry.Owner != opts.Owner {
			continue
		}
		if opts.MinQuality > 0 && entry.QualityScore < opts.MinQuality {
			continue
		}
		if opts.Freshness != "" && entry.Freshness != opts.Freshness {
			continue
		}
		if len(opts.Tags) > 0 && !hasAnyTag(entry.Tags, opts.Tags) {
			continue
		}

		score := 1.0
		if opts.Query != "" {
			score = textMatch(entry, opts.Query)
			if score == 0 {
				continue
			}
		}

		results = append(results, SearchResult{Entry: *entry, Score: math.Round(score*1000) / 1000})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	if len(results) > opts.Limit {
		results = results[:opts.Limit]
	}
	return results
}

// Tag adds tags to a catalog entry.
func (e *Engine) Tag(entryID string, tags []string) (*CatalogEntry, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	entry, ok := e.entries[entryID]
	if !ok {
		return nil, fmt.Errorf("catalog entry %q not found", entryID)
	}

	// Merge tags (deduplicate)
	existing := make(map[string]bool)
	for _, t := range entry.Tags {
		existing[t] = true
	}
	for _, t := range tags {
		if !existing[t] {
			entry.Tags = append(entry.Tags, t)
			existing[t] = true
		}
	}

	entry.UpdatedAt = time.Now()
	entry.QualityScore = calculateQuality(entry)
	return entry, nil
}

// SetLineage sets lineage information for a catalog entry.
func (e *Engine) SetLineage(entryID string, lineage *LineageInfo) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	entry, ok := e.entries[entryID]
	if !ok {
		return fmt.Errorf("catalog entry %q not found", entryID)
	}

	entry.Lineage = lineage
	entry.UpdatedAt = time.Now()
	return nil
}

// Stats returns aggregate catalog statistics.
func (e *Engine) Stats() *CatalogStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	stats := &CatalogStats{
		ByType:      make(map[string]int),
		ByFreshness: make(map[string]int),
		TopTags:     make(map[string]int),
		LastCrawl:   e.lastCrawl,
	}

	var totalQuality float64
	for _, entry := range e.entries {
		stats.TotalEntries++
		stats.ByType[entry.Type]++
		stats.ByFreshness[entry.Freshness]++
		totalQuality += entry.QualityScore

		if entry.Freshness == "stale" || entry.Freshness == "inactive" {
			stats.StaleCount++
		}
		if len(entry.Tags) == 0 {
			stats.UntaggedCount++
		}
		for _, tag := range entry.Tags {
			stats.TopTags[tag]++
		}
	}

	if stats.TotalEntries > 0 {
		stats.AvgQuality = math.Round(totalQuality/float64(stats.TotalEntries)*100) / 100
	}
	return stats
}

// GetEntry returns a catalog entry by ID.
func (e *Engine) GetEntry(id string) (*CatalogEntry, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	entry, ok := e.entries[id]
	if !ok {
		return nil, fmt.Errorf("catalog entry %q not found", id)
	}
	return entry, nil
}

// DeleteEntry removes a catalog entry.
func (e *Engine) DeleteEntry(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.entries[id]; !ok {
		return fmt.Errorf("catalog entry %q not found", id)
	}
	delete(e.entries, id)
	return nil
}

// MarkCrawled records a crawl timestamp.
func (e *Engine) MarkCrawled() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lastCrawl = time.Now()
}

func calculateQuality(entry *CatalogEntry) float64 {
	score := 0.0

	// Has description (+25)
	if entry.Description != "" {
		score += 25
	}
	// Has owner (+20)
	if entry.Owner != "" {
		score += 20
	}
	// Has tags (+15)
	if len(entry.Tags) > 0 {
		score += 15
	}
	// Has lineage (+15)
	if entry.Lineage != nil {
		score += 15
	}
	// Has metadata (+10)
	if len(entry.Metadata) > 0 {
		score += 10
	}
	// Has properties (+10)
	if len(entry.Properties) > 0 {
		score += 10
	}
	// Freshness (+5)
	if entry.Freshness == "live" {
		score += 5
	}

	return math.Min(score, 100)
}

func calculateFreshness(lastUpdated time.Time) string {
	age := time.Since(lastUpdated)
	switch {
	case age < 1*time.Hour:
		return "live"
	case age < 24*time.Hour:
		return "live"
	case age < 7*24*time.Hour:
		return "stale"
	default:
		return "inactive"
	}
}

func hasAnyTag(entryTags, searchTags []string) bool {
	tagSet := make(map[string]bool)
	for _, t := range entryTags {
		tagSet[strings.ToLower(t)] = true
	}
	for _, t := range searchTags {
		if tagSet[strings.ToLower(t)] {
			return true
		}
	}
	return false
}

func textMatch(entry *CatalogEntry, query string) float64 {
	query = strings.ToLower(query)
	terms := strings.Fields(query)
	score := 0.0

	searchable := strings.ToLower(strings.Join([]string{
		entry.Name,
		entry.Description,
		entry.Owner,
		entry.Type,
		strings.Join(entry.Tags, " "),
	}, " "))

	for _, term := range terms {
		if strings.Contains(searchable, term) {
			score += 1.0
		}
		// Boost for name match
		if strings.Contains(strings.ToLower(entry.Name), term) {
			score += 2.0
		}
		// Boost for tag match
		for _, tag := range entry.Tags {
			if strings.EqualFold(tag, term) {
				score += 1.5
			}
		}
	}

	if len(terms) > 0 {
		score /= float64(len(terms))
	}
	return score
}
