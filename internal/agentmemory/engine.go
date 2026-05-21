// Package agentmemory provides persistent per-agent memory with key-value storage,
// TTL expiration, and TF-IDF based semantic search for context recall.
package agentmemory

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/engine"
	"github.com/kafkamcp/kafkamcp/internal/engine/backend"
)

// MemoryEntry represents a single memory item stored by an agent.
type MemoryEntry struct {
	Key         string            `json:"key"`
	Value       any               `json:"value"`
	Category    string            `json:"category,omitempty"` // preference, schema, query_history, context, note
	Tags        []string          `json:"tags,omitempty"`
	AgentID     string            `json:"agent_id"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	ExpiresAt   *time.Time        `json:"expires_at,omitempty"`
	AccessCount int64             `json:"access_count"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// SearchResult is a memory entry with relevance score.
type SearchResult struct {
	Entry     MemoryEntry `json:"entry"`
	Score     float64     `json:"score"`
	MatchType string      `json:"match_type"` // exact, keyword, semantic
}

// MemoryStats contains statistics about an agent's memory store.
type MemoryStats struct {
	AgentID      string         `json:"agent_id"`
	TotalEntries int            `json:"total_entries"`
	Categories   map[string]int `json:"categories"`
	OldestEntry  time.Time      `json:"oldest_entry"`
	NewestEntry  time.Time      `json:"newest_entry"`
	Expiringsoon int            `json:"expiring_soon"`
}

// Engine manages agent memory storage and retrieval.
type Engine struct {
	mu      sync.RWMutex
	entries map[string]map[string]*MemoryEntry // agentID -> key -> entry
	logger  *slog.Logger

	// store optionally persists every entry across restarts. Composite
	// key is agentID + "\x00" + entryKey.
	store *engine.Store[MemoryEntry]
}

// NewEngine creates a new in-memory agent memory engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		entries: make(map[string]map[string]*MemoryEntry),
		logger:  logger,
	}
}

// NewEngineWithBackend creates an agent memory engine backed by be.
// Previously-stored entries are loaded eagerly during construction;
// every subsequent Store/Delete propagates to the backend so memory
// survives restarts.
//
// Expired entries are dropped on load (they would be filtered on read
// anyway, but cleaning them up keeps the backend tidy).
//
// When be is nil this is equivalent to [NewEngine].
func NewEngineWithBackend(ctx context.Context, logger *slog.Logger, be backend.Backend) (*Engine, error) {
	e := NewEngine(logger)
	if be == nil {
		return e, nil
	}
	e.store = engine.NewStore[MemoryEntry](be, "agentmemory.entries")
	if err := e.load(ctx); err != nil {
		return nil, fmt.Errorf("agentmemory: load persisted state: %w", err)
	}
	return e, nil
}

func memoryKey(agentID, key string) string { return agentID + "\x00" + key }

func splitMemoryKey(s string) (agentID, key string, ok bool) {
	i := strings.IndexByte(s, '\x00')
	if i < 0 {
		return "", "", false
	}
	return s[:i], s[i+1:], true
}

func (e *Engine) load(ctx context.Context) error {
	all, err := e.store.List(ctx, "")
	if err != nil {
		return err
	}

	now := time.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	for composite, entry := range all {
		agentID, key, ok := splitMemoryKey(composite)
		if !ok {
			e.logger.Warn("agentmemory: skipping malformed persisted key", "key", composite)
			continue
		}
		if entry.ExpiresAt != nil && entry.ExpiresAt.Before(now) {
			// Garbage-collect expired entries opportunistically.
			if err := e.store.Delete(ctx, composite); err != nil {
				e.logger.Warn("agentmemory: GC expired entry failed", "key", composite, "error", err)
			}
			continue
		}
		copy := entry
		if _, ok := e.entries[agentID]; !ok {
			e.entries[agentID] = make(map[string]*MemoryEntry)
		}
		e.entries[agentID][key] = &copy
	}
	e.logger.Info("agentmemory: restored from persistence", "agents", len(e.entries))
	return nil
}

func (e *Engine) persistLocked(entry *MemoryEntry) {
	if e.store == nil {
		return
	}
	if err := e.store.Set(context.Background(), memoryKey(entry.AgentID, entry.Key), *entry); err != nil {
		e.logger.Warn("agentmemory: persist failed", "agent", entry.AgentID, "key", entry.Key, "error", err)
	}
}

func (e *Engine) deletePersistedLocked(agentID, key string) {
	if e.store == nil {
		return
	}
	if err := e.store.Delete(context.Background(), memoryKey(agentID, key)); err != nil {
		e.logger.Warn("agentmemory: delete persisted failed", "agent", agentID, "key", key, "error", err)
	}
}

// Store creates or updates a memory entry.
func (e *Engine) Store(agentID, key string, value any, category string, tags []string, ttlSec int64, metadata map[string]string) (*MemoryEntry, error) {
	if agentID == "" || key == "" {
		return nil, fmt.Errorf("agent_id and key are required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.entries[agentID]; !ok {
		e.entries[agentID] = make(map[string]*MemoryEntry)
	}

	now := time.Now()
	entry := &MemoryEntry{
		Key:       key,
		Value:     value,
		Category:  category,
		Tags:      tags,
		AgentID:   agentID,
		UpdatedAt: now,
		Metadata:  metadata,
	}

	if existing, ok := e.entries[agentID][key]; ok {
		entry.CreatedAt = existing.CreatedAt
		entry.AccessCount = existing.AccessCount
	} else {
		entry.CreatedAt = now
	}

	if ttlSec > 0 {
		exp := now.Add(time.Duration(ttlSec) * time.Second)
		entry.ExpiresAt = &exp
	}

	e.entries[agentID][key] = entry
	e.persistLocked(entry)
	e.logger.Debug("memory stored", "agent", agentID, "key", key, "category", category)
	return entry, nil
}

// Recall retrieves a memory entry by key.
func (e *Engine) Recall(agentID, key string) (*MemoryEntry, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	agentEntries, ok := e.entries[agentID]
	if !ok {
		return nil, fmt.Errorf("no memory found for agent %q", agentID)
	}

	entry, ok := agentEntries[key]
	if !ok {
		return nil, fmt.Errorf("key %q not found for agent %q", key, agentID)
	}

	// Check expiration
	if entry.ExpiresAt != nil && entry.ExpiresAt.Before(time.Now()) {
		delete(agentEntries, key)
		e.deletePersistedLocked(agentID, key)
		return nil, fmt.Errorf("key %q has expired", key)
	}

	entry.AccessCount++
	e.persistLocked(entry)
	return entry, nil
}

// Delete removes a memory entry.
func (e *Engine) Delete(agentID, key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	agentEntries, ok := e.entries[agentID]
	if !ok {
		return fmt.Errorf("no memory found for agent %q", agentID)
	}

	if _, ok := agentEntries[key]; !ok {
		return fmt.Errorf("key %q not found for agent %q", key, agentID)
	}

	delete(agentEntries, key)
	e.deletePersistedLocked(agentID, key)
	return nil
}

// Search performs TF-IDF based search across agent memory.
func (e *Engine) Search(agentID, query, category string, limit int) []SearchResult {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if limit <= 0 {
		limit = 10
	}

	agentEntries, ok := e.entries[agentID]
	if !ok {
		return nil
	}

	queryTerms := tokenize(query)
	now := time.Now()

	// Build document corpus for TF-IDF
	var docs []docEntry
	for _, entry := range agentEntries {
		// Skip expired entries
		if entry.ExpiresAt != nil && entry.ExpiresAt.Before(now) {
			continue
		}
		if category != "" && entry.Category != category {
			continue
		}
		text := extractText(entry)
		docs = append(docs, docEntry{entry: entry, text: text, tokens: tokenize(text)})
	}

	if len(docs) == 0 {
		return nil
	}

	// Calculate IDF for query terms
	idf := make(map[string]float64)
	for _, term := range queryTerms {
		docCount := 0
		for _, doc := range docs {
			for _, t := range doc.tokens {
				if t == term {
					docCount++
					break
				}
			}
		}
		if docCount > 0 {
			idf[term] = math.Log(float64(len(docs)) / float64(docCount))
		}
	}

	// Score each document
	var results []SearchResult
	for _, doc := range docs {
		score := 0.0
		tf := make(map[string]int)
		for _, t := range doc.tokens {
			tf[t]++
		}

		for _, term := range queryTerms {
			if count, ok := tf[term]; ok {
				termFreq := float64(count) / float64(len(doc.tokens))
				score += termFreq * idf[term]
			}
		}

		if score > 0 {
			results = append(results, SearchResult{
				Entry:     *doc.entry,
				Score:     math.Round(score*1000) / 1000,
				MatchType: "semantic",
			})
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	if len(results) > limit {
		results = results[:limit]
	}
	return results
}

// ListEntries returns all entries for an agent, optionally filtered by category.
func (e *Engine) ListEntries(agentID, category string) []MemoryEntry {
	e.mu.RLock()
	defer e.mu.RUnlock()

	agentEntries, ok := e.entries[agentID]
	if !ok {
		return nil
	}

	now := time.Now()
	var result []MemoryEntry
	for _, entry := range agentEntries {
		if entry.ExpiresAt != nil && entry.ExpiresAt.Before(now) {
			continue
		}
		if category != "" && entry.Category != category {
			continue
		}
		result = append(result, *entry)
	}
	return result
}

// Stats returns memory statistics for an agent.
func (e *Engine) Stats(agentID string) *MemoryStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	stats := &MemoryStats{
		AgentID:    agentID,
		Categories: make(map[string]int),
	}

	agentEntries, ok := e.entries[agentID]
	if !ok {
		return stats
	}

	now := time.Now()
	soonThreshold := now.Add(1 * time.Hour)

	for _, entry := range agentEntries {
		if entry.ExpiresAt != nil && entry.ExpiresAt.Before(now) {
			continue
		}
		stats.TotalEntries++
		cat := entry.Category
		if cat == "" {
			cat = "uncategorized"
		}
		stats.Categories[cat]++

		if stats.OldestEntry.IsZero() || entry.CreatedAt.Before(stats.OldestEntry) {
			stats.OldestEntry = entry.CreatedAt
		}
		if entry.UpdatedAt.After(stats.NewestEntry) {
			stats.NewestEntry = entry.UpdatedAt
		}
		if entry.ExpiresAt != nil && entry.ExpiresAt.Before(soonThreshold) {
			stats.Expiringsoon++
		}
	}
	return stats
}

// CleanExpired removes expired entries across all agents.
func (e *Engine) CleanExpired() int {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()
	cleaned := 0
	for agentID, agentEntries := range e.entries {
		for key, entry := range agentEntries {
			if entry.ExpiresAt != nil && entry.ExpiresAt.Before(now) {
				delete(agentEntries, key)
				e.deletePersistedLocked(agentID, key)
				cleaned++
			}
		}
	}
	return cleaned
}

type docEntry struct {
	entry  *MemoryEntry
	text   string
	tokens []string
}

func extractText(entry *MemoryEntry) string {
	parts := []string{entry.Key}
	if entry.Category != "" {
		parts = append(parts, entry.Category)
	}
	parts = append(parts, entry.Tags...)
	if s, ok := entry.Value.(string); ok {
		parts = append(parts, s)
	}
	for _, v := range entry.Metadata {
		parts = append(parts, v)
	}
	return strings.Join(parts, " ")
}

func tokenize(text string) []string {
	text = strings.ToLower(text)
	fields := strings.FieldsFunc(text, func(r rune) bool {
		return !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-')
	})
	// Deduplicate
	seen := make(map[string]bool)
	var result []string
	for _, f := range fields {
		if len(f) < 2 || seen[f] {
			continue
		}
		seen[f] = true
		result = append(result, f)
	}
	return result
}
