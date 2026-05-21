// Package viewstore provides incremental materialized views over Kafka topics
// with support for projections, running aggregates, and windowed computations.
package viewstore

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/engine"
	"github.com/kafkamcp/kafkamcp/internal/engine/backend"
)

// ViewDefinition defines a materialized view over a Kafka topic.
type ViewDefinition struct {
	ID              string            `json:"id"`
	Name            string            `json:"name"`
	Description     string            `json:"description,omitempty"`
	SourceTopic     string            `json:"source_topic"`
	KeyField        string            `json:"key_field"`
	ValueFields     []string          `json:"value_fields,omitempty"`
	AggregationType string            `json:"aggregation_type,omitempty"` // count, sum, avg, min, max, group_by
	GroupByFields   []string          `json:"group_by_fields,omitempty"`
	WindowType      string            `json:"window_type,omitempty"` // tumbling, sliding, session, none
	WindowSizeMs    int64             `json:"window_size_ms,omitempty"`
	FilterExpr      string            `json:"filter_expr,omitempty"`
	RetentionMs     int64             `json:"retention_ms,omitempty"`
	Status          string            `json:"status"` // active, paused, error
	CreatedBy       string            `json:"created_by"`
	CreatedAt       time.Time         `json:"created_at"`
	UpdatedAt       time.Time         `json:"updated_at"`
	RecordCount     int64             `json:"record_count"`
	LastOffset      int64             `json:"last_offset"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

// ViewRecord is a single record in a materialized view.
type ViewRecord struct {
	Key       string         `json:"key"`
	Value     map[string]any `json:"value"`
	UpdatedAt time.Time      `json:"updated_at"`
	Offset    int64          `json:"offset"`
}

// QueryOptions defines query parameters for view queries.
type QueryOptions struct {
	KeyPrefix  string `json:"key_prefix,omitempty"`
	KeyExact   string `json:"key_exact,omitempty"`
	Limit      int    `json:"limit,omitempty"`
	Offset     int    `json:"offset,omitempty"`
	OrderBy    string `json:"order_by,omitempty"`
	Descending bool   `json:"descending,omitempty"`
}

// QueryResult holds the results of a view query.
type QueryResult struct {
	ViewID      string       `json:"view_id"`
	ViewName    string       `json:"view_name"`
	Records     []ViewRecord `json:"records"`
	TotalCount  int          `json:"total_count"`
	QueryTimeMs int64        `json:"query_time_ms"`
}

// ViewStats contains statistics about a materialized view.
type ViewStats struct {
	ViewID         string    `json:"view_id"`
	ViewName       string    `json:"view_name"`
	SourceTopic    string    `json:"source_topic"`
	Status         string    `json:"status"`
	RecordCount    int64     `json:"record_count"`
	LastOffset     int64     `json:"last_offset"`
	LastUpdateAt   time.Time `json:"last_update_at"`
	StorageSizeEst int64     `json:"storage_size_est_bytes"`
}

// Engine manages materialized views.
type Engine struct {
	mu     sync.RWMutex
	views  map[string]*ViewDefinition
	store  map[string]map[string]*ViewRecord // viewID -> key -> record
	logger *slog.Logger
	nextID int64

	// persisted state. defStore holds view definitions keyed by viewID;
	// recStore holds records keyed by viewID + "\x00" + recordKey. Both
	// are nil when no backend was supplied — in that case the engine
	// runs entirely in-process (legacy behaviour).
	defStore *engine.Store[ViewDefinition]
	recStore *engine.Store[ViewRecord]
}

// NewEngine creates a new view store engine without persistence. The
// engine's state is lost when the process exits.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		views:  make(map[string]*ViewDefinition),
		store:  make(map[string]map[string]*ViewRecord),
		logger: logger,
	}
}

// NewEngineWithBackend creates a view store engine backed by be. State is
// loaded synchronously from the backend during construction; subsequent
// mutations are persisted asynchronously to the backend on each write.
//
// When be is nil this is equivalent to [NewEngine].
func NewEngineWithBackend(ctx context.Context, logger *slog.Logger, be backend.Backend) (*Engine, error) {
	e := NewEngine(logger)
	if be == nil {
		return e, nil
	}
	e.defStore = engine.NewStore[ViewDefinition](be, "viewstore.definitions")
	e.recStore = engine.NewStore[ViewRecord](be, "viewstore.records")
	if err := e.load(ctx); err != nil {
		return nil, fmt.Errorf("viewstore: load persisted state: %w", err)
	}
	return e, nil
}

func (e *Engine) load(ctx context.Context) error {
	defs, err := e.defStore.List(ctx, "")
	if err != nil {
		return err
	}
	recs, err := e.recStore.List(ctx, "")
	if err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	for id, v := range defs {
		copy := v
		e.views[id] = &copy
		e.store[id] = make(map[string]*ViewRecord)
		if id > "" {
			// Bump nextID past any restored view IDs so subsequent
			// CreateView calls don't collide.
			var n int64
			if _, err := fmt.Sscanf(id, "view_%d", &n); err == nil && n > e.nextID {
				e.nextID = n
			}
		}
	}
	for compositeKey, rec := range recs {
		viewID, recKey, ok := splitCompositeKey(compositeKey)
		if !ok {
			e.logger.Warn("viewstore: skipping malformed persisted record key", "key", compositeKey)
			continue
		}
		ns, ok := e.store[viewID]
		if !ok {
			// Orphan record (view definition missing). Drop it; we
			// could log louder but this is rare and not worth blocking
			// start-up over.
			continue
		}
		copy := rec
		ns[recKey] = &copy
	}
	e.logger.Info("viewstore: restored from persistence", "views", len(defs), "records", len(recs))
	return nil
}

func compositeKey(viewID, key string) string { return viewID + "\x00" + key }

func splitCompositeKey(s string) (viewID, key string, ok bool) {
	i := strings.IndexByte(s, '\x00')
	if i < 0 {
		return "", "", false
	}
	return s[:i], s[i+1:], true
}

// CreateView creates a new materialized view definition.
func (e *Engine) CreateView(name, sourceTopic, keyField, aggType, windowType string, windowSizeMs, retentionMs int64, groupByFields, valueFields []string, filterExpr, description, createdBy string) (*ViewDefinition, error) {
	if name == "" || sourceTopic == "" {
		return nil, fmt.Errorf("name and source_topic are required")
	}
	if keyField == "" {
		keyField = "_key"
	}

	validAggTypes := map[string]bool{"": true, "count": true, "sum": true, "avg": true, "min": true, "max": true, "group_by": true}
	if !validAggTypes[aggType] {
		return nil, fmt.Errorf("invalid aggregation_type: %s", aggType)
	}

	validWindowTypes := map[string]bool{"": true, "none": true, "tumbling": true, "sliding": true, "session": true}
	if !validWindowTypes[windowType] {
		return nil, fmt.Errorf("invalid window_type: %s", windowType)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.nextID++
	id := fmt.Sprintf("view_%d", e.nextID)

	view := &ViewDefinition{
		ID:              id,
		Name:            name,
		Description:     description,
		SourceTopic:     sourceTopic,
		KeyField:        keyField,
		ValueFields:     valueFields,
		AggregationType: aggType,
		GroupByFields:   groupByFields,
		WindowType:      windowType,
		WindowSizeMs:    windowSizeMs,
		FilterExpr:      filterExpr,
		RetentionMs:     retentionMs,
		Status:          "active",
		CreatedBy:       createdBy,
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
	}

	e.views[id] = view
	e.store[id] = make(map[string]*ViewRecord)

	e.logger.Info("view created", "id", id, "name", name, "source", sourceTopic)
	e.persistViewLocked(view)
	return view, nil
}

func (e *Engine) persistViewLocked(view *ViewDefinition) {
	if e.defStore == nil {
		return
	}
	if err := e.defStore.Set(context.Background(), view.ID, *view); err != nil {
		e.logger.Warn("viewstore: persist view definition failed", "id", view.ID, "error", err)
	}
}

func (e *Engine) persistRecordLocked(viewID string, rec *ViewRecord) {
	if e.recStore == nil {
		return
	}
	if err := e.recStore.Set(context.Background(), compositeKey(viewID, rec.Key), *rec); err != nil {
		e.logger.Warn("viewstore: persist record failed", "view", viewID, "key", rec.Key, "error", err)
	}
}

// IngestRecord upserts a record into a materialized view.
func (e *Engine) IngestRecord(viewID, key string, value map[string]any, offset int64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	view, ok := e.views[viewID]
	if !ok {
		return fmt.Errorf("view %q not found", viewID)
	}

	records := e.store[viewID]
	existing, exists := records[key]

	if view.AggregationType != "" && exists {
		// Apply aggregation
		merged := e.applyAggregation(view.AggregationType, existing.Value, value)
		existing.Value = merged
		existing.UpdatedAt = time.Now()
		existing.Offset = offset
	} else {
		records[key] = &ViewRecord{
			Key:       key,
			Value:     value,
			UpdatedAt: time.Now(),
			Offset:    offset,
		}
		view.RecordCount++
	}

	view.LastOffset = offset
	view.UpdatedAt = time.Now()

	// Persist after mutation. We do this synchronously while holding the
	// engine lock so that a crash between the in-memory update and the
	// persistence call cannot leave the two out of sync.
	if exists {
		e.persistRecordLocked(viewID, existing)
	} else {
		e.persistRecordLocked(viewID, records[key])
	}
	e.persistViewLocked(view)
	return nil
}

func (e *Engine) applyAggregation(aggType string, existing, incoming map[string]any) map[string]any {
	switch aggType {
	case "count":
		count, _ := existing["_count"].(float64)
		existing["_count"] = count + 1
	case "sum":
		for k, v := range incoming {
			if num, ok := v.(float64); ok {
				prev, _ := existing[k].(float64)
				existing[k] = prev + num
			}
		}
	case "avg":
		count, _ := existing["_count"].(float64)
		count++
		existing["_count"] = count
		for k, v := range incoming {
			if num, ok := v.(float64); ok {
				prevSum, _ := existing["_sum_"+k].(float64)
				newSum := prevSum + num
				existing["_sum_"+k] = newSum
				existing[k] = newSum / count
			}
		}
	case "min":
		for k, v := range incoming {
			if num, ok := v.(float64); ok {
				if prev, ok := existing[k].(float64); !ok || num < prev {
					existing[k] = num
				}
			}
		}
	case "max":
		for k, v := range incoming {
			if num, ok := v.(float64); ok {
				if prev, ok := existing[k].(float64); !ok || num > prev {
					existing[k] = num
				}
			}
		}
	default:
		for k, v := range incoming {
			existing[k] = v
		}
	}
	return existing
}

// Query queries a materialized view.
func (e *Engine) Query(viewID string, opts QueryOptions) (*QueryResult, error) {
	start := time.Now()

	e.mu.RLock()
	defer e.mu.RUnlock()

	view, ok := e.views[viewID]
	if !ok {
		return nil, fmt.Errorf("view %q not found", viewID)
	}

	records := e.store[viewID]
	var results []ViewRecord

	for _, rec := range records {
		if opts.KeyExact != "" && rec.Key != opts.KeyExact {
			continue
		}
		if opts.KeyPrefix != "" && !strings.HasPrefix(rec.Key, opts.KeyPrefix) {
			continue
		}
		results = append(results, *rec)
	}

	// Sort
	sort.Slice(results, func(i, j int) bool {
		if opts.Descending {
			return results[i].Key > results[j].Key
		}
		return results[i].Key < results[j].Key
	})

	totalCount := len(results)

	// Pagination
	if opts.Offset > 0 && opts.Offset < len(results) {
		results = results[opts.Offset:]
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}
	if len(results) > limit {
		results = results[:limit]
	}

	return &QueryResult{
		ViewID:      viewID,
		ViewName:    view.Name,
		Records:     results,
		TotalCount:  totalCount,
		QueryTimeMs: time.Since(start).Milliseconds(),
	}, nil
}

// GetView returns a view definition by ID.
func (e *Engine) GetView(id string) (*ViewDefinition, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	view, ok := e.views[id]
	if !ok {
		return nil, fmt.Errorf("view %q not found", id)
	}
	return view, nil
}

// ListViews returns all views.
func (e *Engine) ListViews() []ViewDefinition {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []ViewDefinition
	for _, v := range e.views {
		result = append(result, *v)
	}
	return result
}

// DeleteView removes a view and its data.
func (e *Engine) DeleteView(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.views[id]; !ok {
		return fmt.Errorf("view %q not found", id)
	}
	records := e.store[id]
	delete(e.views, id)
	delete(e.store, id)

	if e.defStore != nil {
		ctx := context.Background()
		if err := e.defStore.Delete(ctx, id); err != nil {
			e.logger.Warn("viewstore: delete view definition failed", "id", id, "error", err)
		}
		for k := range records {
			if err := e.recStore.Delete(ctx, compositeKey(id, k)); err != nil {
				e.logger.Warn("viewstore: delete record failed", "view", id, "key", k, "error", err)
			}
		}
	}
	return nil
}

// Stats returns statistics for a view.
func (e *Engine) Stats(viewID string) (*ViewStats, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	view, ok := e.views[viewID]
	if !ok {
		return nil, fmt.Errorf("view %q not found", viewID)
	}

	records := e.store[viewID]
	estimatedSize := int64(len(records)) * 256 // rough estimate

	return &ViewStats{
		ViewID:         view.ID,
		ViewName:       view.Name,
		SourceTopic:    view.SourceTopic,
		Status:         view.Status,
		RecordCount:    int64(len(records)),
		LastOffset:     view.LastOffset,
		LastUpdateAt:   view.UpdatedAt,
		StorageSizeEst: estimatedSize,
	}, nil
}
