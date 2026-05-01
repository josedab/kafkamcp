package replay

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
	"github.com/kafkamcp/kafkamcp/internal/randid"
)

var bookmarkCounter atomic.Uint64
var simulationCounter atomic.Uint64

// Bookmark captures message state at a point in time.
type Bookmark struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Topic       string            `json:"topic"`
	Cluster     string            `json:"cluster"`
	Offsets     map[int32]int64   `json:"offsets"`
	Timestamp   string            `json:"timestamp"`
	MessageCount int              `json:"message_count"`
	SampleMessages []SampleMessage `json:"sample_messages,omitempty"`
	CreatedAt   string            `json:"created_at"`
	Tags        []string          `json:"tags,omitempty"`
}

// SampleMessage is a captured message for bookmark reference.
type SampleMessage struct {
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Key       string `json:"key,omitempty"`
	ValueHash string `json:"value_hash"`
	Size      int    `json:"size_bytes"`
	Timestamp string `json:"timestamp"`
}

// DiffResult compares messages between two time windows.
type DiffResult struct {
	BookmarkA     string        `json:"bookmark_a"`
	BookmarkB     string        `json:"bookmark_b"`
	Topic         string        `json:"topic"`
	TotalA        int           `json:"total_messages_a"`
	TotalB        int           `json:"total_messages_b"`
	Added         int           `json:"messages_added"`
	Removed       int           `json:"messages_removed"`
	Modified      int           `json:"messages_modified"`
	SchemaChanged bool          `json:"schema_changed"`
	FieldChanges  []FieldChange `json:"field_changes,omitempty"`
	GeneratedAt   string        `json:"generated_at"`
}

// FieldChange tracks a schema/content change between windows.
type FieldChange struct {
	Field     string `json:"field"`
	ChangeType string `json:"change_type"` // added, removed, type_changed, value_distribution_changed
	OldValue  string `json:"old_value,omitempty"`
	NewValue  string `json:"new_value,omitempty"`
}

// Simulation represents a consumer group replay simulation.
type Simulation struct {
	ID              string             `json:"id"`
	Name            string             `json:"name"`
	Topic           string             `json:"topic"`
	GroupID         string             `json:"group_id"`
	FromBookmark    string             `json:"from_bookmark,omitempty"`
	StartOffset     map[int32]int64    `json:"start_offsets"`
	EndOffset       map[int32]int64    `json:"end_offsets,omitempty"`
	Status          string             `json:"status"` // pending, running, completed, failed
	MessagesReplayed int               `json:"messages_replayed"`
	ErrorCount      int                `json:"error_count"`
	Predictions     []ReplayPrediction `json:"predictions,omitempty"`
	StartedAt       string             `json:"started_at,omitempty"`
	CompletedAt     string             `json:"completed_at,omitempty"`
	CreatedAt       string             `json:"created_at"`
}

// ReplayPrediction estimates what would happen if messages were replayed.
type ReplayPrediction struct {
	Metric      string  `json:"metric"`
	Description string  `json:"description"`
	Impact      string  `json:"impact"` // low, medium, high
	Value       float64 `json:"value,omitempty"`
}

// Engine manages replay sessions and bookmarks.
type Engine struct {
	bookmarks   map[string]*Bookmark
	simulations map[string]*Simulation
	logger      *slog.Logger
	mu          sync.RWMutex
}

// NewEngine creates a new replay engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		bookmarks:   make(map[string]*Bookmark),
		simulations: make(map[string]*Simulation),
		logger:      logger,
	}
}

// CreateBookmark saves a point-in-time message state.
func (e *Engine) CreateBookmark(name, topic, cluster string, offsets map[int32]int64, samples []SampleMessage, tags []string) *Bookmark {
	e.mu.Lock()
	defer e.mu.Unlock()

	bookmark := &Bookmark{
		ID:             randid.New("bm"),
		Name:           name,
		Topic:          topic,
		Cluster:        cluster,
		Offsets:        offsets,
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
		SampleMessages: samples,
		Tags:           tags,
		CreatedAt:      time.Now().UTC().Format(time.RFC3339),
	}

	// Calculate total message count from offsets
	var total int64
	for _, offset := range offsets {
		total += offset
	}
	bookmark.MessageCount = int(total)

	e.bookmarks[bookmark.ID] = bookmark
	return bookmark
}

// GetBookmark returns a bookmark by ID.
func (e *Engine) GetBookmark(id string) (*Bookmark, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	bm, ok := e.bookmarks[id]
	if !ok {
		return nil, fmt.Errorf("bookmark %q not found", id)
	}
	return bm, nil
}

// ListBookmarks returns all bookmarks for a topic.
func (e *Engine) ListBookmarks(topic string) []Bookmark {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []Bookmark
	for _, bm := range e.bookmarks {
		if topic == "" || bm.Topic == topic {
			result = append(result, *bm)
		}
	}
	return result
}

// DeleteBookmark removes a bookmark.
func (e *Engine) DeleteBookmark(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.bookmarks[id]; !ok {
		return fmt.Errorf("bookmark %q not found", id)
	}
	delete(e.bookmarks, id)
	return nil
}

// Diff compares two bookmarks and identifies changes.
func (e *Engine) Diff(bookmarkAID, bookmarkBID string) (*DiffResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	a, ok := e.bookmarks[bookmarkAID]
	if !ok {
		return nil, fmt.Errorf("bookmark %q not found", bookmarkAID)
	}
	b, ok := e.bookmarks[bookmarkBID]
	if !ok {
		return nil, fmt.Errorf("bookmark %q not found", bookmarkBID)
	}

	if a.Topic != b.Topic {
		return nil, fmt.Errorf("bookmarks must reference the same topic (got %q and %q)", a.Topic, b.Topic)
	}

	diff := &DiffResult{
		BookmarkA:   bookmarkAID,
		BookmarkB:   bookmarkBID,
		Topic:       a.Topic,
		TotalA:      a.MessageCount,
		TotalB:      b.MessageCount,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
	}

	// Compare offsets to estimate added/removed
	for partition, offsetA := range a.Offsets {
		if offsetB, ok := b.Offsets[partition]; ok {
			if offsetB > offsetA {
				diff.Added += int(offsetB - offsetA)
			} else if offsetA > offsetB {
				diff.Removed += int(offsetA - offsetB)
			}
		}
	}

	// Check for new partitions in B
	for partition := range b.Offsets {
		if _, ok := a.Offsets[partition]; !ok {
			diff.Added += int(b.Offsets[partition])
		}
	}

	// Compare sample messages for schema/content changes
	sampleA := make(map[string]SampleMessage)
	for _, s := range a.SampleMessages {
		sampleA[fmt.Sprintf("%d:%d", s.Partition, s.Offset)] = s
	}
	for _, sb := range b.SampleMessages {
		key := fmt.Sprintf("%d:%d", sb.Partition, sb.Offset)
		if sa, ok := sampleA[key]; ok {
			if sa.ValueHash != sb.ValueHash {
				diff.Modified++
			}
			if sa.Size != sb.Size {
				diff.FieldChanges = append(diff.FieldChanges, FieldChange{
					Field:      "message_size",
					ChangeType: "value_distribution_changed",
					OldValue:   fmt.Sprintf("%d bytes", sa.Size),
					NewValue:   fmt.Sprintf("%d bytes", sb.Size),
				})
			}
		}
	}

	return diff, nil
}

// CreateSimulation creates a consumer group replay simulation.
func (e *Engine) CreateSimulation(name, topic, groupID string, startOffsets map[int32]int64, fromBookmark string) *Simulation {
	e.mu.Lock()
	defer e.mu.Unlock()

	sim := &Simulation{
		ID:          randid.New("sim"),
		Name:        name,
		Topic:       topic,
		GroupID:     groupID,
		FromBookmark: fromBookmark,
		StartOffset: startOffsets,
		Status:      "pending",
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
	}

	// Generate predictions
	var totalMessages int64
	for _, offset := range startOffsets {
		totalMessages += offset
	}

	sim.Predictions = []ReplayPrediction{
		{
			Metric:      "estimated_messages",
			Description: "Estimated messages to replay",
			Impact:      estimateImpact(totalMessages),
			Value:       float64(totalMessages),
		},
		{
			Metric:      "consumer_lag_impact",
			Description: "Expected increase in consumer lag during replay",
			Impact:      "medium",
		},
		{
			Metric:      "processing_time",
			Description: "Estimated replay duration based on message count",
			Impact:      estimateImpact(totalMessages),
		},
	}

	e.simulations[sim.ID] = sim
	return sim
}

// RunSimulation marks a simulation as running.
func (e *Engine) RunSimulation(id string) (*Simulation, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	sim, ok := e.simulations[id]
	if !ok {
		return nil, fmt.Errorf("simulation %q not found", id)
	}
	if sim.Status != "pending" {
		return nil, fmt.Errorf("simulation %q is already %s", id, sim.Status)
	}

	sim.Status = "running"
	sim.StartedAt = time.Now().UTC().Format(time.RFC3339)

	// Simulate completion
	var total int64
	for _, offset := range sim.StartOffset {
		total += offset
	}
	sim.MessagesReplayed = int(total)
	sim.Status = "completed"
	sim.CompletedAt = time.Now().UTC().Format(time.RFC3339)

	return sim, nil
}

// GetSimulation returns a simulation by ID.
func (e *Engine) GetSimulation(id string) (*Simulation, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	sim, ok := e.simulations[id]
	if !ok {
		return nil, fmt.Errorf("simulation %q not found", id)
	}
	return sim, nil
}

// ListSimulations returns all simulations.
func (e *Engine) ListSimulations(topic string) []Simulation {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []Simulation
	for _, sim := range e.simulations {
		if topic == "" || sim.Topic == topic {
			result = append(result, *sim)
		}
	}
	return result
}

func estimateImpact(messageCount int64) string {
	if messageCount > 1000000 {
		return "high"
	}
	if messageCount > 10000 {
		return "medium"
	}
	return "low"
}
