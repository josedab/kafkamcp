package cdc

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// OperationType represents the type of CDC operation.
type OperationType string

const (
	OpInsert OperationType = "INSERT"
	OpUpdate OperationType = "UPDATE"
	OpDelete OperationType = "DELETE"
)

// SinkType represents the target data store type.
type SinkType string

const (
	SinkPostgres    SinkType = "postgres"
	SinkMySQL       SinkType = "mysql"
	SinkMongoDB     SinkType = "mongodb"
	SinkElastic     SinkType = "elasticsearch"
	SinkS3          SinkType = "s3"
	SinkBigQuery    SinkType = "bigquery"
	SinkSnowflake   SinkType = "snowflake"
	SinkRedis       SinkType = "redis"
	SinkCustomHTTP  SinkType = "http"
)

// Bridge defines a CDC bridge configuration.
type Bridge struct {
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	SourceTopic string         `json:"source_topic"`
	SourceCluster string       `json:"source_cluster,omitempty"`
	Sink        SinkConfig     `json:"sink"`
	Transform   *TransformSpec `json:"transform,omitempty"`
	Status      string         `json:"status"` // active, paused, stopped, error
	CreatedBy   string         `json:"created_by"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	Stats       BridgeStats    `json:"stats"`
}

// SinkConfig configures the target data store.
type SinkConfig struct {
	Type           SinkType          `json:"type"`
	ConnectionURL  string            `json:"connection_url,omitempty"`
	Table          string            `json:"table,omitempty"`
	Database       string            `json:"database,omitempty"`
	Collection     string            `json:"collection,omitempty"`
	Index          string            `json:"index,omitempty"`
	Bucket         string            `json:"bucket,omitempty"`
	KeyField       string            `json:"key_field,omitempty"`
	BatchSize      int               `json:"batch_size,omitempty"`
	FlushInterval  string            `json:"flush_interval,omitempty"`
	Properties     map[string]string `json:"properties,omitempty"`
}

// TransformSpec defines how messages are transformed before writing to the sink.
type TransformSpec struct {
	Format       string            `json:"format,omitempty"`        // json, avro, protobuf
	FieldMapping map[string]string `json:"field_mapping,omitempty"` // source_field -> sink_field
	FilterExpr   string            `json:"filter_expr,omitempty"`   // simple filter expression
	FlattenJSON  bool              `json:"flatten_json,omitempty"`
	IncludeMetadata bool           `json:"include_metadata,omitempty"`
}

// BridgeStats tracks CDC bridge performance.
type BridgeStats struct {
	RecordsProcessed int64     `json:"records_processed"`
	RecordsFailed    int64     `json:"records_failed"`
	BytesProcessed   int64     `json:"bytes_processed"`
	LastProcessed    time.Time `json:"last_processed,omitempty"`
	LastError        string    `json:"last_error,omitempty"`
	AvgLatencyMs     float64   `json:"avg_latency_ms"`
	Inserts          int64     `json:"inserts"`
	Updates          int64     `json:"updates"`
	Deletes          int64     `json:"deletes"`
}

// CDCEvent represents a captured change data event.
type CDCEvent struct {
	Operation OperationType     `json:"operation"`
	Timestamp time.Time         `json:"timestamp"`
	Key       string            `json:"key"`
	Before    map[string]any    `json:"before,omitempty"`
	After     map[string]any    `json:"after,omitempty"`
	Source    CDCSource         `json:"source"`
	Headers  map[string]string  `json:"headers,omitempty"`
}

// CDCSource identifies where a CDC event originated.
type CDCSource struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Cluster   string `json:"cluster,omitempty"`
}

// Engine manages CDC bridges.
type Engine struct {
	mu      sync.RWMutex
	bridges map[string]*Bridge
	events  map[string][]CDCEvent // bridgeID -> recent events
	logger  *slog.Logger
	nextID  int64
}

// NewEngine creates a new CDC engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		bridges: make(map[string]*Bridge),
		events:  make(map[string][]CDCEvent),
		logger:  logger,
	}
}

// CreateBridge creates a new CDC bridge configuration.
func (e *Engine) CreateBridge(name, sourceTopic, createdBy string, sink SinkConfig, transform *TransformSpec) (*Bridge, error) {
	if name == "" {
		return nil, fmt.Errorf("bridge name is required")
	}
	if sourceTopic == "" {
		return nil, fmt.Errorf("source_topic is required")
	}
	if err := validateSinkConfig(sink); err != nil {
		return nil, fmt.Errorf("invalid sink config: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.nextID++
	id := fmt.Sprintf("cdc_%d", e.nextID)
	now := time.Now()

	bridge := &Bridge{
		ID:          id,
		Name:        name,
		SourceTopic: sourceTopic,
		Sink:        sink,
		Transform:   transform,
		Status:      "active",
		CreatedBy:   createdBy,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	e.bridges[id] = bridge
	e.events[id] = make([]CDCEvent, 0)
	e.logger.Info("CDC bridge created", "id", id, "name", name, "sink_type", sink.Type)
	return bridge, nil
}

// ProcessEvent processes a CDC event through a bridge.
func (e *Engine) ProcessEvent(bridgeID string, rawKey, rawValue string, partition int32, offset int64, topic string) (*CDCEvent, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	bridge, ok := e.bridges[bridgeID]
	if !ok {
		return nil, fmt.Errorf("bridge %q not found", bridgeID)
	}
	if bridge.Status != "active" {
		return nil, fmt.Errorf("bridge %q is %s", bridgeID, bridge.Status)
	}

	event := &CDCEvent{
		Key:       rawKey,
		Timestamp: time.Now(),
		Source: CDCSource{
			Topic:     topic,
			Partition: partition,
			Offset:    offset,
		},
	}

	// Parse value as JSON to determine operation type
	var valueMap map[string]any
	if rawValue != "" {
		if err := json.Unmarshal([]byte(rawValue), &valueMap); err != nil {
			// Non-JSON value: treat as INSERT
			event.Operation = OpInsert
			event.After = map[string]any{"value": rawValue}
		} else {
			event.After = valueMap
			// Detect operation from Debezium-style envelope
			if op, ok := valueMap["op"].(string); ok {
				switch op {
				case "c", "r":
					event.Operation = OpInsert
				case "u":
					event.Operation = OpUpdate
					if before, ok := valueMap["before"].(map[string]any); ok {
						event.Before = before
					}
				case "d":
					event.Operation = OpDelete
					event.Before = valueMap
					event.After = nil
				default:
					event.Operation = OpInsert
				}
			} else {
				event.Operation = OpInsert
			}
		}
	} else {
		// Null value = tombstone = DELETE
		event.Operation = OpDelete
	}

	// Apply transform if configured
	if bridge.Transform != nil && event.After != nil {
		event.After = applyTransform(bridge.Transform, event.After)
	}

	// Update stats
	bridge.Stats.RecordsProcessed++
	bridge.Stats.BytesProcessed += int64(len(rawValue))
	bridge.Stats.LastProcessed = time.Now()
	switch event.Operation {
	case OpInsert:
		bridge.Stats.Inserts++
	case OpUpdate:
		bridge.Stats.Updates++
	case OpDelete:
		bridge.Stats.Deletes++
	}
	bridge.UpdatedAt = time.Now()

	// Keep recent events (max 100)
	events := e.events[bridgeID]
	events = append(events, *event)
	if len(events) > 100 {
		events = events[len(events)-100:]
	}
	e.events[bridgeID] = events

	return event, nil
}

// GetBridge returns a bridge by ID.
func (e *Engine) GetBridge(id string) (*Bridge, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	bridge, ok := e.bridges[id]
	if !ok {
		return nil, fmt.Errorf("bridge %q not found", id)
	}
	return bridge, nil
}

// ListBridges returns all bridges.
func (e *Engine) ListBridges() []Bridge {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]Bridge, 0, len(e.bridges))
	for _, b := range e.bridges {
		result = append(result, *b)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.After(result[j].CreatedAt) })
	return result
}

// UpdateBridgeStatus updates the status of a bridge.
func (e *Engine) UpdateBridgeStatus(id, status string) error {
	validStatuses := map[string]bool{"active": true, "paused": true, "stopped": true, "error": true}
	if !validStatuses[status] {
		return fmt.Errorf("invalid status %q", status)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	bridge, ok := e.bridges[id]
	if !ok {
		return fmt.Errorf("bridge %q not found", id)
	}
	bridge.Status = status
	bridge.UpdatedAt = time.Now()
	return nil
}

// DeleteBridge removes a bridge.
func (e *Engine) DeleteBridge(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.bridges[id]; !ok {
		return fmt.Errorf("bridge %q not found", id)
	}
	delete(e.bridges, id)
	delete(e.events, id)
	return nil
}

// GetRecentEvents returns recent CDC events for a bridge.
func (e *Engine) GetRecentEvents(bridgeID string, limit int) ([]CDCEvent, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	events, ok := e.events[bridgeID]
	if !ok {
		return nil, fmt.Errorf("bridge %q not found", bridgeID)
	}
	if limit <= 0 || limit > len(events) {
		limit = len(events)
	}
	return events[len(events)-limit:], nil
}

func validateSinkConfig(sink SinkConfig) error {
	validTypes := map[SinkType]bool{
		SinkPostgres: true, SinkMySQL: true, SinkMongoDB: true,
		SinkElastic: true, SinkS3: true, SinkBigQuery: true,
		SinkSnowflake: true, SinkRedis: true, SinkCustomHTTP: true,
	}
	if !validTypes[sink.Type] {
		return fmt.Errorf("unsupported sink type %q", sink.Type)
	}
	if sink.BatchSize < 0 {
		return fmt.Errorf("batch_size must be non-negative")
	}
	return nil
}

func applyTransform(spec *TransformSpec, data map[string]any) map[string]any {
	result := make(map[string]any)

	if spec.FieldMapping != nil && len(spec.FieldMapping) > 0 {
		for srcField, dstField := range spec.FieldMapping {
			if val, ok := data[srcField]; ok {
				result[dstField] = val
			}
		}
	} else {
		for k, v := range data {
			result[k] = v
		}
	}

	if spec.FlattenJSON {
		result = flattenMap("", result)
	}

	if spec.IncludeMetadata {
		result["_cdc_processed_at"] = time.Now().UTC().Format(time.RFC3339)
	}

	return result
}

func flattenMap(prefix string, m map[string]any) map[string]any {
	result := make(map[string]any)
	for k, v := range m {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}
		if nested, ok := v.(map[string]any); ok {
			for nk, nv := range flattenMap(key, nested) {
				result[nk] = nv
			}
		} else {
			result[key] = v
		}
	}
	return result
}
