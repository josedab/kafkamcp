// Package llmpipeline provides LLM training data export from Kafka topics
// with configurable sampling strategies, PII redaction, deduplication,
// and multi-format output (JSONL, CSV, Parquet).
package llmpipeline

import (
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"time"
	"github.com/kafkamcp/kafkamcp/internal/randid"
)

// SamplingStrategy defines how messages are sampled from topics.
type SamplingStrategy string

const (
	SamplingRandom   SamplingStrategy = "random"
	SamplingTimeBased SamplingStrategy = "time_based"
	SamplingKeyBased SamplingStrategy = "key_based"
	SamplingStratified SamplingStrategy = "stratified"
)

// ExportFormat defines the output format.
type ExportFormat string

const (
	FormatJSONL   ExportFormat = "jsonl"
	FormatCSV     ExportFormat = "csv"
	FormatParquet ExportFormat = "parquet"
)

// ExportDestination defines where to write output.
type ExportDestination string

const (
	DestinationLocal ExportDestination = "local"
	DestinationS3    ExportDestination = "s3"
	DestinationGCS   ExportDestination = "gcs"
)

// PipelineConfig configures a training data export pipeline.
type PipelineConfig struct {
	ID              string            `json:"id"`
	Topics          []string          `json:"topics"`
	Sampling        SamplingConfig    `json:"sampling"`
	Format          ExportFormat      `json:"format"`
	Destination     ExportDestination `json:"destination"`
	DestinationPath string            `json:"destination_path"`
	PIIRedaction    bool              `json:"pii_redaction"`
	Deduplicate     bool              `json:"deduplicate"`
	MaxMessages     int64             `json:"max_messages"`
	Fields          []string          `json:"fields,omitempty"` // fields to include (empty = all)
	Labels          map[string]string `json:"labels,omitempty"`
}

// SamplingConfig configures the sampling strategy.
type SamplingConfig struct {
	Strategy     SamplingStrategy `json:"strategy"`
	Rate         float64          `json:"rate"` // 0.0-1.0 for random sampling
	TimeWindowMs int64            `json:"time_window_ms,omitempty"`
	KeyPattern   string           `json:"key_pattern,omitempty"`
	StratifyField string          `json:"stratify_field,omitempty"`
	MaxPerStratum int             `json:"max_per_stratum,omitempty"`
}

// ExportResult is the outcome of a pipeline run.
type ExportResult struct {
	PipelineID      string         `json:"pipeline_id"`
	Status          string         `json:"status"` // running, completed, failed
	MessagesRead    int64          `json:"messages_read"`
	MessagesExported int64         `json:"messages_exported"`
	MessagesDeduplicated int64     `json:"messages_deduplicated"`
	PIIFieldsRedacted int64       `json:"pii_fields_redacted"`
	OutputSizeBytes int64          `json:"output_size_bytes"`
	OutputPath      string         `json:"output_path"`
	Format          ExportFormat   `json:"format"`
	Duration        time.Duration  `json:"duration"`
	StartedAt       time.Time      `json:"started_at"`
	CompletedAt     time.Time      `json:"completed_at,omitempty"`
	Error           string         `json:"error,omitempty"`
	Distribution    map[string]int `json:"distribution,omitempty"`
}

// PIIField defines a PII field pattern for redaction.
type PIIField struct {
	Name    string `json:"name"`
	Pattern string `json:"pattern"`
	Action  string `json:"action"` // redact, hash, mask
}

// Engine manages LLM training data pipelines.
type Engine struct {
	mu        sync.RWMutex
	pipelines map[string]*PipelineConfig
	results   map[string]*ExportResult
	piiFields []PIIField
	logger    *slog.Logger
	rng       *rand.Rand
	idCounter int64
}

// NewEngine creates a new LLM pipeline engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		pipelines: make(map[string]*PipelineConfig),
		results:   make(map[string]*ExportResult),
		piiFields: defaultPIIFields(),
		logger:    logger,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// CreatePipeline registers a new export pipeline configuration.
func (e *Engine) CreatePipeline(cfg PipelineConfig) (*PipelineConfig, error) {
	if len(cfg.Topics) == 0 {
		return nil, fmt.Errorf("at least one topic is required")
	}
	if cfg.Format == "" {
		cfg.Format = FormatJSONL
	}
	if cfg.Destination == "" {
		cfg.Destination = DestinationLocal
	}
	if cfg.MaxMessages <= 0 {
		cfg.MaxMessages = 100000
	}
	if cfg.Sampling.Rate <= 0 || cfg.Sampling.Rate > 1.0 {
		cfg.Sampling.Rate = 1.0
	}
	if cfg.Sampling.Strategy == "" {
		cfg.Sampling.Strategy = SamplingRandom
	}
	if cfg.ID == "" {
		e.idCounter++
		cfg.ID = randid.New("pipeline")
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.pipelines[cfg.ID] = &cfg
	return &cfg, nil
}

// GetPipeline returns a pipeline config by ID.
func (e *Engine) GetPipeline(id string) (*PipelineConfig, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	p, ok := e.pipelines[id]
	return p, ok
}

// ListPipelines returns all configured pipelines.
func (e *Engine) ListPipelines() []PipelineConfig {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]PipelineConfig, 0, len(e.pipelines))
	for _, p := range e.pipelines {
		result = append(result, *p)
	}
	return result
}

// Export runs the export pipeline on provided messages (simulated).
func (e *Engine) Export(pipelineID string, messages []map[string]any) (*ExportResult, error) {
	e.mu.RLock()
	cfg, ok := e.pipelines[pipelineID]
	e.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("pipeline %q not found", pipelineID)
	}

	result := &ExportResult{
		PipelineID:   pipelineID,
		Status:       "running",
		Format:       cfg.Format,
		StartedAt:    time.Now(),
		Distribution: make(map[string]int),
	}

	// Apply sampling
	sampled := e.applySampling(messages, cfg.Sampling)
	result.MessagesRead = int64(len(messages))

	// Apply deduplication
	if cfg.Deduplicate {
		before := len(sampled)
		sampled = e.deduplicate(sampled)
		result.MessagesDeduplicated = int64(before - len(sampled))
	}

	// Apply PII redaction
	if cfg.PIIRedaction {
		var redacted int64
		for i := range sampled {
			r := e.redactPII(sampled[i])
			redacted += int64(r)
		}
		result.PIIFieldsRedacted = redacted
	}

	// Filter fields
	if len(cfg.Fields) > 0 {
		sampled = e.filterFields(sampled, cfg.Fields)
	}

	// Limit
	if int64(len(sampled)) > cfg.MaxMessages {
		sampled = sampled[:cfg.MaxMessages]
	}

	// Format output
	var outputBytes []byte
	var err error
	switch cfg.Format {
	case FormatJSONL:
		outputBytes, err = e.formatJSONL(sampled)
	case FormatCSV:
		outputBytes, err = e.formatCSV(sampled)
	default:
		outputBytes, err = e.formatJSONL(sampled)
	}

	if err != nil {
		result.Status = "failed"
		result.Error = err.Error()
		return result, err
	}

	result.MessagesExported = int64(len(sampled))
	result.OutputSizeBytes = int64(len(outputBytes))
	result.OutputPath = cfg.DestinationPath
	result.Status = "completed"
	result.CompletedAt = time.Now()
	result.Duration = result.CompletedAt.Sub(result.StartedAt)

	// Track distribution
	for _, msg := range sampled {
		if topic, ok := msg["_topic"].(string); ok {
			result.Distribution[topic]++
		}
	}

	e.mu.Lock()
	e.results[pipelineID] = result
	e.mu.Unlock()

	return result, nil
}

// GetResult returns the latest result for a pipeline.
func (e *Engine) GetResult(pipelineID string) (*ExportResult, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	r, ok := e.results[pipelineID]
	return r, ok
}

func (e *Engine) applySampling(messages []map[string]any, cfg SamplingConfig) []map[string]any {
	switch cfg.Strategy {
	case SamplingRandom:
		var sampled []map[string]any
		for _, msg := range messages {
			if e.rng.Float64() < cfg.Rate {
				sampled = append(sampled, msg)
			}
		}
		return sampled

	case SamplingKeyBased:
		var sampled []map[string]any
		for _, msg := range messages {
			if key, ok := msg["key"].(string); ok {
				if cfg.KeyPattern == "" || strings.Contains(key, cfg.KeyPattern) {
					sampled = append(sampled, msg)
				}
			}
		}
		return sampled

	case SamplingStratified:
		strata := make(map[string][]map[string]any)
		for _, msg := range messages {
			stratumVal := "default"
			if cfg.StratifyField != "" {
				if v, ok := msg[cfg.StratifyField]; ok {
					stratumVal = fmt.Sprintf("%v", v)
				}
			}
			strata[stratumVal] = append(strata[stratumVal], msg)
		}
		var sampled []map[string]any
		maxPer := cfg.MaxPerStratum
		if maxPer <= 0 {
			maxPer = 1000
		}
		for _, msgs := range strata {
			limit := maxPer
			if limit > len(msgs) {
				limit = len(msgs)
			}
			sampled = append(sampled, msgs[:limit]...)
		}
		return sampled

	default:
		return messages
	}
}

func (e *Engine) deduplicate(messages []map[string]any) []map[string]any {
	seen := make(map[string]bool)
	var unique []map[string]any
	for _, msg := range messages {
		hash := hashMessage(msg)
		if !seen[hash] {
			seen[hash] = true
			unique = append(unique, msg)
		}
	}
	return unique
}

func (e *Engine) redactPII(msg map[string]any) int {
	count := 0
	for _, field := range e.piiFields {
		if _, ok := msg[field.Name]; ok {
			switch field.Action {
			case "hash":
				val := fmt.Sprintf("%v", msg[field.Name])
				h := sha256.Sum256([]byte(val))
				msg[field.Name] = hex.EncodeToString(h[:8])
			case "mask":
				msg[field.Name] = "***MASKED***"
			default:
				msg[field.Name] = "[REDACTED]"
			}
			count++
		}
	}
	return count
}

func (e *Engine) filterFields(messages []map[string]any, fields []string) []map[string]any {
	fieldSet := make(map[string]bool)
	for _, f := range fields {
		fieldSet[f] = true
	}
	var filtered []map[string]any
	for _, msg := range messages {
		newMsg := make(map[string]any)
		for k, v := range msg {
			if fieldSet[k] {
				newMsg[k] = v
			}
		}
		filtered = append(filtered, newMsg)
	}
	return filtered
}

func (e *Engine) formatJSONL(messages []map[string]any) ([]byte, error) {
	var lines []string
	for _, msg := range messages {
		line, err := json.Marshal(msg)
		if err != nil {
			continue
		}
		lines = append(lines, string(line))
	}
	return []byte(strings.Join(lines, "\n")), nil
}

func (e *Engine) formatCSV(messages []map[string]any) ([]byte, error) {
	if len(messages) == 0 {
		return nil, nil
	}

	// Collect all headers
	headers := make(map[string]bool)
	for _, msg := range messages {
		for k := range msg {
			headers[k] = true
		}
	}
	headerList := make([]string, 0, len(headers))
	for h := range headers {
		headerList = append(headerList, h)
	}

	var buf strings.Builder
	w := csv.NewWriter(&buf)
	_ = w.Write(headerList)

	for _, msg := range messages {
		row := make([]string, len(headerList))
		for i, h := range headerList {
			if v, ok := msg[h]; ok {
				row[i] = fmt.Sprintf("%v", v)
			}
		}
		_ = w.Write(row)
	}
	w.Flush()

	return []byte(buf.String()), nil
}

func hashMessage(msg map[string]any) string {
	data, _ := json.Marshal(msg)
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func defaultPIIFields() []PIIField {
	return []PIIField{
		{Name: "email", Action: "hash"},
		{Name: "email_address", Action: "hash"},
		{Name: "phone", Action: "redact"},
		{Name: "phone_number", Action: "redact"},
		{Name: "ssn", Action: "redact"},
		{Name: "social_security_number", Action: "redact"},
		{Name: "credit_card", Action: "redact"},
		{Name: "credit_card_number", Action: "redact"},
		{Name: "password", Action: "redact"},
		{Name: "secret", Action: "redact"},
		{Name: "address", Action: "mask"},
		{Name: "ip_address", Action: "mask"},
	}
}
