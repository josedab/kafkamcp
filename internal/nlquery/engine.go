package nlquery

import (
	"log/slog"
	"regexp"
	"sync"
	"time"
)

// QueryPlan represents the decomposed NL query execution plan.
type QueryPlan struct {
	Intent      string            `json:"intent"`
	Topics      []string          `json:"topics"`
	Filters     []Filter          `json:"filters"`
	Aggregation *AggregationStep  `json:"aggregation,omitempty"`
	TimeRange   *TimeRange        `json:"time_range,omitempty"`
	Limit       int               `json:"limit"`
	SortBy      string            `json:"sort_by,omitempty"`
	RawQuery    string            `json:"raw_query"`
}

// Filter represents a field-level filter condition.
type Filter struct {
	Field    string `json:"field"`
	Operator string `json:"operator"` // eq, neq, contains, gt, lt, gte, lte, regex
	Value    string `json:"value"`
}

// AggregationStep defines an aggregation operation.
type AggregationStep struct {
	Function string `json:"function"` // count, sum, avg, min, max, group_by
	Field    string `json:"field"`
	GroupBy  string `json:"group_by,omitempty"`
}

// TimeRange defines a temporal filter.
type TimeRange struct {
	Start    string `json:"start"`
	End      string `json:"end,omitempty"`
	Duration string `json:"duration,omitempty"`
}

// QueryResult contains the result of an NL query.
type QueryResult struct {
	Plan       QueryPlan        `json:"plan"`
	Data       []map[string]any `json:"data"`
	TotalCount int              `json:"total_count"`
	Provenance Provenance       `json:"provenance"`
	Duration   string           `json:"duration"`
}

// Provenance tracks where data came from.
type Provenance struct {
	TopicsScanned    []string `json:"topics_scanned"`
	MessagesScanned  int      `json:"messages_scanned"`
	MessagesMatched  int      `json:"messages_matched"`
	FiltersApplied   int      `json:"filters_applied"`
}

// SchemaInfo provides field information for a topic.
type SchemaInfo struct {
	Topic  string            `json:"topic"`
	Fields map[string]string `json:"fields"` // field name -> type
}

// ExecutionResult represents the result of executing a parsed query plan.
type ExecutionResult struct {
	Plan         QueryPlan        `json:"plan"`
	Data         []map[string]any `json:"data"`
	TotalCount   int              `json:"total_count"`
	Provenance   Provenance       `json:"provenance"`
	Duration     string           `json:"duration"`
	FromCache    bool             `json:"from_cache,omitempty"`
	Suggestions  []string         `json:"suggestions,omitempty"`
}

// QueryHistoryEntry records a past query for auto-suggest and re-use.
type QueryHistoryEntry struct {
	Query     string    `json:"query"`
	Plan      QueryPlan `json:"plan"`
	Timestamp time.Time `json:"timestamp"`
	ResultCount int     `json:"result_count"`
	DurationMs  int64   `json:"duration_ms"`
}

// CachedResult stores a cached query result with expiry.
type CachedResult struct {
	Result   ExecutionResult
	CachedAt time.Time
	TTL      time.Duration
}

// Engine is the natural language query processing engine.
type Engine struct {
	logger          *slog.Logger
	schemas         map[string]SchemaInfo
	queryPatterns   []queryPattern
	queryHistory    []QueryHistoryEntry
	queryCache      map[string]*CachedResult
	cacheTTL        time.Duration
	maxHistory      int
	maxCacheEntries int
	mu              sync.RWMutex
}

type queryPattern struct {
	pattern *regexp.Regexp
	intent  string
	extract func(matches []string) partialPlan
}

type partialPlan struct {
	filters     []Filter
	aggregation *AggregationStep
	timeRange   *TimeRange
	topics      []string
}

// NewEngine creates a new NL query engine.
func NewEngine(logger *slog.Logger) *Engine {
	e := &Engine{
		logger:          logger,
		schemas:         make(map[string]SchemaInfo),
		queryCache:      make(map[string]*CachedResult),
		cacheTTL:        5 * time.Minute,
		maxHistory:      100,
		maxCacheEntries: 1000,
	}
	e.initPatterns()
	return e
}

// RegisterSchema adds schema information for topic field resolution.
func (e *Engine) RegisterSchema(info SchemaInfo) {
	e.schemas[info.Topic] = info
}
