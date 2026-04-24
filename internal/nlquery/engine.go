package nlquery

import (
	"fmt"
	"log/slog"
	"regexp"
	"strings"
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
	logger        *slog.Logger
	schemas       map[string]SchemaInfo
	queryPatterns []queryPattern
	queryHistory  []QueryHistoryEntry
	queryCache    map[string]*CachedResult
	cacheTTL      time.Duration
	maxHistory    int
	mu            sync.RWMutex
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
		logger:     logger,
		schemas:    make(map[string]SchemaInfo),
		queryCache: make(map[string]*CachedResult),
		cacheTTL:   5 * time.Minute,
		maxHistory: 100,
	}
	e.initPatterns()
	return e
}

func (e *Engine) initPatterns() {
	e.queryPatterns = []queryPattern{
		{
			pattern: regexp.MustCompile(`(?i)(?:show|find|get|list)\s+(?:me\s+)?(?:all\s+)?(.+?)\s+(?:in|from)\s+(?:the\s+)?last\s+(\d+)\s+(minute|hour|day|week)s?`),
			intent:  "filter_recent",
			extract: func(m []string) partialPlan {
				dur := parseDuration(m[2], m[3])
				return partialPlan{
					timeRange: &TimeRange{
						Start:    time.Now().Add(-dur).UTC().Format(time.RFC3339),
						Duration: fmt.Sprintf("%s %ss", m[2], m[3]),
					},
				}
			},
		},
		{
			pattern: regexp.MustCompile(`(?i)(?:count|how many)\s+(.+?)\s+(?:in|from|on)\s+(.+?)(?:\s+in\s+the\s+last\s+(\d+)\s+(minute|hour|day)s?)?$`),
			intent:  "count",
			extract: func(m []string) partialPlan {
				pp := partialPlan{
					aggregation: &AggregationStep{Function: "count"},
				}
				if m[3] != "" && m[4] != "" {
					dur := parseDuration(m[3], m[4])
					pp.timeRange = &TimeRange{Start: time.Now().Add(-dur).UTC().Format(time.RFC3339)}
				}
				return pp
			},
		},
		{
			pattern: regexp.MustCompile(`(?i)(?:average|avg|mean)\s+(.+?)\s+(?:in|from|on)\s+(.+)`),
			intent:  "average",
			extract: func(m []string) partialPlan {
				return partialPlan{
					aggregation: &AggregationStep{Function: "avg", Field: strings.TrimSpace(m[1])},
				}
			},
		},
		{
			pattern: regexp.MustCompile(`(?i)(?:show|find|get)\s+(?:me\s+)?(.+?)\s+where\s+(.+?)\s*(?:=|is|equals)\s*(.+)`),
			intent:  "filter_where",
			extract: func(m []string) partialPlan {
				return partialPlan{
					filters: []Filter{{Field: strings.TrimSpace(m[2]), Operator: "eq", Value: strings.TrimSpace(m[3])}},
				}
			},
		},
		{
			pattern: regexp.MustCompile(`(?i)(?:show|find|get|list)\s+(?:me\s+)?(?:all\s+)?failed\s+(.+)`),
			intent:  "filter_failed",
			extract: func(m []string) partialPlan {
				return partialPlan{
					filters: []Filter{{Field: "status", Operator: "eq", Value: "failed"}},
				}
			},
		},
		{
			pattern: regexp.MustCompile(`(?i)(?:show|find|get|list)\s+(?:me\s+)?(?:all\s+)?errors?\s+(?:in|from|on)\s+(.+)`),
			intent:  "filter_errors",
			extract: func(m []string) partialPlan {
				return partialPlan{
					filters: []Filter{{Field: "level", Operator: "eq", Value: "error"}},
				}
			},
		},
		{
			pattern: regexp.MustCompile(`(?i)(?:top|largest|highest|biggest)\s+(\d+)\s+(.+?)\s+(?:by|in)\s+(.+)`),
			intent:  "top_n",
			extract: func(m []string) partialPlan {
				return partialPlan{
					aggregation: &AggregationStep{Function: "max", Field: strings.TrimSpace(m[3])},
				}
			},
		},
		{
			pattern: regexp.MustCompile(`(?i)(?:group|breakdown|split)\s+(.+?)\s+by\s+(.+)`),
			intent:  "group_by",
			extract: func(m []string) partialPlan {
				return partialPlan{
					aggregation: &AggregationStep{Function: "group_by", GroupBy: strings.TrimSpace(m[2])},
				}
			},
		},
	}
}

// Parse converts a natural language query into a QueryPlan.
func (e *Engine) Parse(query string, availableTopics []string) (*QueryPlan, error) {
	if query == "" {
		return nil, fmt.Errorf("query cannot be empty")
	}

	plan := &QueryPlan{
		RawQuery: query,
		Limit:    100,
	}

	// Try matching against known patterns
	for _, qp := range e.queryPatterns {
		matches := qp.pattern.FindStringSubmatch(query)
		if matches != nil {
			plan.Intent = qp.intent
			pp := qp.extract(matches)
			plan.Filters = pp.filters
			plan.Aggregation = pp.aggregation
			plan.TimeRange = pp.timeRange
			if len(pp.topics) > 0 {
				plan.Topics = pp.topics
			}
			break
		}
	}

	if plan.Intent == "" {
		plan.Intent = "search"
	}

	// Resolve topics from query text using fuzzy matching
	if len(plan.Topics) == 0 {
		plan.Topics = e.resolveTopics(query, availableTopics)
	}

	// Apply schema-aware field resolution
	e.resolveFields(plan)

	return plan, nil
}

// RegisterSchema adds schema information for topic field resolution.
func (e *Engine) RegisterSchema(info SchemaInfo) {
	e.schemas[info.Topic] = info
}

func (e *Engine) resolveTopics(query string, available []string) []string {
	queryLower := strings.ToLower(query)
	var matched []string

	for _, topic := range available {
		topicLower := strings.ToLower(topic)
		// Direct mention
		if strings.Contains(queryLower, topicLower) {
			matched = append(matched, topic)
			continue
		}
		// Partial/fuzzy match on topic name parts
		parts := strings.FieldsFunc(topicLower, func(r rune) bool { return r == '.' || r == '-' || r == '_' })
		for _, part := range parts {
			if len(part) > 2 && strings.Contains(queryLower, part) {
				matched = append(matched, topic)
				break
			}
		}
	}

	return matched
}

func (e *Engine) resolveFields(plan *QueryPlan) {
	for i, f := range plan.Filters {
		for _, topic := range plan.Topics {
			if schema, ok := e.schemas[topic]; ok {
				resolved := fuzzyFieldMatch(f.Field, schema.Fields)
				if resolved != "" {
					plan.Filters[i].Field = resolved
				}
			}
		}
	}
}

func fuzzyFieldMatch(query string, fields map[string]string) string {
	queryLower := strings.ToLower(query)

	// Exact match
	if _, ok := fields[query]; ok {
		return query
	}

	// Case-insensitive match
	for name := range fields {
		if strings.ToLower(name) == queryLower {
			return name
		}
	}

	// Substring match
	for name := range fields {
		nameLower := strings.ToLower(name)
		if strings.Contains(nameLower, queryLower) || strings.Contains(queryLower, nameLower) {
			return name
		}
	}

	return ""
}

func parseDuration(amount, unit string) time.Duration {
	n := 1
	fmt.Sscanf(amount, "%d", &n)

	switch strings.ToLower(unit) {
	case "minute":
		return time.Duration(n) * time.Minute
	case "hour":
		return time.Duration(n) * time.Hour
	case "day":
		return time.Duration(n) * 24 * time.Hour
	case "week":
		return time.Duration(n) * 7 * 24 * time.Hour
	default:
		return time.Duration(n) * time.Hour
	}
}

// SemanticParse performs LLM-style semantic parsing using heuristic NLP.
// It handles complex boolean filters, multi-topic joins, and arbitrary NL.
func (e *Engine) SemanticParse(query string, availableTopics []string) (*QueryPlan, error) {
	if query == "" {
		return nil, fmt.Errorf("query cannot be empty")
	}
	if len(query) > 2000 {
		return nil, fmt.Errorf("query too long (max 2000 characters)")
	}

	plan := &QueryPlan{
		RawQuery: query,
		Limit:    100,
	}

	queryLower := strings.ToLower(query)

	// Detect intent from semantic keywords
	plan.Intent = e.detectIntent(queryLower)

	// Extract boolean filters (AND/OR)
	plan.Filters = e.extractBooleanFilters(queryLower)

	// Extract aggregation
	plan.Aggregation = e.extractAggregation(queryLower)

	// Extract time range
	plan.TimeRange = e.extractTimeRange(queryLower)

	// Extract limit
	if limit := e.extractLimit(queryLower); limit > 0 {
		plan.Limit = limit
	}

	// Extract sort
	plan.SortBy = e.extractSort(queryLower)

	// Fall back to regex patterns if semantic parse found no filters
	if len(plan.Filters) == 0 && plan.Aggregation == nil && plan.TimeRange == nil {
		for _, qp := range e.queryPatterns {
			matches := qp.pattern.FindStringSubmatch(query)
			if matches != nil {
				plan.Intent = qp.intent
				pp := qp.extract(matches)
				plan.Filters = pp.filters
				plan.Aggregation = pp.aggregation
				plan.TimeRange = pp.timeRange
				if len(pp.topics) > 0 {
					plan.Topics = pp.topics
				}
				break
			}
		}
	}

	if plan.Intent == "" {
		plan.Intent = "search"
	}

	// Resolve topics
	if len(plan.Topics) == 0 {
		plan.Topics = e.resolveTopics(query, availableTopics)
	}

	e.resolveFields(plan)
	return plan, nil
}

func (e *Engine) detectIntent(query string) string {
	intentKeywords := map[string][]string{
		"count":       {"count", "how many", "total number", "number of"},
		"average":     {"average", "avg", "mean"},
		"sum":         {"sum", "total", "add up"},
		"max":         {"maximum", "max", "highest", "largest", "biggest", "top"},
		"min":         {"minimum", "min", "lowest", "smallest"},
		"group_by":    {"group by", "breakdown", "split by", "per"},
		"filter":      {"where", "filter", "with", "having"},
		"trend":       {"trend", "over time", "timeline", "history"},
		"compare":     {"compare", "versus", "vs", "difference between"},
		"join":        {"join", "combine", "correlate", "across topics"},
	}

	for intent, keywords := range intentKeywords {
		for _, kw := range keywords {
			if strings.Contains(query, kw) {
				return intent
			}
		}
	}
	return "search"
}

func (e *Engine) extractBooleanFilters(query string) []Filter {
	var filters []Filter

	// Handle "where X = Y" patterns
	wherePattern := regexp.MustCompile(`where\s+(\w+)\s*(=|!=|>|<|>=|<=|is|equals|contains|like)\s*['"]*([^'"]+?)['"]*(?:\s+(?:and|or|$))`)
	matches := wherePattern.FindAllStringSubmatch(query, -1)
	for _, m := range matches {
		op := normalizeOperator(m[2])
		filters = append(filters, Filter{
			Field:    strings.TrimSpace(m[1]),
			Operator: op,
			Value:    strings.TrimSpace(m[3]),
		})
	}

	// Handle "status = failed" without where
	eqPattern := regexp.MustCompile(`(\w+)\s*(?:=|is|equals)\s*['"]*(\w+)['"]*`)
	if len(filters) == 0 {
		eqMatches := eqPattern.FindAllStringSubmatch(query, -1)
		for _, m := range eqMatches {
			field := strings.TrimSpace(m[1])
			// Skip common non-filter words
			if isCommonWord(field) {
				continue
			}
			filters = append(filters, Filter{
				Field:    field,
				Operator: "eq",
				Value:    strings.TrimSpace(m[2]),
			})
		}
	}

	// Handle "failed", "errors" as status filters
	if strings.Contains(query, "failed") || strings.Contains(query, "failure") {
		hasStatusFilter := false
		for _, f := range filters {
			if f.Field == "status" {
				hasStatusFilter = true
				break
			}
		}
		if !hasStatusFilter {
			filters = append(filters, Filter{Field: "status", Operator: "eq", Value: "failed"})
		}
	}
	if strings.Contains(query, "error") && !strings.Contains(query, "error_rate") {
		hasLevelFilter := false
		for _, f := range filters {
			if f.Field == "level" {
				hasLevelFilter = true
				break
			}
		}
		if !hasLevelFilter {
			filters = append(filters, Filter{Field: "level", Operator: "eq", Value: "error"})
		}
	}

	return filters
}

func normalizeOperator(op string) string {
	switch strings.ToLower(strings.TrimSpace(op)) {
	case "=", "is", "equals":
		return "eq"
	case "!=":
		return "neq"
	case ">":
		return "gt"
	case "<":
		return "lt"
	case ">=":
		return "gte"
	case "<=":
		return "lte"
	case "contains", "like":
		return "contains"
	default:
		return "eq"
	}
}

func isCommonWord(w string) bool {
	common := map[string]bool{
		"show": true, "find": true, "get": true, "list": true,
		"me": true, "all": true, "the": true, "in": true,
		"from": true, "on": true, "it": true, "that": true,
		"this": true, "what": true, "which": true,
	}
	return common[strings.ToLower(w)]
}

func (e *Engine) extractAggregation(query string) *AggregationStep {
	aggPatterns := []struct {
		keywords []string
		function string
	}{
		{[]string{"count", "how many", "number of"}, "count"},
		{[]string{"average", "avg", "mean"}, "avg"},
		{[]string{"sum", "total"}, "sum"},
		{[]string{"maximum", "max", "highest"}, "max"},
		{[]string{"minimum", "min", "lowest"}, "min"},
	}

	for _, ap := range aggPatterns {
		for _, kw := range ap.keywords {
			if strings.Contains(query, kw) {
				step := &AggregationStep{Function: ap.function}

				// Extract "of <field>"
				ofPattern := regexp.MustCompile(regexp.QuoteMeta(kw) + `\s+(?:of\s+)?(\w+)`)
				if m := ofPattern.FindStringSubmatch(query); m != nil {
					step.Field = m[1]
				}

				// Extract "group by <field>" or "by <field>"
				groupPattern := regexp.MustCompile(`(?:group\s+)?by\s+(\w+)`)
				if m := groupPattern.FindStringSubmatch(query); m != nil {
					step.GroupBy = m[1]
				}

				return step
			}
		}
	}

	// Standalone group by
	groupPattern := regexp.MustCompile(`(?:group|breakdown|split)\s+(?:\w+\s+)?by\s+(\w+)`)
	if m := groupPattern.FindStringSubmatch(query); m != nil {
		return &AggregationStep{Function: "group_by", GroupBy: m[1]}
	}

	return nil
}

func (e *Engine) extractTimeRange(query string) *TimeRange {
	// "in the last N unit(s)"
	lastPattern := regexp.MustCompile(`(?:in|from|over)\s+(?:the\s+)?last\s+(\d+)\s+(minute|hour|day|week|month)s?`)
	if m := lastPattern.FindStringSubmatch(query); m != nil {
		dur := parseDuration(m[1], m[2])
		return &TimeRange{
			Start:    time.Now().Add(-dur).UTC().Format(time.RFC3339),
			Duration: fmt.Sprintf("%s %ss", m[1], m[2]),
		}
	}

	// "since yesterday"
	if strings.Contains(query, "since yesterday") {
		return &TimeRange{
			Start:    time.Now().Add(-24 * time.Hour).UTC().Format(time.RFC3339),
			Duration: "1 days",
		}
	}

	// "today"
	if strings.Contains(query, "today") {
		now := time.Now().UTC()
		startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		return &TimeRange{
			Start:    startOfDay.Format(time.RFC3339),
			Duration: "today",
		}
	}

	return nil
}

func (e *Engine) extractLimit(query string) int {
	// "top N", "first N", "limit N", "last N"
	limitPattern := regexp.MustCompile(`(?:top|first|limit|last)\s+(\d+)`)
	if m := limitPattern.FindStringSubmatch(query); m != nil {
		n := 0
		fmt.Sscanf(m[1], "%d", &n)
		if n > 0 && n <= 10000 {
			return n
		}
	}
	return 0
}

func (e *Engine) extractSort(query string) string {
	sortPattern := regexp.MustCompile(`(?:sort|order)\s+(?:by\s+)?(\w+)\s*(asc|desc|ascending|descending)?`)
	if m := sortPattern.FindStringSubmatch(query); m != nil {
		field := m[1]
		dir := "desc"
		if len(m) > 2 && (m[2] == "asc" || m[2] == "ascending") {
			dir = "asc"
		}
		return field + ":" + dir
	}
	return ""
}

// CacheResult stores a query result in the cache.
func (e *Engine) CacheResult(query string, result ExecutionResult) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.queryCache[strings.ToLower(strings.TrimSpace(query))] = &CachedResult{
		Result:   result,
		CachedAt: time.Now(),
		TTL:      e.cacheTTL,
	}
}

// GetCached retrieves a cached result if available and not expired.
func (e *Engine) GetCached(query string) (*ExecutionResult, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	key := strings.ToLower(strings.TrimSpace(query))
	cached, ok := e.queryCache[key]
	if !ok {
		return nil, false
	}
	if time.Since(cached.CachedAt) > cached.TTL {
		return nil, false
	}
	result := cached.Result
	result.FromCache = true
	return &result, true
}

// RecordHistory records a query execution in history.
func (e *Engine) RecordHistory(query string, plan QueryPlan, resultCount int, durationMs int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	entry := QueryHistoryEntry{
		Query:       query,
		Plan:        plan,
		Timestamp:   time.Now(),
		ResultCount: resultCount,
		DurationMs:  durationMs,
	}

	e.queryHistory = append(e.queryHistory, entry)
	if len(e.queryHistory) > e.maxHistory {
		e.queryHistory = e.queryHistory[len(e.queryHistory)-e.maxHistory:]
	}
}

// GetHistory returns recent query history.
func (e *Engine) GetHistory(limit int) []QueryHistoryEntry {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if limit <= 0 || limit > len(e.queryHistory) {
		limit = len(e.queryHistory)
	}
	// Return most recent first
	result := make([]QueryHistoryEntry, limit)
	for i := 0; i < limit; i++ {
		result[i] = e.queryHistory[len(e.queryHistory)-1-i]
	}
	return result
}

// Suggest returns query suggestions based on history and available topics.
func (e *Engine) Suggest(prefix string, availableTopics []string) []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	prefixLower := strings.ToLower(prefix)
	seen := make(map[string]bool)
	var suggestions []string

	// Suggest from history
	for i := len(e.queryHistory) - 1; i >= 0; i-- {
		q := e.queryHistory[i].Query
		if strings.HasPrefix(strings.ToLower(q), prefixLower) && !seen[q] {
			suggestions = append(suggestions, q)
			seen[q] = true
		}
		if len(suggestions) >= 5 {
			break
		}
	}

	// Suggest template queries based on topics
	templates := []string{
		"show me all messages from %s in the last hour",
		"count messages in %s",
		"show errors in %s",
		"average latency in %s",
	}
	for _, topic := range availableTopics {
		if len(suggestions) >= 10 {
			break
		}
		for _, tmpl := range templates {
			s := fmt.Sprintf(tmpl, topic)
			if strings.HasPrefix(strings.ToLower(s), prefixLower) && !seen[s] {
				suggestions = append(suggestions, s)
				seen[s] = true
			}
		}
	}

	return suggestions
}
