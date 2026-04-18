package nlquery

import (
	"fmt"
	"log/slog"
	"regexp"
	"strings"
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

// Engine is the natural language query processing engine.
type Engine struct {
	logger       *slog.Logger
	schemas      map[string]SchemaInfo
	queryPatterns []queryPattern
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
		logger:  logger,
		schemas: make(map[string]SchemaInfo),
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
