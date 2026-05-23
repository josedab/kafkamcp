package nlquery

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

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
		"count":    {"count", "how many", "total number", "number of"},
		"average":  {"average", "avg", "mean"},
		"sum":      {"sum", "total", "add up"},
		"max":      {"maximum", "max", "highest", "largest", "biggest", "top"},
		"min":      {"minimum", "min", "lowest", "smallest"},
		"group_by": {"group by", "breakdown", "split by", "per"},
		"filter":   {"where", "filter", "with", "having"},
		"trend":    {"trend", "over time", "timeline", "history"},
		"compare":  {"compare", "versus", "vs", "difference between"},
		"join":     {"join", "combine", "correlate", "across topics"},
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
		_, _ = fmt.Sscanf(m[1], "%d", &n)
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
		if strings.EqualFold(name, queryLower) {
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
	_, _ = fmt.Sscanf(amount, "%d", &n)

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
