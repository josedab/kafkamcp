// Package streamsql provides a lightweight streaming SQL engine for querying
// Kafka topics with support for tumbling/sliding/session windows, aggregations,
// and continuous result emission.
package streamsql

import (
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Query represents a parsed streaming SQL query.
type Query struct {
	ID          string            `json:"id"`
	SQL         string            `json:"sql"`
	SourceTopic string            `json:"source_topic"`
	SelectFields []string         `json:"select_fields"`
	WhereClause  *WhereClause     `json:"where_clause,omitempty"`
	GroupBy      []string          `json:"group_by,omitempty"`
	Window       *WindowSpec       `json:"window,omitempty"`
	AggFunctions []AggFunction     `json:"agg_functions,omitempty"`
	Limit        int               `json:"limit,omitempty"`
	Status       string            `json:"status"` // created, running, completed, error
	CreatedBy    string            `json:"created_by"`
	CreatedAt    time.Time         `json:"created_at"`
	Error        string            `json:"error,omitempty"`
}

// WhereClause represents a filter condition.
type WhereClause struct {
	Field    string `json:"field"`
	Operator string `json:"operator"` // =, !=, >, <, >=, <=, LIKE, IN
	Value    string `json:"value"`
}

// WindowSpec defines a window for streaming aggregation.
type WindowSpec struct {
	Type     string `json:"type"` // tumbling, sliding, session
	SizeMs   int64  `json:"size_ms"`
	SlideMs  int64  `json:"slide_ms,omitempty"` // for sliding windows
	GapMs    int64  `json:"gap_ms,omitempty"`   // for session windows
}

// AggFunction represents an aggregation function.
type AggFunction struct {
	Function string `json:"function"` // COUNT, SUM, AVG, MIN, MAX
	Field    string `json:"field"`
	Alias    string `json:"alias"`
}

// QueryResult holds the results of a SQL query execution.
type QueryResult struct {
	QueryID     string           `json:"query_id"`
	SQL         string           `json:"sql"`
	Columns     []string         `json:"columns"`
	Rows        []map[string]any `json:"rows"`
	RowCount    int              `json:"row_count"`
	WindowStart *time.Time       `json:"window_start,omitempty"`
	WindowEnd   *time.Time       `json:"window_end,omitempty"`
	QueryTimeMs int64            `json:"query_time_ms"`
}

// QueryPlan describes how a SQL query will be executed.
type QueryPlan struct {
	QueryID    string   `json:"query_id"`
	SQL        string   `json:"sql"`
	Steps      []string `json:"steps"`
	EstCost    string   `json:"estimated_cost"`
	SourceType string   `json:"source_type"`
}

// Engine manages streaming SQL queries.
type Engine struct {
	mu      sync.RWMutex
	queries map[string]*Query
	results map[string]*QueryResult
	logger  *slog.Logger
	nextID  int64
}

// NewEngine creates a new streaming SQL engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		queries: make(map[string]*Query),
		results: make(map[string]*QueryResult),
		logger:  logger,
	}
}

// Parse parses a SQL query string into a Query struct.
func (e *Engine) Parse(sql, createdBy string) (*Query, error) {
	if sql == "" {
		return nil, fmt.Errorf("SQL query is required")
	}

	sql = strings.TrimSpace(sql)
	upperSQL := strings.ToUpper(sql)

	if !strings.HasPrefix(upperSQL, "SELECT") {
		return nil, fmt.Errorf("only SELECT queries are supported")
	}

	query := &Query{
		SQL:       sql,
		CreatedBy: createdBy,
		CreatedAt: time.Now(),
		Status:    "created",
	}

	// Extract FROM topic
	fromRegex := regexp.MustCompile(`(?i)FROM\s+(\S+)`)
	if matches := fromRegex.FindStringSubmatch(sql); len(matches) > 1 {
		query.SourceTopic = strings.Trim(matches[1], "`\"'")
	} else {
		return nil, fmt.Errorf("FROM clause is required")
	}

	// Extract SELECT fields
	selectRegex := regexp.MustCompile(`(?i)SELECT\s+(.+?)\s+FROM`)
	if matches := selectRegex.FindStringSubmatch(sql); len(matches) > 1 {
		fields := strings.Split(matches[1], ",")
		for _, f := range fields {
			f = strings.TrimSpace(f)
			// Check for aggregate functions
			aggRegex := regexp.MustCompile(`(?i)(COUNT|SUM|AVG|MIN|MAX)\s*\(\s*(\*|\w+)\s*\)(?:\s+(?:AS\s+)?(\w+))?`)
			if aggMatches := aggRegex.FindStringSubmatch(f); len(aggMatches) > 1 {
				alias := aggMatches[3]
				if alias == "" {
					alias = strings.ToLower(aggMatches[1]) + "_" + aggMatches[2]
				}
				query.AggFunctions = append(query.AggFunctions, AggFunction{
					Function: strings.ToUpper(aggMatches[1]),
					Field:    aggMatches[2],
					Alias:    alias,
				})
				query.SelectFields = append(query.SelectFields, alias)
			} else {
				query.SelectFields = append(query.SelectFields, f)
			}
		}
	}

	// Extract WHERE clause (simple single condition)
	whereRegex := regexp.MustCompile(`(?i)WHERE\s+(\w+)\s*(=|!=|>|<|>=|<=|LIKE|IN)\s*'?([^'\s]+)'?`)
	if matches := whereRegex.FindStringSubmatch(sql); len(matches) > 3 {
		query.WhereClause = &WhereClause{
			Field:    matches[1],
			Operator: strings.ToUpper(matches[2]),
			Value:    matches[3],
		}
	}

	// Extract GROUP BY
	groupByRegex := regexp.MustCompile(`(?i)GROUP\s+BY\s+(.+?)(?:\s+WINDOW|\s+HAVING|\s+ORDER|\s+LIMIT|$)`)
	if matches := groupByRegex.FindStringSubmatch(sql); len(matches) > 1 {
		fields := strings.Split(matches[1], ",")
		for _, f := range fields {
			query.GroupBy = append(query.GroupBy, strings.TrimSpace(f))
		}
	}

	// Extract WINDOW clause
	windowRegex := regexp.MustCompile(`(?i)WINDOW\s+(TUMBLING|SLIDING|SESSION)\s+(\d+)(ms|s|m|h)`)
	if matches := windowRegex.FindStringSubmatch(sql); len(matches) > 3 {
		size, err := strconv.ParseInt(matches[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid WINDOW size %q: %w", matches[2], err)
		}
		switch matches[3] {
		case "s":
			size *= 1000
		case "m":
			size *= 60000
		case "h":
			size *= 3600000
		}
		query.Window = &WindowSpec{
			Type:   strings.ToLower(matches[1]),
			SizeMs: size,
		}
	}

	// Extract LIMIT
	limitRegex := regexp.MustCompile(`(?i)LIMIT\s+(\d+)`)
	if matches := limitRegex.FindStringSubmatch(sql); len(matches) > 1 {
		limit, err := strconv.Atoi(matches[1])
		if err != nil {
			return nil, fmt.Errorf("invalid LIMIT value %q: %w", matches[1], err)
		}
		query.Limit = limit
	}

	e.mu.Lock()
	e.nextID++
	query.ID = fmt.Sprintf("sql_%d", e.nextID)
	e.queries[query.ID] = query
	e.mu.Unlock()

	return query, nil
}

// Execute runs a parsed query against provided messages.
func (e *Engine) Execute(queryID string, messages []map[string]any) (*QueryResult, error) {
	e.mu.Lock()
	query, ok := e.queries[queryID]
	if !ok {
		e.mu.Unlock()
		return nil, fmt.Errorf("query %q not found", queryID)
	}
	query.Status = "running"
	e.mu.Unlock()

	start := time.Now()

	// Apply WHERE filter
	var filtered []map[string]any
	for _, msg := range messages {
		if query.WhereClause != nil {
			if !matchWhere(msg, query.WhereClause) {
				continue
			}
		}
		filtered = append(filtered, msg)
	}

	var rows []map[string]any

	if len(query.AggFunctions) > 0 || len(query.GroupBy) > 0 {
		rows = e.executeAggregation(query, filtered)
	} else {
		// Simple projection
		for _, msg := range filtered {
			row := make(map[string]any)
			for _, field := range query.SelectFields {
				if field == "*" {
					for k, v := range msg {
						row[k] = v
					}
				} else if val, ok := msg[field]; ok {
					row[field] = val
				}
			}
			rows = append(rows, row)
		}
	}

	if query.Limit > 0 && len(rows) > query.Limit {
		rows = rows[:query.Limit]
	}

	result := &QueryResult{
		QueryID:     queryID,
		SQL:         query.SQL,
		Columns:     query.SelectFields,
		Rows:        rows,
		RowCount:    len(rows),
		QueryTimeMs: time.Since(start).Milliseconds(),
	}

	e.mu.Lock()
	query.Status = "completed"
	e.results[queryID] = result
	e.mu.Unlock()

	return result, nil
}

func (e *Engine) executeAggregation(query *Query, messages []map[string]any) []map[string]any {
	// Group messages
	groups := make(map[string][]map[string]any)
	for _, msg := range messages {
		key := "_all"
		if len(query.GroupBy) > 0 {
			var parts []string
			for _, field := range query.GroupBy {
				val := fmt.Sprintf("%v", msg[field])
				parts = append(parts, val)
			}
			key = strings.Join(parts, "|")
		}
		groups[key] = append(groups[key], msg)
	}

	var rows []map[string]any
	for groupKey, groupMsgs := range groups {
		row := make(map[string]any)

		// Add group by values
		if len(query.GroupBy) > 0 && len(groupMsgs) > 0 {
			for _, field := range query.GroupBy {
				row[field] = groupMsgs[0][field]
			}
		}
		_ = groupKey

		// Apply aggregation functions
		for _, agg := range query.AggFunctions {
			switch agg.Function {
			case "COUNT":
				row[agg.Alias] = len(groupMsgs)
			case "SUM":
				var sum float64
				for _, m := range groupMsgs {
					sum += toFloat64(m[agg.Field])
				}
				row[agg.Alias] = sum
			case "AVG":
				var sum float64
				for _, m := range groupMsgs {
					sum += toFloat64(m[agg.Field])
				}
				if len(groupMsgs) > 0 {
					row[agg.Alias] = sum / float64(len(groupMsgs))
				}
			case "MIN":
				min := toFloat64(groupMsgs[0][agg.Field])
				for _, m := range groupMsgs[1:] {
					v := toFloat64(m[agg.Field])
					if v < min {
						min = v
					}
				}
				row[agg.Alias] = min
			case "MAX":
				max := toFloat64(groupMsgs[0][agg.Field])
				for _, m := range groupMsgs[1:] {
					v := toFloat64(m[agg.Field])
					if v > max {
						max = v
					}
				}
				row[agg.Alias] = max
			}
		}

		rows = append(rows, row)
	}

	sort.Slice(rows, func(i, j int) bool {
		for _, field := range query.GroupBy {
			vi := fmt.Sprintf("%v", rows[i][field])
			vj := fmt.Sprintf("%v", rows[j][field])
			if vi != vj {
				return vi < vj
			}
		}
		return false
	})

	return rows
}

// Explain generates a query execution plan.
func (e *Engine) Explain(queryID string) (*QueryPlan, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	query, ok := e.queries[queryID]
	if !ok {
		return nil, fmt.Errorf("query %q not found", queryID)
	}

	plan := &QueryPlan{
		QueryID:    queryID,
		SQL:        query.SQL,
		SourceType: "kafka_topic",
	}

	plan.Steps = append(plan.Steps, fmt.Sprintf("1. Consume from topic: %s", query.SourceTopic))

	if query.WhereClause != nil {
		plan.Steps = append(plan.Steps, fmt.Sprintf("2. Filter: %s %s %s", query.WhereClause.Field, query.WhereClause.Operator, query.WhereClause.Value))
	}

	if query.Window != nil {
		plan.Steps = append(plan.Steps, fmt.Sprintf("3. Window: %s (%dms)", query.Window.Type, query.Window.SizeMs))
	}

	if len(query.GroupBy) > 0 {
		plan.Steps = append(plan.Steps, fmt.Sprintf("4. Group by: %s", strings.Join(query.GroupBy, ", ")))
	}

	if len(query.AggFunctions) > 0 {
		var aggs []string
		for _, a := range query.AggFunctions {
			aggs = append(aggs, fmt.Sprintf("%s(%s)", a.Function, a.Field))
		}
		plan.Steps = append(plan.Steps, fmt.Sprintf("5. Aggregate: %s", strings.Join(aggs, ", ")))
	}

	plan.Steps = append(plan.Steps, fmt.Sprintf("6. Project: %s", strings.Join(query.SelectFields, ", ")))

	if query.Limit > 0 {
		plan.Steps = append(plan.Steps, fmt.Sprintf("7. Limit: %d", query.Limit))
	}

	plan.EstCost = "low"
	if query.Window != nil || len(query.AggFunctions) > 0 {
		plan.EstCost = "medium"
	}

	return plan, nil
}

// GetQuery returns a query by ID.
func (e *Engine) GetQuery(id string) (*Query, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	q, ok := e.queries[id]
	if !ok {
		return nil, fmt.Errorf("query %q not found", id)
	}
	return q, nil
}

// ListQueries returns all queries.
func (e *Engine) ListQueries(limit int) []Query {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if limit <= 0 {
		limit = 50
	}

	var result []Query
	for _, q := range e.queries {
		result = append(result, *q)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.After(result[j].CreatedAt)
	})

	if len(result) > limit {
		result = result[:limit]
	}
	return result
}

// DeleteQuery removes a query.
func (e *Engine) DeleteQuery(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.queries[id]; !ok {
		return fmt.Errorf("query %q not found", id)
	}
	delete(e.queries, id)
	delete(e.results, id)
	return nil
}

func matchWhere(msg map[string]any, where *WhereClause) bool {
	val, ok := msg[where.Field]
	if !ok {
		return false
	}

	strVal := fmt.Sprintf("%v", val)
	switch where.Operator {
	case "=":
		return strVal == where.Value
	case "!=":
		return strVal != where.Value
	case ">":
		return toFloat64(val) > toFloat64(where.Value)
	case "<":
		return toFloat64(val) < toFloat64(where.Value)
	case ">=":
		return toFloat64(val) >= toFloat64(where.Value)
	case "<=":
		return toFloat64(val) <= toFloat64(where.Value)
	case "LIKE":
		pattern := strings.ReplaceAll(where.Value, "%", ".*")
		matched, _ := regexp.MatchString("(?i)^"+pattern+"$", strVal)
		return matched
	default:
		return strVal == where.Value
	}
}

func toFloat64(v any) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	default:
		return 0
	}
}
