package streamsql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/randid"
)

// CostModel defines the cost parameters for query optimization.
type CostModel struct {
	CPUCostPerMessage   float64 `json:"cpu_cost_per_message"`     // cost units per message processed
	NetworkCostPerMB    float64 `json:"network_cost_per_mb"`      // cost units per MB transferred
	StorageCostPerGB    float64 `json:"storage_cost_per_gb_hour"` // cost units per GB/hour
	AggregationOverhead float64 `json:"aggregation_overhead"`     // multiplier for aggregation ops
	JoinOverhead        float64 `json:"join_overhead"`            // multiplier for join ops
	WindowOverhead      float64 `json:"window_overhead"`          // multiplier for windowed ops
}

// DefaultCostModel returns a reasonable default cost model.
func DefaultCostModel() CostModel {
	return CostModel{
		CPUCostPerMessage:   0.001,
		NetworkCostPerMB:    0.01,
		StorageCostPerGB:    0.05,
		AggregationOverhead: 1.5,
		JoinOverhead:        3.0,
		WindowOverhead:      2.0,
	}
}

// TopicStats holds statistics about a topic for cost estimation.
type TopicStats struct {
	Topic            string  `json:"topic"`
	Partitions       int     `json:"partitions"`
	MessagesPerSec   float64 `json:"messages_per_sec"`
	AvgMessageSizeKB float64 `json:"avg_message_size_kb"`
	TotalSizeGB      float64 `json:"total_size_gb"`
	ConsumerGroups   int     `json:"consumer_groups"`
}

// ExecutionPlan describes how a query will be executed with cost estimates.
type ExecutionPlan struct {
	ID                string         `json:"id"`
	QuerySQL          string         `json:"query_sql"`
	Strategy          string         `json:"strategy"` // full_scan, partition_pruned, indexed, sampled
	EstimatedCost     float64        `json:"estimated_cost"`
	EstimatedMessages int64          `json:"estimated_messages"`
	EstimatedTimeMs   int64          `json:"estimated_time_ms"`
	EstimatedDataMB   float64        `json:"estimated_data_mb"`
	Steps             []PlanStep     `json:"steps"`
	Optimizations     []Optimization `json:"optimizations"`
	BudgetStatus      string         `json:"budget_status"` // within_budget, warning, over_budget
	GeneratedAt       string         `json:"generated_at"`
}

// PlanStep is a step in the execution plan.
type PlanStep struct {
	Order       int     `json:"order"`
	Operation   string  `json:"operation"` // scan, filter, aggregate, window, sort, limit
	Description string  `json:"description"`
	Cost        float64 `json:"cost"`
	Selectivity float64 `json:"selectivity"` // 0.0-1.0, fraction of data passing this step
}

// Optimization is a suggested query optimization.
type Optimization struct {
	Type        string  `json:"type"` // partition_pruning, predicate_pushdown, sampling, parallelism
	Description string  `json:"description"`
	SavedCost   float64 `json:"saved_cost"`
	Applied     bool    `json:"applied"`
}

// Budget defines spending limits for query execution.
type Budget struct {
	ID              string  `json:"id"`
	Name            string  `json:"name"`
	MaxCostPerQuery float64 `json:"max_cost_per_query"`
	MaxCostPerHour  float64 `json:"max_cost_per_hour"`
	CurrentSpend    float64 `json:"current_spend_this_hour"`
	AlertThreshold  float64 `json:"alert_threshold"` // 0.0-1.0
	Status          string  `json:"status"`          // active, warning, exceeded
	CreatedAt       string  `json:"created_at"`
}

// OptimizeQuery analyzes a query and generates a cost-aware execution plan.
func (e *Engine) OptimizeQuery(sql string, topicStats *TopicStats, costModel *CostModel, budget *Budget) (*ExecutionPlan, error) {
	if sql == "" {
		return nil, fmt.Errorf("SQL query is required")
	}

	if costModel == nil {
		cm := DefaultCostModel()
		costModel = &cm
	}

	// Parse the query to understand structure
	query, err := e.parseSQL(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	plan := &ExecutionPlan{
		ID:          randid.New("plan"),
		QuerySQL:    sql,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
	}

	// Estimate data volume
	var messagesPerSec float64 = 1000
	var avgSizeKB float64 = 1.0
	partitions := 1
	if topicStats != nil {
		messagesPerSec = topicStats.MessagesPerSec
		avgSizeKB = topicStats.AvgMessageSizeKB
		partitions = topicStats.Partitions
	}

	// Build execution steps and calculate costs
	step := 1
	totalCost := 0.0
	selectivity := 1.0

	// Step 1: Scan
	scanMessages := int64(messagesPerSec * 60) // 1 minute window default
	if query.Limit > 0 && int64(query.Limit) < scanMessages {
		scanMessages = int64(query.Limit)
	}
	scanCost := float64(scanMessages) * costModel.CPUCostPerMessage
	dataMB := float64(scanMessages) * avgSizeKB / 1024.0
	scanCost += dataMB * costModel.NetworkCostPerMB

	scanStrategy := "full_scan"
	if partitions > 1 && query.WhereClause != nil {
		scanStrategy = "partition_pruned"
	}

	plan.Steps = append(plan.Steps, PlanStep{
		Order:       step,
		Operation:   "scan",
		Description: fmt.Sprintf("Scan topic %s (%s, %d partitions)", query.SourceTopic, scanStrategy, partitions),
		Cost:        scanCost,
		Selectivity: 1.0,
	})
	totalCost += scanCost
	step++

	// Step 2: Filter (if WHERE clause)
	if query.WhereClause != nil {
		selectivity = 0.3 // estimate 30% pass filter
		filterCost := float64(scanMessages) * costModel.CPUCostPerMessage * 0.1
		plan.Steps = append(plan.Steps, PlanStep{
			Order:       step,
			Operation:   "filter",
			Description: fmt.Sprintf("Filter: %s %s %s", query.WhereClause.Field, query.WhereClause.Operator, query.WhereClause.Value),
			Cost:        filterCost,
			Selectivity: selectivity,
		})
		totalCost += filterCost

		plan.Optimizations = append(plan.Optimizations, Optimization{
			Type:        "predicate_pushdown",
			Description: "Push filter predicate to partition scan to reduce data read",
			SavedCost:   scanCost * 0.2,
			Applied:     true,
		})
		step++
	}

	// Step 3: Aggregation (if GROUP BY or agg functions)
	if len(query.AggFunctions) > 0 || len(query.GroupBy) > 0 {
		aggCost := float64(scanMessages) * selectivity * costModel.CPUCostPerMessage * costModel.AggregationOverhead
		plan.Steps = append(plan.Steps, PlanStep{
			Order:       step,
			Operation:   "aggregate",
			Description: fmt.Sprintf("Aggregate: %d functions, group by %v", len(query.AggFunctions), query.GroupBy),
			Cost:        aggCost,
			Selectivity: 0.1, // aggregation reduces output significantly
		})
		totalCost += aggCost
		selectivity *= 0.1
		step++
	}

	// Step 4: Window (if window spec)
	if query.Window != nil {
		windowCost := float64(scanMessages) * selectivity * costModel.CPUCostPerMessage * costModel.WindowOverhead
		plan.Steps = append(plan.Steps, PlanStep{
			Order:       step,
			Operation:   "window",
			Description: fmt.Sprintf("Window: %s (%dms)", query.Window.Type, query.Window.SizeMs),
			Cost:        windowCost,
			Selectivity: selectivity,
		})
		totalCost += windowCost
		step++
	}

	// Step 5: Limit
	if query.Limit > 0 {
		plan.Steps = append(plan.Steps, PlanStep{
			Order:       step,
			Operation:   "limit",
			Description: fmt.Sprintf("Limit output to %d rows", query.Limit),
			Cost:        0.001,
			Selectivity: float64(query.Limit) / float64(scanMessages),
		})
		totalCost += 0.001
	}

	// Add parallelism optimization
	if partitions > 1 {
		plan.Optimizations = append(plan.Optimizations, Optimization{
			Type:        "parallelism",
			Description: fmt.Sprintf("Execute across %d partitions in parallel", partitions),
			SavedCost:   totalCost * float64(partitions-1) / float64(partitions),
			Applied:     true,
		})
	}

	// Sampling optimization for large datasets
	if scanMessages > 100000 {
		plan.Optimizations = append(plan.Optimizations, Optimization{
			Type:        "sampling",
			Description: "Use reservoir sampling for approximate aggregation (10% sample)",
			SavedCost:   totalCost * 0.7,
			Applied:     false,
		})
	}

	plan.Strategy = scanStrategy
	plan.EstimatedCost = totalCost
	plan.EstimatedMessages = scanMessages
	plan.EstimatedTimeMs = int64(totalCost * 100) // rough estimate
	plan.EstimatedDataMB = dataMB

	// Budget check
	plan.BudgetStatus = "within_budget"
	if budget != nil {
		if totalCost > budget.MaxCostPerQuery {
			plan.BudgetStatus = "over_budget"
		} else if budget.CurrentSpend+totalCost > budget.MaxCostPerHour*budget.AlertThreshold {
			plan.BudgetStatus = "warning"
		}
	}

	return plan, nil
}

// CreateBudget creates a cost budget.
func (e *Engine) CreateBudget(name string, maxPerQuery, maxPerHour, alertThreshold float64) *Budget {
	e.mu.Lock()
	defer e.mu.Unlock()

	if alertThreshold <= 0 || alertThreshold > 1.0 {
		alertThreshold = 0.8
	}

	budget := &Budget{
		ID:              randid.New("budget"),
		Name:            name,
		MaxCostPerQuery: maxPerQuery,
		MaxCostPerHour:  maxPerHour,
		AlertThreshold:  alertThreshold,
		Status:          "active",
		CreatedAt:       time.Now().UTC().Format(time.RFC3339),
	}

	if e.budgets == nil {
		e.budgets = make(map[string]*Budget)
	}
	e.budgets[budget.ID] = budget
	return budget
}

// GetBudget returns a budget by ID.
func (e *Engine) GetBudget(id string) (*Budget, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	b, ok := e.budgets[id]
	if !ok {
		return nil, fmt.Errorf("budget %q not found", id)
	}
	return b, nil
}

// ListBudgets returns all budgets.
func (e *Engine) ListBudgets() []Budget {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]Budget, 0, len(e.budgets))
	for _, b := range e.budgets {
		result = append(result, *b)
	}
	return result
}

// parseSQL is a simplified SQL parser for optimization purposes.
func (e *Engine) parseSQL(sql string) (*Query, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return nil, fmt.Errorf("empty SQL")
	}

	upper := strings.ToUpper(sql)
	if !strings.HasPrefix(upper, "SELECT") {
		return nil, fmt.Errorf("only SELECT queries are supported")
	}

	q := &Query{SQL: sql}

	// Extract FROM clause
	fromIdx := strings.Index(upper, "FROM")
	if fromIdx < 0 {
		return nil, fmt.Errorf("missing FROM clause")
	}
	// Parse topic from FROM clause
	rest := strings.TrimSpace(sql[fromIdx+4:])
	topicEnd := strings.IndexAny(rest, " \t\n")
	if topicEnd < 0 {
		q.SourceTopic = rest
	} else {
		q.SourceTopic = rest[:topicEnd]
	}

	// Extract select fields
	selectPart := strings.TrimSpace(sql[6:fromIdx])
	fields := strings.Split(selectPart, ",")
	for _, f := range fields {
		f = strings.TrimSpace(f)
		if f != "" {
			q.SelectFields = append(q.SelectFields, f)
			// Check for aggregation functions
			for _, agg := range []string{"COUNT", "SUM", "AVG", "MIN", "MAX"} {
				if strings.Contains(strings.ToUpper(f), agg+"(") {
					q.AggFunctions = append(q.AggFunctions, AggFunction{
						Function: agg,
						Field:    f,
						Alias:    f,
					})
				}
			}
		}
	}

	// Extract WHERE clause
	whereIdx := strings.Index(upper, "WHERE")
	if whereIdx > 0 {
		whereEnd := len(sql)
		for _, kw := range []string{"GROUP BY", "WINDOW", "LIMIT", "ORDER BY"} {
			idx := strings.Index(upper[whereIdx:], kw)
			if idx > 0 && whereIdx+idx < whereEnd {
				whereEnd = whereIdx + idx
			}
		}
		wherePart := strings.TrimSpace(sql[whereIdx+5 : whereEnd])
		q.WhereClause = parseSimpleWhere(wherePart)
	}

	// Extract GROUP BY
	groupIdx := strings.Index(upper, "GROUP BY")
	if groupIdx > 0 {
		groupEnd := len(sql)
		for _, kw := range []string{"WINDOW", "LIMIT", "ORDER BY", "HAVING"} {
			idx := strings.Index(upper[groupIdx:], kw)
			if idx > 0 && groupIdx+idx < groupEnd {
				groupEnd = groupIdx + idx
			}
		}
		groupPart := strings.TrimSpace(sql[groupIdx+8 : groupEnd])
		for _, g := range strings.Split(groupPart, ",") {
			g = strings.TrimSpace(g)
			if g != "" {
				q.GroupBy = append(q.GroupBy, g)
			}
		}
	}

	// Extract WINDOW
	windowIdx := strings.Index(upper, "WINDOW")
	if windowIdx > 0 {
		windowPart := strings.TrimSpace(sql[windowIdx+6:])
		q.Window = parseWindowSpec(windowPart)
	}

	// Extract LIMIT
	limitIdx := strings.Index(upper, "LIMIT")
	if limitIdx > 0 {
		limitPart := strings.TrimSpace(sql[limitIdx+5:])
		re := regexp.MustCompile(`^\d+`)
		if m := re.FindString(limitPart); m != "" {
			n, _ := strconv.Atoi(m)
			q.Limit = n
		}
	}

	return q, nil
}

func parseSimpleWhere(s string) *WhereClause {
	for _, op := range []string{">=", "<=", "!=", "=", ">", "<", "LIKE"} {
		idx := strings.Index(strings.ToUpper(s), op)
		if idx > 0 {
			return &WhereClause{
				Field:    strings.TrimSpace(s[:idx]),
				Operator: op,
				Value:    strings.TrimSpace(strings.Trim(s[idx+len(op):], "' \"")),
			}
		}
	}
	return nil
}

func parseWindowSpec(s string) *WindowSpec {
	upper := strings.ToUpper(s)
	w := &WindowSpec{SizeMs: 60000} // default 60s

	if strings.Contains(upper, "TUMBLING") {
		w.Type = "tumbling"
	} else if strings.Contains(upper, "SLIDING") {
		w.Type = "sliding"
		w.SlideMs = 10000
	} else if strings.Contains(upper, "SESSION") {
		w.Type = "session"
		w.GapMs = 30000
	} else {
		w.Type = "tumbling"
	}

	re := regexp.MustCompile(`(\d+)\s*(s|ms|m|h)`)
	if m := re.FindStringSubmatch(s); len(m) == 3 {
		n, _ := strconv.ParseInt(m[1], 10, 64)
		switch m[2] {
		case "ms":
			w.SizeMs = n
		case "s":
			w.SizeMs = n * 1000
		case "m":
			w.SizeMs = n * 60000
		case "h":
			w.SizeMs = n * 3600000
		}
	}

	return w
}
