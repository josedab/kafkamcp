package tenant

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// UsageRecord represents a single usage entry for rollup.
type UsageRecord struct {
	TenantID  string    `json:"tenant_id"`
	AgentID   string    `json:"agent_id"`
	ToolName  string    `json:"tool_name"`
	Cluster   string    `json:"cluster"`
	Messages  int64     `json:"messages"`
	Bytes     int64     `json:"bytes"`
	Timestamp time.Time `json:"timestamp"`
}

// UsageRollup represents aggregated usage over a period.
type UsageRollup struct {
	TenantID       string           `json:"tenant_id"`
	TenantName     string           `json:"tenant_name,omitempty"`
	Period         string           `json:"period"` // "hourly", "daily", "monthly"
	PeriodStart    string           `json:"period_start"`
	PeriodEnd      string           `json:"period_end"`
	TotalToolCalls int64            `json:"total_tool_calls"`
	TotalMessages  int64            `json:"total_messages"`
	TotalBytes     int64            `json:"total_bytes"`
	UniqueAgents   int              `json:"unique_agents"`
	ByTool         map[string]int64 `json:"by_tool"`
	ByAgent        map[string]int64 `json:"by_agent"`
}

// ChargebackReport represents a cost attribution report.
type ChargebackReport struct {
	TenantID     string        `json:"tenant_id"`
	TenantName   string        `json:"tenant_name"`
	Plan         string        `json:"plan"`
	Period       string        `json:"period"`
	PeriodStart  string        `json:"period_start"`
	PeriodEnd    string        `json:"period_end"`
	Usage        UsageRollup   `json:"usage"`
	Cost         CostBreakdown `json:"cost"`
	BudgetStatus BudgetStatus  `json:"budget_status"`
	Forecast     UsageForecast `json:"forecast"`
}

// CostBreakdown itemizes costs by category.
type CostBreakdown struct {
	ToolCallCost float64 `json:"tool_call_cost"`
	MessageCost  float64 `json:"message_cost"`
	StorageCost  float64 `json:"storage_cost"`
	TotalCost    float64 `json:"total_cost"`
	Currency     string  `json:"currency"`
}

// BudgetStatus tracks budget consumption.
type BudgetStatus struct {
	BudgetLimit     float64 `json:"budget_limit"`
	CurrentSpend    float64 `json:"current_spend"`
	RemainingBudget float64 `json:"remaining_budget"`
	UtilizationPct  float64 `json:"utilization_pct"`
	OnTrack         bool    `json:"on_track"`
}

// UsageForecast predicts future usage.
type UsageForecast struct {
	PredictedToolCalls int64   `json:"predicted_tool_calls"`
	PredictedMessages  int64   `json:"predicted_messages"`
	PredictedCost      float64 `json:"predicted_cost"`
	ForecastPeriod     string  `json:"forecast_period"`
}

// PricingTier defines cost per unit for billing.
type PricingTier struct {
	Plan             string  `json:"plan"`
	CostPerToolCall  float64 `json:"cost_per_tool_call"`
	CostPerMessage   float64 `json:"cost_per_message"`
	CostPerGBStorage float64 `json:"cost_per_gb_storage"`
	MonthlyBase      float64 `json:"monthly_base"`
}

var defaultPricing = map[string]PricingTier{
	"free":       {Plan: "free", CostPerToolCall: 0, CostPerMessage: 0, CostPerGBStorage: 0, MonthlyBase: 0},
	"pro":        {Plan: "pro", CostPerToolCall: 0.001, CostPerMessage: 0.0001, CostPerGBStorage: 0.10, MonthlyBase: 49.0},
	"enterprise": {Plan: "enterprise", CostPerToolCall: 0.0005, CostPerMessage: 0.00005, CostPerGBStorage: 0.05, MonthlyBase: 299.0},
}

// UsageTracker records and queries fine-grained usage data.
type UsageTracker struct {
	mu         sync.RWMutex
	records    []UsageRecord
	maxRecords int
}

// NewUsageTracker creates a new usage tracker.
func NewUsageTracker() *UsageTracker {
	return &UsageTracker{
		maxRecords: 100000,
	}
}

// Record adds a usage record.
func (ut *UsageTracker) Record(r UsageRecord) {
	ut.mu.Lock()
	defer ut.mu.Unlock()

	if r.Timestamp.IsZero() {
		r.Timestamp = time.Now()
	}
	ut.records = append(ut.records, r)
	if len(ut.records) > ut.maxRecords {
		ut.records = ut.records[len(ut.records)-ut.maxRecords:]
	}
}

// GenerateRollup creates an aggregated usage rollup for a tenant.
func (ut *UsageTracker) GenerateRollup(tenantID, period string, start, end time.Time) *UsageRollup {
	ut.mu.RLock()
	defer ut.mu.RUnlock()

	rollup := &UsageRollup{
		TenantID:    tenantID,
		Period:      period,
		PeriodStart: start.UTC().Format(time.RFC3339),
		PeriodEnd:   end.UTC().Format(time.RFC3339),
		ByTool:      make(map[string]int64),
		ByAgent:     make(map[string]int64),
	}

	agentSet := make(map[string]bool)
	for _, r := range ut.records {
		if r.TenantID != tenantID {
			continue
		}
		if r.Timestamp.Before(start) || r.Timestamp.After(end) {
			continue
		}

		rollup.TotalToolCalls++
		rollup.TotalMessages += r.Messages
		rollup.TotalBytes += r.Bytes
		rollup.ByTool[r.ToolName]++
		rollup.ByAgent[r.AgentID]++
		agentSet[r.AgentID] = true
	}

	rollup.UniqueAgents = len(agentSet)
	return rollup
}

// GenerateChargebackReport creates a cost attribution report.
func (ut *UsageTracker) GenerateChargebackReport(tenant *Tenant, period string, start, end time.Time) *ChargebackReport {
	rollup := ut.GenerateRollup(tenant.ID, period, start, end)
	rollup.TenantName = tenant.Name

	pricing, ok := defaultPricing[tenant.Plan]
	if !ok {
		pricing = defaultPricing["free"]
	}

	cost := CostBreakdown{
		ToolCallCost: float64(rollup.TotalToolCalls) * pricing.CostPerToolCall,
		MessageCost:  float64(rollup.TotalMessages) * pricing.CostPerMessage,
		StorageCost:  float64(rollup.TotalBytes) / (1024 * 1024 * 1024) * pricing.CostPerGBStorage,
		Currency:     "USD",
	}
	cost.TotalCost = cost.ToolCallCost + cost.MessageCost + cost.StorageCost + pricing.MonthlyBase

	// Budget estimation (monthly budget = 2x base price)
	budgetLimit := pricing.MonthlyBase * 2
	if budgetLimit == 0 {
		budgetLimit = 100 // default for free tier
	}
	budget := BudgetStatus{
		BudgetLimit:     budgetLimit,
		CurrentSpend:    cost.TotalCost,
		RemainingBudget: budgetLimit - cost.TotalCost,
		UtilizationPct:  (cost.TotalCost / budgetLimit) * 100,
		OnTrack:         cost.TotalCost < budgetLimit*0.8,
	}

	// Simple forecast: extrapolate current usage to end of period
	daysSoFar := time.Since(start).Hours() / 24
	if daysSoFar < 1 {
		daysSoFar = 1
	}
	daysInPeriod := end.Sub(start).Hours() / 24
	multiplier := daysInPeriod / daysSoFar

	forecast := UsageForecast{
		PredictedToolCalls: int64(float64(rollup.TotalToolCalls) * multiplier),
		PredictedMessages:  int64(float64(rollup.TotalMessages) * multiplier),
		PredictedCost:      cost.TotalCost * multiplier,
		ForecastPeriod:     period,
	}

	return &ChargebackReport{
		TenantID:     tenant.ID,
		TenantName:   tenant.Name,
		Plan:         tenant.Plan,
		Period:       period,
		PeriodStart:  start.UTC().Format(time.RFC3339),
		PeriodEnd:    end.UTC().Format(time.RFC3339),
		Usage:        *rollup,
		Cost:         cost,
		BudgetStatus: budget,
		Forecast:     forecast,
	}
}

// ExportCSV generates a CSV string of usage records for a tenant.
func (ut *UsageTracker) ExportCSV(tenantID string, start, end time.Time) (string, error) {
	ut.mu.RLock()
	defer ut.mu.RUnlock()

	var buf strings.Builder
	w := csv.NewWriter(&buf)
	w.Write([]string{"timestamp", "tenant_id", "agent_id", "tool_name", "cluster", "messages", "bytes"})

	for _, r := range ut.records {
		if r.TenantID != tenantID {
			continue
		}
		if r.Timestamp.Before(start) || r.Timestamp.After(end) {
			continue
		}
		w.Write([]string{
			r.Timestamp.UTC().Format(time.RFC3339),
			r.TenantID,
			r.AgentID,
			r.ToolName,
			r.Cluster,
			fmt.Sprintf("%d", r.Messages),
			fmt.Sprintf("%d", r.Bytes),
		})
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return "", fmt.Errorf("flushing CSV: %w", err)
	}
	return buf.String(), nil
}

// ExportJSON generates a JSON string of usage records for a tenant.
func (ut *UsageTracker) ExportJSON(tenantID string, start, end time.Time) (string, error) {
	ut.mu.RLock()
	defer ut.mu.RUnlock()

	var filtered []UsageRecord
	for _, r := range ut.records {
		if r.TenantID != tenantID {
			continue
		}
		if r.Timestamp.Before(start) || r.Timestamp.After(end) {
			continue
		}
		filtered = append(filtered, r)
	}

	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Timestamp.Before(filtered[j].Timestamp)
	})

	data, err := json.MarshalIndent(filtered, "", "  ")
	if err != nil {
		return "", err
	}
	return string(data), nil
}
