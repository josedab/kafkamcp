package finops

import (
	"fmt"
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"
)

// CostSnapshot captures cost data at a point in time.
type CostSnapshot struct {
	Timestamp       time.Time       `json:"timestamp"`
	Provider        string          `json:"provider"`
	TotalMonthly    float64         `json:"total_monthly"`
	BrokerCost      float64         `json:"broker_cost"`
	StorageCost     float64         `json:"storage_cost"`
	NetworkCost     float64         `json:"network_cost"`
	TopicBreakdown  []TopicCost     `json:"topic_breakdown,omitempty"`
	TrendDirection  string          `json:"trend_direction"` // up, down, stable
	ChangePercent   float64         `json:"change_percent"`
}

// TopicCost is the cost of a single topic.
type TopicCost struct {
	Name            string  `json:"name"`
	MonthlyCost     float64 `json:"monthly_cost"`
	StorageGB       float64 `json:"storage_gb"`
	Partitions      int     `json:"partitions"`
	MsgsPerSec      float64 `json:"msgs_per_sec"`
	CostPerMessage  float64 `json:"cost_per_message"`
	Utilization     float64 `json:"utilization"` // 0-100%
}

// RightsizeRecommendation suggests optimization changes.
type RightsizeRecommendation struct {
	Type            string  `json:"type"` // reduce_partitions, reduce_replication, compress, archive, delete
	Topic           string  `json:"topic"`
	Description     string  `json:"description"`
	CurrentCost     float64 `json:"current_cost_month"`
	ProjectedCost   float64 `json:"projected_cost_month"`
	Savings         float64 `json:"savings_month"`
	SavingsPercent  float64 `json:"savings_percent"`
	Impact          string  `json:"impact"` // low, medium, high
	Effort          string  `json:"effort"` // low, medium, high
	Priority        int     `json:"priority"` // 1=highest
}

// SpendForecast projects future spending.
type SpendForecast struct {
	CurrentMonthly  float64          `json:"current_monthly"`
	Projections     []ForecastPoint  `json:"projections"`
	GrowthRate      float64          `json:"growth_rate_percent"`
	Confidence      float64          `json:"confidence"` // 0-1
	Assumptions     []string         `json:"assumptions"`
}

// ForecastPoint is a projected cost at a future date.
type ForecastPoint struct {
	Month     string  `json:"month"`
	Projected float64 `json:"projected_cost"`
	LowerBound float64 `json:"lower_bound"`
	UpperBound float64 `json:"upper_bound"`
}

// BudgetAlert defines a spending alert.
type BudgetAlert struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Threshold   float64 `json:"threshold"`
	Current     float64 `json:"current"`
	Percentage  float64 `json:"percentage"`
	Status      string  `json:"status"` // ok, warning, exceeded
}

// Dashboard manages cost intelligence and FinOps.
type Dashboard struct {
	mu          sync.RWMutex
	snapshots   []CostSnapshot
	budgets     map[string]*BudgetAlert
	topicCosts  map[string]*TopicCost
	logger      *slog.Logger
	nextID      int64
}

// NewDashboard creates a new FinOps dashboard.
func NewDashboard(logger *slog.Logger) *Dashboard {
	return &Dashboard{
		snapshots:  make([]CostSnapshot, 0),
		budgets:    make(map[string]*BudgetAlert),
		topicCosts: make(map[string]*TopicCost),
		logger:     logger,
	}
}

// RecordTopicCost records or updates cost data for a topic.
func (d *Dashboard) RecordTopicCost(name string, monthlyCost, storageGB float64, partitions int, msgsPerSec float64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	costPerMsg := 0.0
	if msgsPerSec > 0 {
		costPerMsg = monthlyCost / (msgsPerSec * 86400 * 30)
	}

	utilization := math.Min(msgsPerSec/100, 1.0) * 100 // normalized to 100 msgs/s

	d.topicCosts[name] = &TopicCost{
		Name:           name,
		MonthlyCost:    monthlyCost,
		StorageGB:      storageGB,
		Partitions:     partitions,
		MsgsPerSec:     msgsPerSec,
		CostPerMessage: costPerMsg,
		Utilization:    utilization,
	}
}

// TakeSnapshot captures current cost state.
func (d *Dashboard) TakeSnapshot(provider string) *CostSnapshot {
	d.mu.Lock()
	defer d.mu.Unlock()

	snapshot := CostSnapshot{
		Timestamp: time.Now(),
		Provider:  provider,
	}

	for _, tc := range d.topicCosts {
		snapshot.TotalMonthly += tc.MonthlyCost
		snapshot.StorageCost += tc.StorageGB * 0.10
		snapshot.TopicBreakdown = append(snapshot.TopicBreakdown, *tc)
	}
	snapshot.BrokerCost = snapshot.TotalMonthly * 0.4
	snapshot.NetworkCost = snapshot.TotalMonthly * 0.2

	// Determine trend
	if len(d.snapshots) > 0 {
		prev := d.snapshots[len(d.snapshots)-1]
		if prev.TotalMonthly > 0 {
			change := ((snapshot.TotalMonthly - prev.TotalMonthly) / prev.TotalMonthly) * 100
			snapshot.ChangePercent = math.Round(change*100) / 100
			switch {
			case change > 5:
				snapshot.TrendDirection = "up"
			case change < -5:
				snapshot.TrendDirection = "down"
			default:
				snapshot.TrendDirection = "stable"
			}
		}
	} else {
		snapshot.TrendDirection = "stable"
	}

	sort.Slice(snapshot.TopicBreakdown, func(i, j int) bool {
		return snapshot.TopicBreakdown[i].MonthlyCost > snapshot.TopicBreakdown[j].MonthlyCost
	})

	d.snapshots = append(d.snapshots, snapshot)
	if len(d.snapshots) > 365 {
		d.snapshots = d.snapshots[len(d.snapshots)-365:]
	}

	return &snapshot
}

// GetRightsizeRecommendations analyzes topics and returns optimization recommendations.
func (d *Dashboard) GetRightsizeRecommendations() []RightsizeRecommendation {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var recs []RightsizeRecommendation
	priority := 1

	for _, tc := range d.topicCosts {
		// Underutilized topics
		if tc.Utilization < 10 && tc.MonthlyCost > 1 {
			savings := tc.MonthlyCost * 0.5
			recs = append(recs, RightsizeRecommendation{
				Type:           "archive",
				Topic:          tc.Name,
				Description:    fmt.Sprintf("Topic %q is %.0f%% utilized — consider archiving or reducing retention", tc.Name, tc.Utilization),
				CurrentCost:    tc.MonthlyCost,
				ProjectedCost:  tc.MonthlyCost - savings,
				Savings:        savings,
				SavingsPercent: 50,
				Impact:         "low",
				Effort:         "low",
				Priority:       priority,
			})
			priority++
		}

		// Over-partitioned
		if tc.Partitions > 12 && tc.MsgsPerSec < 10 {
			savings := tc.MonthlyCost * 0.3
			recs = append(recs, RightsizeRecommendation{
				Type:           "reduce_partitions",
				Topic:          tc.Name,
				Description:    fmt.Sprintf("Topic %q has %d partitions but only %.1f msgs/s — reduce partitions", tc.Name, tc.Partitions, tc.MsgsPerSec),
				CurrentCost:    tc.MonthlyCost,
				ProjectedCost:  tc.MonthlyCost - savings,
				Savings:        savings,
				SavingsPercent: 30,
				Impact:         "medium",
				Effort:         "medium",
				Priority:       priority,
			})
			priority++
		}

		// High storage topics
		if tc.StorageGB > 100 {
			savings := tc.StorageGB * 0.10 * 0.5
			recs = append(recs, RightsizeRecommendation{
				Type:           "compress",
				Topic:          tc.Name,
				Description:    fmt.Sprintf("Topic %q uses %.1f GB storage — enable compression to reduce by ~50%%", tc.Name, tc.StorageGB),
				CurrentCost:    tc.MonthlyCost,
				ProjectedCost:  tc.MonthlyCost - savings,
				Savings:        savings,
				SavingsPercent: math.Round(savings/tc.MonthlyCost*100*100) / 100,
				Impact:         "low",
				Effort:         "low",
				Priority:       priority,
			})
			priority++
		}
	}

	sort.Slice(recs, func(i, j int) bool { return recs[i].Savings > recs[j].Savings })
	for i := range recs {
		recs[i].Priority = i + 1
	}

	return recs
}

// ForecastSpend projects future spending based on historical trends.
func (d *Dashboard) ForecastSpend(months int) *SpendForecast {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if months <= 0 {
		months = 6
	}

	var currentMonthly float64
	for _, tc := range d.topicCosts {
		currentMonthly += tc.MonthlyCost
	}

	// Calculate growth rate from snapshots
	growthRate := 5.0 // default 5% monthly growth
	confidence := 0.5

	if len(d.snapshots) >= 2 {
		first := d.snapshots[0].TotalMonthly
		last := d.snapshots[len(d.snapshots)-1].TotalMonthly
		if first > 0 {
			totalGrowth := (last - first) / first
			growthRate = totalGrowth / float64(len(d.snapshots)) * 100
			confidence = math.Min(float64(len(d.snapshots))/12.0, 0.95)
		}
	}

	forecast := &SpendForecast{
		CurrentMonthly: currentMonthly,
		GrowthRate:     math.Round(growthRate*100) / 100,
		Confidence:     confidence,
		Assumptions: []string{
			fmt.Sprintf("%.1f%% monthly growth rate based on historical data", growthRate),
			"No major infrastructure changes",
			"Current provider pricing remains stable",
		},
	}

	now := time.Now()
	for i := 1; i <= months; i++ {
		month := now.AddDate(0, i, 0)
		projected := currentMonthly * math.Pow(1+growthRate/100, float64(i))
		margin := projected * 0.15 * float64(i) / float64(months)

		forecast.Projections = append(forecast.Projections, ForecastPoint{
			Month:      month.Format("2006-01"),
			Projected:  math.Round(projected*100) / 100,
			LowerBound: math.Round((projected-margin)*100) / 100,
			UpperBound: math.Round((projected+margin)*100) / 100,
		})
	}

	return forecast
}

// SetBudget creates or updates a budget alert.
func (d *Dashboard) SetBudget(name string, threshold float64) (*BudgetAlert, error) {
	if name == "" {
		return nil, fmt.Errorf("budget name is required")
	}
	if threshold <= 0 {
		return nil, fmt.Errorf("threshold must be positive")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	d.nextID++
	id := fmt.Sprintf("budget_%d", d.nextID)

	var current float64
	for _, tc := range d.topicCosts {
		current += tc.MonthlyCost
	}

	pct := 0.0
	if threshold > 0 {
		pct = (current / threshold) * 100
	}

	status := "ok"
	if pct >= 100 {
		status = "exceeded"
	} else if pct >= 80 {
		status = "warning"
	}

	alert := &BudgetAlert{
		ID:         id,
		Name:       name,
		Threshold:  threshold,
		Current:    current,
		Percentage: math.Round(pct*100) / 100,
		Status:     status,
	}

	d.budgets[id] = alert
	return alert, nil
}

// GetBudgets returns all budget alerts.
func (d *Dashboard) GetBudgets() []BudgetAlert {
	d.mu.RLock()
	defer d.mu.RUnlock()

	result := make([]BudgetAlert, 0, len(d.budgets))
	for _, b := range d.budgets {
		result = append(result, *b)
	}
	return result
}

// GetSnapshots returns recent cost snapshots.
func (d *Dashboard) GetSnapshots(limit int) []CostSnapshot {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if limit <= 0 || limit > len(d.snapshots) {
		limit = len(d.snapshots)
	}
	return d.snapshots[len(d.snapshots)-limit:]
}

// GetSummary returns a high-level cost summary.
func (d *Dashboard) GetSummary() map[string]any {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var total float64
	var topicCount int
	var totalStorage float64
	for _, tc := range d.topicCosts {
		total += tc.MonthlyCost
		topicCount++
		totalStorage += tc.StorageGB
	}

	recs := d.GetRightsizeRecommendations()
	var totalSavings float64
	for _, r := range recs {
		totalSavings += r.Savings
	}

	return map[string]any{
		"total_monthly_cost":   math.Round(total*100) / 100,
		"total_annual_cost":    math.Round(total*12*100) / 100,
		"topic_count":          topicCount,
		"total_storage_gb":     math.Round(totalStorage*100) / 100,
		"optimization_count":   len(recs),
		"potential_savings":    math.Round(totalSavings*100) / 100,
		"savings_percent":     safePercent(totalSavings, total),
		"budget_alerts":       len(d.budgets),
		"snapshot_count":      len(d.snapshots),
	}
}

func safePercent(part, total float64) float64 {
	if total == 0 {
		return 0
	}
	return math.Round(part/total*100*100) / 100
}
