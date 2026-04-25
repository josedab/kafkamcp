// Package costattribution provides real-time cost metering, cloud pricing
// integration, and budget alert management for Kafka resources.
package costattribution

import (
	"fmt"
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"
)

// LiveCost represents real-time cost metrics for a resource.
type LiveCost struct {
	ResourceType   string    `json:"resource_type"` // topic, consumer_group, connector
	ResourceName   string    `json:"resource_name"`
	BytesIn        int64     `json:"bytes_in"`
	BytesOut       int64     `json:"bytes_out"`
	MessagesIn     int64     `json:"messages_in"`
	MessagesOut    int64     `json:"messages_out"`
	StorageBytes   int64     `json:"storage_bytes"`
	ComputeCostHr  float64   `json:"compute_cost_per_hr"`
	StorageCostHr  float64   `json:"storage_cost_per_hr"`
	NetworkCostHr  float64   `json:"network_cost_per_hr"`
	TotalCostHr    float64   `json:"total_cost_per_hr"`
	TotalCostDay   float64   `json:"total_cost_per_day"`
	TotalCostMonth float64   `json:"total_cost_per_month"`
	LastUpdated    time.Time `json:"last_updated"`
}

// CostAllocation maps costs to teams, services, or projects.
type CostAllocation struct {
	ID              string           `json:"id"`
	Name            string           `json:"name"`
	Owner           string           `json:"owner"`
	Resources       []string         `json:"resources"`
	TotalCostMonth  float64          `json:"total_cost_per_month"`
	ResourceCosts   []ResourceCost   `json:"resource_costs"`
	Tags            map[string]string `json:"tags,omitempty"`
	UpdatedAt       time.Time        `json:"updated_at"`
}

// ResourceCost is the cost for a single resource in an allocation.
type ResourceCost struct {
	ResourceName   string  `json:"resource_name"`
	ResourceType   string  `json:"resource_type"`
	MonthlyCost    float64 `json:"monthly_cost"`
	PctOfTotal     float64 `json:"pct_of_total"`
}

// Budget defines a spending limit with alert thresholds.
type Budget struct {
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	Owner          string    `json:"owner"`
	MonthlyLimit   float64   `json:"monthly_limit"`
	CurrentSpend   float64   `json:"current_spend"`
	AlertThresholds []float64 `json:"alert_thresholds"` // e.g., [0.5, 0.8, 0.9, 1.0]
	AlertsFired    []string   `json:"alerts_fired,omitempty"`
	Resources      []string  `json:"resources,omitempty"`
	Status         string    `json:"status"` // ok, warning, critical, exceeded
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// PricingTier defines cloud provider pricing.
type PricingTier struct {
	Provider         string  `json:"provider"`
	StoragePerGBHr   float64 `json:"storage_per_gb_hr"`
	NetworkPerGBHr   float64 `json:"network_per_gb_hr"`
	ComputePerCPUHr  float64 `json:"compute_per_cpu_hr"`
	PartitionPerHr   float64 `json:"partition_per_hr"`
}

// Engine manages cost attribution and budget tracking.
type Engine struct {
	mu          sync.RWMutex
	meters      map[string]*LiveCost      // resource key -> live cost
	allocations map[string]*CostAllocation
	budgets     map[string]*Budget
	pricing     PricingTier
	logger      *slog.Logger
	nextID      int64
}

// NewEngine creates a new cost attribution engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		meters:      make(map[string]*LiveCost),
		allocations: make(map[string]*CostAllocation),
		budgets:     make(map[string]*Budget),
		pricing: PricingTier{
			Provider:        "aws-msk",
			StoragePerGBHr:  0.10 / 720,  // $0.10/GB/month
			NetworkPerGBHr:  0.02 / 720,
			ComputePerCPUHr: 0.21,
			PartitionPerHr:  0.0015 / 720,
		},
		logger: logger,
	}
}

// RecordMetrics records resource usage metrics for cost calculation.
func (e *Engine) RecordMetrics(resourceType, resourceName string, bytesIn, bytesOut, msgsIn, msgsOut, storageBytes int64) *LiveCost {
	e.mu.Lock()
	defer e.mu.Unlock()

	key := resourceType + ":" + resourceName
	meter, ok := e.meters[key]
	if !ok {
		meter = &LiveCost{
			ResourceType: resourceType,
			ResourceName: resourceName,
		}
		e.meters[key] = meter
	}

	meter.BytesIn += bytesIn
	meter.BytesOut += bytesOut
	meter.MessagesIn += msgsIn
	meter.MessagesOut += msgsOut
	meter.StorageBytes = storageBytes
	meter.LastUpdated = time.Now()

	// Calculate costs
	storageGB := float64(storageBytes) / (1024 * 1024 * 1024)
	networkGB := float64(bytesIn+bytesOut) / (1024 * 1024 * 1024)

	meter.StorageCostHr = storageGB * e.pricing.StoragePerGBHr
	meter.NetworkCostHr = networkGB * e.pricing.NetworkPerGBHr
	meter.ComputeCostHr = e.pricing.ComputePerCPUHr * 0.1 // fraction of CPU
	meter.TotalCostHr = meter.StorageCostHr + meter.NetworkCostHr + meter.ComputeCostHr
	meter.TotalCostDay = meter.TotalCostHr * 24
	meter.TotalCostMonth = meter.TotalCostHr * 720

	// Update budget alerts
	e.checkBudgetAlerts()

	return meter
}

// GetLiveCost returns live cost metrics for a resource.
func (e *Engine) GetLiveCost(resourceType, resourceName string) (*LiveCost, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	key := resourceType + ":" + resourceName
	meter, ok := e.meters[key]
	if !ok {
		return nil, fmt.Errorf("no cost data for %s:%s", resourceType, resourceName)
	}
	return meter, nil
}

// GetAllLiveCosts returns live costs for all tracked resources.
func (e *Engine) GetAllLiveCosts() []LiveCost {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []LiveCost
	for _, m := range e.meters {
		result = append(result, *m)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].TotalCostMonth > result[j].TotalCostMonth
	})
	return result
}

// CreateAllocation creates a cost allocation group.
func (e *Engine) CreateAllocation(name, owner string, resources []string, tags map[string]string) (*CostAllocation, error) {
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.nextID++
	id := fmt.Sprintf("alloc_%d", e.nextID)

	alloc := &CostAllocation{
		ID:        id,
		Name:      name,
		Owner:     owner,
		Resources: resources,
		Tags:      tags,
		UpdatedAt: time.Now(),
	}

	// Calculate costs for resources
	var totalCost float64
	for _, res := range resources {
		for key, meter := range e.meters {
			if key == res || meter.ResourceName == res {
				rc := ResourceCost{
					ResourceName: meter.ResourceName,
					ResourceType: meter.ResourceType,
					MonthlyCost:  math.Round(meter.TotalCostMonth*100) / 100,
				}
				totalCost += meter.TotalCostMonth
				alloc.ResourceCosts = append(alloc.ResourceCosts, rc)
			}
		}
	}

	alloc.TotalCostMonth = math.Round(totalCost*100) / 100

	// Calculate percentage
	for i := range alloc.ResourceCosts {
		if totalCost > 0 {
			alloc.ResourceCosts[i].PctOfTotal = math.Round(alloc.ResourceCosts[i].MonthlyCost/totalCost*10000) / 100
		}
	}

	e.allocations[id] = alloc
	return alloc, nil
}

// GetAllocations returns all cost allocations.
func (e *Engine) GetAllocations() []CostAllocation {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []CostAllocation
	for _, a := range e.allocations {
		result = append(result, *a)
	}
	return result
}

// CreateBudget creates a spending budget with alert thresholds.
func (e *Engine) CreateBudget(name, owner string, monthlyLimit float64, resources []string, thresholds []float64) (*Budget, error) {
	if name == "" || monthlyLimit <= 0 {
		return nil, fmt.Errorf("name and monthly_limit (> 0) are required")
	}

	if len(thresholds) == 0 {
		thresholds = []float64{0.5, 0.8, 0.9, 1.0}
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.nextID++
	id := fmt.Sprintf("budget_%d", e.nextID)

	budget := &Budget{
		ID:              id,
		Name:            name,
		Owner:           owner,
		MonthlyLimit:    monthlyLimit,
		AlertThresholds: thresholds,
		Resources:       resources,
		Status:          "ok",
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
	}

	// Calculate current spend
	var currentSpend float64
	for _, res := range resources {
		for key, meter := range e.meters {
			if key == res || meter.ResourceName == res {
				currentSpend += meter.TotalCostMonth
			}
		}
	}
	budget.CurrentSpend = math.Round(currentSpend*100) / 100
	budget.Status = e.budgetStatus(budget)

	e.budgets[id] = budget
	return budget, nil
}

// GetBudgets returns all budgets.
func (e *Engine) GetBudgets() []Budget {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []Budget
	for _, b := range e.budgets {
		result = append(result, *b)
	}
	return result
}

// SetPricing sets the cloud provider pricing tier.
func (e *Engine) SetPricing(pricing PricingTier) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pricing = pricing
}

func (e *Engine) checkBudgetAlerts() {
	for _, budget := range e.budgets {
		var currentSpend float64
		for _, res := range budget.Resources {
			for key, meter := range e.meters {
				if key == res || meter.ResourceName == res {
					currentSpend += meter.TotalCostMonth
				}
			}
		}
		budget.CurrentSpend = math.Round(currentSpend*100) / 100
		budget.Status = e.budgetStatus(budget)
		budget.UpdatedAt = time.Now()
	}
}

func (e *Engine) budgetStatus(budget *Budget) string {
	if budget.MonthlyLimit <= 0 {
		return "ok"
	}
	ratio := budget.CurrentSpend / budget.MonthlyLimit

	switch {
	case ratio >= 1.0:
		return "exceeded"
	case ratio >= 0.9:
		return "critical"
	case ratio >= 0.8:
		return "warning"
	default:
		return "ok"
	}
}
