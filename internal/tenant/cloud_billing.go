package tenant

import (
	"fmt"
	"time"
)

// BillingInfo holds tenant billing information.
type BillingInfo struct {
	StripeCustomerID     string    `json:"stripe_customer_id,omitempty"`
	StripeSubscriptionID string    `json:"stripe_subscription_id,omitempty"`
	PlanID               string    `json:"plan_id"`
	BillingEmail         string    `json:"billing_email"`
	CurrentPeriodStart   time.Time `json:"current_period_start"`
	CurrentPeriodEnd     time.Time `json:"current_period_end"`
	Status               string    `json:"status"` // "active", "past_due", "canceled"
}

// UsageBillingRecord records usage for billing purposes.
type UsageBillingRecord struct {
	TenantID         string    `json:"tenant_id"`
	PeriodStart      time.Time `json:"period_start"`
	PeriodEnd        time.Time `json:"period_end"`
	ToolCalls        int64     `json:"tool_calls"`
	MessagesConsumed int64     `json:"messages_consumed"`
	MessagesProduced int64     `json:"messages_produced"`
	BytesTransferred int64     `json:"bytes_transferred"`
	ComputedCostUSD  float64   `json:"computed_cost_usd"`
}

// SetupBilling configures billing for a tenant.
func (cp *ControlPlane) SetupBilling(tenantID string, billing BillingInfo) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if _, err := cp.tenantMgr.GetTenant(tenantID); err != nil {
		return err
	}

	cp.billing[tenantID] = &billing
	return nil
}

// GetBilling returns billing info for a tenant.
func (cp *ControlPlane) GetBilling(tenantID string) (*BillingInfo, error) {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	b, ok := cp.billing[tenantID]
	if !ok {
		return nil, fmt.Errorf("no billing info for tenant %q", tenantID)
	}
	return b, nil
}

// RecordUsagePeriod records a billing usage period.
func (cp *ControlPlane) RecordUsagePeriod(record UsageBillingRecord) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	// Compute cost based on plan
	record.ComputedCostUSD = cp.computeCost(record)
	cp.usageRecords = append(cp.usageRecords, record)

	if len(cp.usageRecords) > 10000 {
		cp.usageRecords = cp.usageRecords[len(cp.usageRecords)-10000:]
	}
}

func (cp *ControlPlane) computeCost(record UsageBillingRecord) float64 {
	// Usage-based pricing
	const (
		costPerToolCall = 0.0001  // $0.01 per 100 calls
		costPerMessage  = 0.00001 // $0.01 per 1000 messages
		costPerGB       = 0.10    // $0.10 per GB transferred
	)

	cost := float64(record.ToolCalls) * costPerToolCall
	cost += float64(record.MessagesConsumed+record.MessagesProduced) * costPerMessage
	cost += float64(record.BytesTransferred) / (1024 * 1024 * 1024) * costPerGB

	return cost
}

// GetUsageRecords returns usage records for a tenant.
func (cp *ControlPlane) GetUsageRecords(tenantID string) []UsageBillingRecord {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	var result []UsageBillingRecord
	for _, r := range cp.usageRecords {
		if r.TenantID == tenantID {
			result = append(result, r)
		}
	}
	return result
}
