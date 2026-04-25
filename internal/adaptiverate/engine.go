// Package adaptiverate provides ML-driven rate limiting that learns per-agent
// consumption patterns, predicts burst behavior, and auto-adjusts quotas.
package adaptiverate

import (
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"
)

// AgentProfile tracks consumption patterns for an agent.
type AgentProfile struct {
	AgentID          string           `json:"agent_id"`
	WindowSize       time.Duration    `json:"-"`
	Samples          []Sample         `json:"-"`
	CurrentRate      float64          `json:"current_rate_per_min"`
	AvgRate          float64          `json:"avg_rate_per_min"`
	PeakRate         float64          `json:"peak_rate_per_min"`
	StdDev           float64          `json:"std_dev"`
	BaseQuota        int              `json:"base_quota_per_min"`
	DynamicQuota     int              `json:"dynamic_quota_per_min"`
	Credits          float64          `json:"burst_credits"`
	MaxCredits       float64          `json:"max_credits"`
	CreditRefillRate float64          `json:"credit_refill_rate_per_sec"`
	LastRequest      time.Time        `json:"last_request"`
	TotalRequests    int64            `json:"total_requests"`
	DeniedRequests   int64            `json:"denied_requests"`
	BurstEvents      int              `json:"burst_events"`
	Warnings         []string         `json:"warnings,omitempty"`
}

// Sample represents a single rate measurement.
type Sample struct {
	Timestamp time.Time
	Count     int
}

// QuotaStatus describes the current quota state for an agent.
type QuotaStatus struct {
	AgentID         string  `json:"agent_id"`
	BaseQuota       int     `json:"base_quota_per_min"`
	DynamicQuota    int     `json:"dynamic_quota_per_min"`
	CurrentRate     float64 `json:"current_rate_per_min"`
	AvgRate         float64 `json:"avg_rate_per_min"`
	PeakRate        float64 `json:"peak_rate_per_min"`
	BurstCredits    float64 `json:"burst_credits_remaining"`
	MaxCredits      float64 `json:"max_credits"`
	Utilization     float64 `json:"utilization_pct"`
	TotalRequests   int64   `json:"total_requests"`
	DeniedRequests  int64   `json:"denied_requests"`
	DenyRate        float64 `json:"deny_rate_pct"`
	Warnings        []string `json:"warnings,omitempty"`
	Recommendation  string  `json:"recommendation,omitempty"`
}

// QuotaRequest represents a request to adjust an agent's quota.
type QuotaRequest struct {
	AgentID    string `json:"agent_id"`
	Action     string `json:"action"` // increase, decrease, reset, grant_credits
	Amount     int    `json:"amount,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

// QuotaResponse is the result of a quota adjustment.
type QuotaResponse struct {
	AgentID      string `json:"agent_id"`
	Action       string `json:"action"`
	PreviousQuota int   `json:"previous_quota"`
	NewQuota     int    `json:"new_quota"`
	Status       string `json:"status"`
	Message      string `json:"message"`
}

// Engine manages adaptive rate limiting.
type Engine struct {
	mu       sync.RWMutex
	profiles map[string]*AgentProfile
	logger   *slog.Logger
}

// NewEngine creates a new adaptive rate engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		profiles: make(map[string]*AgentProfile),
		logger:   logger,
	}
}

// RecordRequest records a request from an agent and updates the profile.
func (e *Engine) RecordRequest(agentID string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	profile := e.getOrCreateProfile(agentID)
	now := time.Now()

	profile.TotalRequests++
	profile.LastRequest = now
	profile.Samples = append(profile.Samples, Sample{Timestamp: now, Count: 1})

	// Keep last 10 minutes of samples
	cutoff := now.Add(-10 * time.Minute)
	var kept []Sample
	for _, s := range profile.Samples {
		if s.Timestamp.After(cutoff) {
			kept = append(kept, s)
		}
	}
	profile.Samples = kept

	// Recalculate rates
	e.updateRates(profile)

	// Auto-adjust quota based on patterns
	e.autoAdjustQuota(profile)

	// Refill burst credits
	e.refillCredits(profile, now)
}

// Allow checks if an agent's request should be allowed under adaptive limits.
func (e *Engine) Allow(agentID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	profile := e.getOrCreateProfile(agentID)

	if profile.CurrentRate <= float64(profile.DynamicQuota) {
		return true
	}

	// Use burst credits
	if profile.Credits >= 1.0 {
		profile.Credits--
		return true
	}

	profile.DeniedRequests++
	return false
}

// GetStatus returns the quota status for an agent.
func (e *Engine) GetStatus(agentID string) *QuotaStatus {
	e.mu.RLock()
	defer e.mu.RUnlock()

	profile, ok := e.profiles[agentID]
	if !ok {
		return &QuotaStatus{AgentID: agentID, BaseQuota: 60, DynamicQuota: 60}
	}

	utilization := 0.0
	if profile.DynamicQuota > 0 {
		utilization = (profile.CurrentRate / float64(profile.DynamicQuota)) * 100
	}

	denyRate := 0.0
	if profile.TotalRequests > 0 {
		denyRate = (float64(profile.DeniedRequests) / float64(profile.TotalRequests)) * 100
	}

	status := &QuotaStatus{
		AgentID:        agentID,
		BaseQuota:      profile.BaseQuota,
		DynamicQuota:   profile.DynamicQuota,
		CurrentRate:    math.Round(profile.CurrentRate*100) / 100,
		AvgRate:        math.Round(profile.AvgRate*100) / 100,
		PeakRate:       math.Round(profile.PeakRate*100) / 100,
		BurstCredits:   math.Round(profile.Credits*100) / 100,
		MaxCredits:     profile.MaxCredits,
		Utilization:    math.Round(utilization*100) / 100,
		TotalRequests:  profile.TotalRequests,
		DeniedRequests: profile.DeniedRequests,
		DenyRate:       math.Round(denyRate*100) / 100,
		Warnings:       profile.Warnings,
	}

	// Add recommendation
	if utilization > 90 {
		status.Recommendation = "Agent is near quota limit. Consider requesting a quota increase."
	} else if utilization < 20 && profile.TotalRequests > 100 {
		status.Recommendation = "Agent is significantly under quota. Quota could be reduced to free up cluster resources."
	}

	return status
}

// AdjustQuota processes a quota adjustment request.
func (e *Engine) AdjustQuota(req QuotaRequest) (*QuotaResponse, error) {
	if req.AgentID == "" {
		return nil, fmt.Errorf("agent_id is required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	profile := e.getOrCreateProfile(req.AgentID)
	prevQuota := profile.DynamicQuota

	switch req.Action {
	case "increase":
		amount := req.Amount
		if amount <= 0 {
			amount = profile.BaseQuota / 4
		}
		profile.DynamicQuota += amount
		if profile.DynamicQuota > profile.BaseQuota*5 {
			profile.DynamicQuota = profile.BaseQuota * 5
		}
	case "decrease":
		amount := req.Amount
		if amount <= 0 {
			amount = profile.BaseQuota / 4
		}
		profile.DynamicQuota -= amount
		if profile.DynamicQuota < profile.BaseQuota/4 {
			profile.DynamicQuota = profile.BaseQuota / 4
		}
	case "reset":
		profile.DynamicQuota = profile.BaseQuota
		profile.Credits = profile.MaxCredits
	case "grant_credits":
		amount := req.Amount
		if amount <= 0 {
			amount = int(profile.MaxCredits / 2)
		}
		profile.Credits += float64(amount)
		if profile.Credits > profile.MaxCredits*2 {
			profile.Credits = profile.MaxCredits * 2
		}
	default:
		return nil, fmt.Errorf("action must be: increase, decrease, reset, or grant_credits")
	}

	return &QuotaResponse{
		AgentID:       req.AgentID,
		Action:        req.Action,
		PreviousQuota: prevQuota,
		NewQuota:      profile.DynamicQuota,
		Status:        "applied",
		Message:       fmt.Sprintf("Quota adjusted from %d to %d per minute", prevQuota, profile.DynamicQuota),
	}, nil
}

// AllStatus returns status for all agents.
func (e *Engine) AllStatus() []QuotaStatus {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []QuotaStatus
	for agentID := range e.profiles {
		// Copy unlock/relock is not needed since we iterate a copy
		profile := e.profiles[agentID]
		utilization := 0.0
		if profile.DynamicQuota > 0 {
			utilization = (profile.CurrentRate / float64(profile.DynamicQuota)) * 100
		}
		result = append(result, QuotaStatus{
			AgentID:      agentID,
			BaseQuota:    profile.BaseQuota,
			DynamicQuota: profile.DynamicQuota,
			CurrentRate:  math.Round(profile.CurrentRate*100) / 100,
			BurstCredits: math.Round(profile.Credits*100) / 100,
		})
		_ = utilization
	}
	return result
}

func (e *Engine) getOrCreateProfile(agentID string) *AgentProfile {
	if profile, ok := e.profiles[agentID]; ok {
		return profile
	}
	profile := &AgentProfile{
		AgentID:          agentID,
		BaseQuota:        60,
		DynamicQuota:     60,
		Credits:          10,
		MaxCredits:       20,
		CreditRefillRate: 0.1,
	}
	e.profiles[agentID] = profile
	return profile
}

func (e *Engine) updateRates(profile *AgentProfile) {
	if len(profile.Samples) == 0 {
		return
	}

	// Calculate current rate (requests in last minute)
	now := time.Now()
	minuteAgo := now.Add(-1 * time.Minute)
	var recentCount int
	for _, s := range profile.Samples {
		if s.Timestamp.After(minuteAgo) {
			recentCount += s.Count
		}
	}
	profile.CurrentRate = float64(recentCount)

	// Average rate
	if len(profile.Samples) > 0 {
		totalCount := 0
		for _, s := range profile.Samples {
			totalCount += s.Count
		}
		duration := now.Sub(profile.Samples[0].Timestamp).Minutes()
		if duration > 0 {
			profile.AvgRate = float64(totalCount) / duration
		}
	}

	// Peak rate
	if profile.CurrentRate > profile.PeakRate {
		profile.PeakRate = profile.CurrentRate
	}

	// Standard deviation
	var sumSq float64
	for _, s := range profile.Samples {
		diff := float64(s.Count) - profile.AvgRate/60
		sumSq += diff * diff
	}
	if len(profile.Samples) > 1 {
		profile.StdDev = math.Sqrt(sumSq / float64(len(profile.Samples)-1))
	}
}

func (e *Engine) autoAdjustQuota(profile *AgentProfile) {
	profile.Warnings = nil

	// Detect burst pattern
	if profile.CurrentRate > float64(profile.DynamicQuota)*0.8 {
		profile.BurstEvents++
		if profile.BurstEvents > 3 {
			// Auto-increase by 20%
			newQuota := int(float64(profile.DynamicQuota) * 1.2)
			if newQuota <= profile.BaseQuota*3 {
				profile.DynamicQuota = newQuota
				profile.Warnings = append(profile.Warnings, "Quota auto-increased due to sustained high usage")
			} else {
				profile.Warnings = append(profile.Warnings, "Agent approaching maximum quota ceiling")
			}
		}
	} else {
		profile.BurstEvents = 0
	}

	// Detect low usage
	if profile.TotalRequests > 1000 && profile.CurrentRate < float64(profile.DynamicQuota)*0.1 {
		newQuota := int(float64(profile.DynamicQuota) * 0.9)
		if newQuota >= profile.BaseQuota/2 {
			profile.DynamicQuota = newQuota
		}
	}
}

func (e *Engine) refillCredits(profile *AgentProfile, now time.Time) {
	if !profile.LastRequest.IsZero() {
		elapsed := now.Sub(profile.LastRequest).Seconds()
		profile.Credits += elapsed * profile.CreditRefillRate
		if profile.Credits > profile.MaxCredits {
			profile.Credits = profile.MaxCredits
		}
	}
}
