// Package retention provides intelligent data retention policy management,
// analyzing usage patterns, compliance requirements, and cost projections
// to recommend and enforce optimal retention policies per topic.
package retention

import (
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/randid"
)

// ComplianceFramework represents a regulatory framework.
type ComplianceFramework string

// ComplianceGDPR and related values define constants for this package.
const (
	ComplianceGDPR  ComplianceFramework = "GDPR"
	ComplianceHIPAA ComplianceFramework = "HIPAA"
	ComplianceSOX   ComplianceFramework = "SOX"
	CompliancePCI   ComplianceFramework = "PCI_DSS"
	ComplianceNone  ComplianceFramework = "none"
)

// RetentionPolicy defines the retention configuration for a topic.
type RetentionPolicy struct {
	Topic              string              `json:"topic" yaml:"topic"`
	CurrentRetentionMs int64               `json:"current_retention_ms"`
	TargetRetentionMs  int64               `json:"target_retention_ms,omitempty"`
	Compliance         ComplianceFramework `json:"compliance" yaml:"compliance"`
	MinRetentionMs     int64               `json:"min_retention_ms" yaml:"min_retention_ms"`
	MaxRetentionMs     int64               `json:"max_retention_ms" yaml:"max_retention_ms"`
	CriticalityTag     string              `json:"criticality" yaml:"criticality"` // low, medium, high, critical
	AutoApply          bool                `json:"auto_apply" yaml:"auto_apply"`
	GracePeriod        time.Duration       `json:"grace_period" yaml:"grace_period"`
}

// UsageMetrics captures topic usage patterns for retention analysis.
type UsageMetrics struct {
	Topic           string    `json:"topic"`
	ConsumerLag     int64     `json:"consumer_lag"`
	ReadFrequencyHz float64   `json:"read_frequency_hz"`
	LastReadTime    time.Time `json:"last_read_time"`
	StorageBytes    int64     `json:"storage_bytes"`
	MessageCount    int64     `json:"message_count"`
	ActiveConsumers int       `json:"active_consumers"`
	BytesPerDay     int64     `json:"bytes_per_day"`
}

// RetentionRecommendation is a suggested retention change.
type RetentionRecommendation struct {
	Topic                string              `json:"topic"`
	CurrentRetentionMs   int64               `json:"current_retention_ms"`
	RecommendedMs        int64               `json:"recommended_retention_ms"`
	Reason               string              `json:"reason"`
	EstimatedSavingsPct  float64             `json:"estimated_savings_pct"`
	EstimatedSavingBytes int64               `json:"estimated_savings_bytes"`
	Compliance           ComplianceFramework `json:"compliance"`
	RiskLevel            string              `json:"risk_level"` // low, medium, high
	RequiresApproval     bool                `json:"requires_approval"`
}

// RetentionChangeRequest represents a pending retention change.
type RetentionChangeRequest struct {
	ID          string    `json:"id"`
	Topic       string    `json:"topic"`
	CurrentMs   int64     `json:"current_retention_ms"`
	TargetMs    int64     `json:"target_retention_ms"`
	Reason      string    `json:"reason"`
	Status      string    `json:"status"` // pending, approved, applied, rejected, rolled_back
	RequestedAt time.Time `json:"requested_at"`
	ApprovedAt  time.Time `json:"approved_at,omitempty"`
	AppliedAt   time.Time `json:"applied_at,omitempty"`
	ApprovedBy  string    `json:"approved_by,omitempty"`
	GraceEndsAt time.Time `json:"grace_ends_at,omitempty"`
}

// Engine manages intelligent retention policies.
type Engine struct {
	mu              sync.RWMutex
	policies        map[string]*RetentionPolicy
	recommendations []RetentionRecommendation
	changeRequests  map[string]*RetentionChangeRequest
	complianceRules map[ComplianceFramework]complianceRule
	logger          *slog.Logger
}

type complianceRule struct {
	MinRetentionMs int64
	MaxRetentionMs int64
	Description    string
}

// NewEngine creates a new retention policy engine.
func NewEngine(logger *slog.Logger) *Engine {
	e := &Engine{
		policies:       make(map[string]*RetentionPolicy),
		changeRequests: make(map[string]*RetentionChangeRequest),
		logger:         logger,
	}
	e.complianceRules = map[ComplianceFramework]complianceRule{
		ComplianceGDPR: {
			MaxRetentionMs: 365 * 24 * 3600 * 1000, // 1 year max
			Description:    "GDPR right to deletion - data must not be retained beyond necessity",
		},
		ComplianceHIPAA: {
			MinRetentionMs: 6 * 365 * 24 * 3600 * 1000, // 6 years min
			Description:    "HIPAA requires minimum 6-year retention for medical records",
		},
		ComplianceSOX: {
			MinRetentionMs: 7 * 365 * 24 * 3600 * 1000, // 7 years min
			Description:    "SOX requires 7-year retention for financial records",
		},
		CompliancePCI: {
			MinRetentionMs: 365 * 24 * 3600 * 1000, // 1 year min
			Description:    "PCI DSS requires minimum 1-year retention for audit trails",
		},
	}
	return e
}

// SetPolicy configures a retention policy for a topic.
func (e *Engine) SetPolicy(policy RetentionPolicy) error {
	if policy.Topic == "" {
		return fmt.Errorf("topic is required")
	}

	// Validate against compliance rules
	if policy.Compliance != ComplianceNone && policy.Compliance != "" {
		rule, ok := e.complianceRules[policy.Compliance]
		if ok {
			if rule.MinRetentionMs > 0 && policy.TargetRetentionMs > 0 && policy.TargetRetentionMs < rule.MinRetentionMs {
				return fmt.Errorf("retention %dms violates %s minimum retention of %dms",
					policy.TargetRetentionMs, policy.Compliance, rule.MinRetentionMs)
			}
			if rule.MaxRetentionMs > 0 && policy.TargetRetentionMs > rule.MaxRetentionMs {
				return fmt.Errorf("retention %dms violates %s maximum retention of %dms",
					policy.TargetRetentionMs, policy.Compliance, rule.MaxRetentionMs)
			}
		}
	}

	if policy.GracePeriod == 0 {
		policy.GracePeriod = 24 * time.Hour
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.policies[policy.Topic] = &policy
	return nil
}

// GetPolicy returns the retention policy for a topic.
func (e *Engine) GetPolicy(topic string) (*RetentionPolicy, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	p, ok := e.policies[topic]
	return p, ok
}

// Analyze evaluates usage metrics and generates retention recommendations.
func (e *Engine) Analyze(metrics []UsageMetrics) []RetentionRecommendation {
	e.mu.Lock()
	defer e.mu.Unlock()

	var recs []RetentionRecommendation
	for _, m := range metrics {
		policy, ok := e.policies[m.Topic]
		if !ok {
			continue
		}

		rec := e.generateRecommendation(m, policy)
		if rec != nil {
			recs = append(recs, *rec)
		}
	}

	e.recommendations = recs
	return recs
}

// RequestChange creates a retention change request with approval workflow.
func (e *Engine) RequestChange(topic string, targetMs int64, reason string) (*RetentionChangeRequest, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	policy, ok := e.policies[topic]
	if !ok {
		return nil, fmt.Errorf("no policy configured for topic %q", topic)
	}

	// Validate against compliance
	if policy.Compliance != ComplianceNone && policy.Compliance != "" {
		if rule, ok := e.complianceRules[policy.Compliance]; ok {
			if rule.MinRetentionMs > 0 && targetMs < rule.MinRetentionMs {
				return nil, fmt.Errorf("target retention violates %s compliance", policy.Compliance)
			}
		}
	}

	req := &RetentionChangeRequest{
		ID:          fmt.Sprintf("ret-%s-%s", topic, randid.New("")),
		Topic:       topic,
		CurrentMs:   policy.CurrentRetentionMs,
		TargetMs:    targetMs,
		Reason:      reason,
		Status:      "pending",
		RequestedAt: time.Now(),
	}

	// Reducing retention requires approval and grace period
	if targetMs < policy.CurrentRetentionMs {
		req.GraceEndsAt = time.Now().Add(policy.GracePeriod)
	} else if policy.AutoApply {
		req.Status = "approved"
		req.ApprovedAt = time.Now()
		req.ApprovedBy = "auto"
	}

	e.changeRequests[req.ID] = req
	return req, nil
}

// ApproveChange approves a pending change request.
func (e *Engine) ApproveChange(requestID, approvedBy string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	req, ok := e.changeRequests[requestID]
	if !ok {
		return fmt.Errorf("change request %q not found", requestID)
	}
	if req.Status != "pending" {
		return fmt.Errorf("change request %q is not pending (status: %s)", requestID, req.Status)
	}

	req.Status = "approved"
	req.ApprovedAt = time.Now()
	req.ApprovedBy = approvedBy
	return nil
}

// GetChangeRequests returns pending change requests.
func (e *Engine) GetChangeRequests(status string) []RetentionChangeRequest {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []RetentionChangeRequest
	for _, req := range e.changeRequests {
		if status == "" || req.Status == status {
			result = append(result, *req)
		}
	}
	return result
}

// GetRecommendations returns the latest recommendations.
func (e *Engine) GetRecommendations() []RetentionRecommendation {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]RetentionRecommendation, len(e.recommendations))
	copy(result, e.recommendations)
	return result
}

func (e *Engine) generateRecommendation(m UsageMetrics, policy *RetentionPolicy) *RetentionRecommendation {
	currentMs := policy.CurrentRetentionMs
	if currentMs <= 0 {
		return nil
	}

	// Score factors
	readRecency := time.Since(m.LastReadTime)
	lagRatio := float64(m.ConsumerLag) / math.Max(float64(m.MessageCount), 1)

	var recommendedMs int64
	var reason string
	var riskLevel string

	switch {
	case m.ActiveConsumers == 0 && readRecency > 30*24*time.Hour:
		recommendedMs = currentMs / 4
		reason = "No active consumers and no reads in 30+ days"
		riskLevel = "low"

	case lagRatio < 0.01 && m.ReadFrequencyHz < 0.1:
		recommendedMs = currentMs / 2
		reason = "Low consumer lag and infrequent reads suggest shorter retention is safe"
		riskLevel = "medium"

	case m.BytesPerDay > 0 && m.StorageBytes > 0:
		daysStored := float64(m.StorageBytes) / float64(m.BytesPerDay)
		if daysStored > 90 && m.ReadFrequencyHz < 1.0 {
			recommendedMs = int64(30 * 24 * 3600 * 1000) // 30 days
			reason = fmt.Sprintf("Topic stores %.0f days of data with low read frequency", daysStored)
			riskLevel = "medium"
		} else {
			return nil
		}

	default:
		return nil
	}

	// Apply compliance constraints
	if policy.Compliance != ComplianceNone && policy.Compliance != "" {
		if rule, ok := e.complianceRules[policy.Compliance]; ok {
			if rule.MinRetentionMs > 0 && recommendedMs < rule.MinRetentionMs {
				recommendedMs = rule.MinRetentionMs
				reason += fmt.Sprintf(" (constrained by %s minimum)", policy.Compliance)
			}
		}
	}

	if recommendedMs >= currentMs {
		return nil
	}

	savingsPct := float64(currentMs-recommendedMs) / float64(currentMs) * 100
	savingsBytes := int64(float64(m.StorageBytes) * savingsPct / 100)

	return &RetentionRecommendation{
		Topic:                m.Topic,
		CurrentRetentionMs:   currentMs,
		RecommendedMs:        recommendedMs,
		Reason:               reason,
		EstimatedSavingsPct:  math.Round(savingsPct*100) / 100,
		EstimatedSavingBytes: savingsBytes,
		Compliance:           policy.Compliance,
		RiskLevel:            riskLevel,
		RequiresApproval:     recommendedMs < currentMs,
	}
}
