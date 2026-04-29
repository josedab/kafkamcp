// Package topicportal provides self-service topic provisioning with
// configurable multi-step approval chains and notification webhooks.
package topicportal

import (
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"
)

// RequestStatus represents the state of a topic provisioning request.
type RequestStatus string

const (
	StatusPending   RequestStatus = "pending"
	StatusApproved  RequestStatus = "approved"
	StatusRejected  RequestStatus = "rejected"
	StatusCreated   RequestStatus = "created"
	StatusExpired   RequestStatus = "expired"
)

// TopicRequest is a self-service topic provisioning request.
type TopicRequest struct {
	ID                string            `json:"id"`
	TopicName         string            `json:"topic_name"`
	RequestedBy       string            `json:"requested_by"`
	Justification     string            `json:"justification"`
	Owner             string            `json:"owner"`
	RetentionMs       int64             `json:"retention_ms"`
	Partitions        int               `json:"partitions"`
	ReplicationFactor int               `json:"replication_factor"`
	SchemaSubject     string            `json:"schema_subject,omitempty"`
	Consumers         []string          `json:"consumers,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Status            RequestStatus     `json:"status"`
	ApprovalChain     []ApprovalStep    `json:"approval_chain"`
	CurrentStep       int               `json:"current_step"`
	CreatedAt         time.Time         `json:"created_at"`
	UpdatedAt         time.Time         `json:"updated_at"`
	ResolvedAt        time.Time         `json:"resolved_at,omitempty"`
	RejectReason      string            `json:"reject_reason,omitempty"`
}

// ApprovalStep represents one step in a multi-step approval chain.
type ApprovalStep struct {
	Approver   string        `json:"approver"`
	Role       string        `json:"role"`
	Status     RequestStatus `json:"status"`
	Comment    string        `json:"comment,omitempty"`
	ApprovedAt time.Time     `json:"approved_at,omitempty"`
	Timeout    time.Duration `json:"timeout,omitempty"`
}

// ApprovalChainConfig defines the approval workflow.
type ApprovalChainConfig struct {
	Steps             []ApprovalStepConfig `json:"steps" yaml:"steps"`
	AutoApprovePatterns []string           `json:"auto_approve_patterns" yaml:"auto_approve_patterns"`
	DefaultTimeout    time.Duration        `json:"default_timeout" yaml:"default_timeout"`
	WebhookURL        string               `json:"webhook_url,omitempty" yaml:"webhook_url,omitempty"`
}

// ApprovalStepConfig defines a step template.
type ApprovalStepConfig struct {
	Role    string        `json:"role" yaml:"role"`
	Timeout time.Duration `json:"timeout,omitempty" yaml:"timeout,omitempty"`
}

// DefaultApprovalChainConfig returns a standard approval chain.
func DefaultApprovalChainConfig() ApprovalChainConfig {
	return ApprovalChainConfig{
		Steps: []ApprovalStepConfig{
			{Role: "team_lead", Timeout: 24 * time.Hour},
			{Role: "platform_admin", Timeout: 48 * time.Hour},
		},
		AutoApprovePatterns: []string{`^dev\..*`, `^test\..*`, `^sandbox\..*`},
		DefaultTimeout:      72 * time.Hour,
	}
}

// Engine manages topic provisioning requests and approval workflows.
type Engine struct {
	mu               sync.RWMutex
	requests         map[string]*TopicRequest
	chainConfig      ApprovalChainConfig
	autoApproveRegex []*regexp.Regexp
	logger           *slog.Logger
}

// NewEngine creates a new topic portal engine.
func NewEngine(cfg ApprovalChainConfig, logger *slog.Logger) *Engine {
	var regexes []*regexp.Regexp
	for _, pattern := range cfg.AutoApprovePatterns {
		if re, err := regexp.Compile(pattern); err == nil {
			regexes = append(regexes, re)
		}
	}

	return &Engine{
		requests:         make(map[string]*TopicRequest),
		chainConfig:      cfg,
		autoApproveRegex: regexes,
		logger:           logger,
	}
}

// SubmitRequest creates a new topic provisioning request.
func (e *Engine) SubmitRequest(req TopicRequest) (*TopicRequest, error) {
	if req.TopicName == "" {
		return nil, fmt.Errorf("topic_name is required")
	}
	if req.RequestedBy == "" {
		return nil, fmt.Errorf("requested_by is required")
	}
	if req.Justification == "" {
		return nil, fmt.Errorf("justification is required")
	}
	if req.Partitions < 1 {
		req.Partitions = 1
	}
	if req.ReplicationFactor < 1 {
		req.ReplicationFactor = 3
	}
	if req.RetentionMs <= 0 {
		req.RetentionMs = 7 * 24 * 3600 * 1000
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	req.ID = fmt.Sprintf("req-%s-%d", req.TopicName, time.Now().UnixNano())
	req.CreatedAt = time.Now()
	req.UpdatedAt = time.Now()
	req.Status = StatusPending

	// Check auto-approve
	if e.matchesAutoApprove(req.TopicName) {
		req.Status = StatusApproved
		req.ResolvedAt = time.Now()
		e.requests[req.ID] = &req
		if e.logger != nil {
			e.logger.Info("topic request auto-approved", "topic", req.TopicName, "pattern", "auto_approve")
		}
		return &req, nil
	}

	// Build approval chain
	for _, stepCfg := range e.chainConfig.Steps {
		timeout := stepCfg.Timeout
		if timeout == 0 {
			timeout = e.chainConfig.DefaultTimeout
		}
		req.ApprovalChain = append(req.ApprovalChain, ApprovalStep{
			Role:    stepCfg.Role,
			Status:  StatusPending,
			Timeout: timeout,
		})
	}
	req.CurrentStep = 0

	e.requests[req.ID] = &req
	return &req, nil
}

// Approve advances the approval chain for a request.
func (e *Engine) Approve(requestID, approver, comment string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	req, ok := e.requests[requestID]
	if !ok {
		return fmt.Errorf("request %q not found", requestID)
	}
	if req.Status != StatusPending {
		return fmt.Errorf("request %q is not pending (status: %s)", requestID, req.Status)
	}
	if req.CurrentStep >= len(req.ApprovalChain) {
		return fmt.Errorf("request %q has no pending approval steps", requestID)
	}

	step := &req.ApprovalChain[req.CurrentStep]
	step.Status = StatusApproved
	step.Approver = approver
	step.Comment = comment
	step.ApprovedAt = time.Now()
	req.CurrentStep++
	req.UpdatedAt = time.Now()

	// Check if all steps approved
	if req.CurrentStep >= len(req.ApprovalChain) {
		req.Status = StatusApproved
		req.ResolvedAt = time.Now()
	}

	return nil
}

// Reject rejects a provisioning request.
func (e *Engine) Reject(requestID, rejectedBy, reason string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	req, ok := e.requests[requestID]
	if !ok {
		return fmt.Errorf("request %q not found", requestID)
	}
	if req.Status != StatusPending {
		return fmt.Errorf("request %q is not pending", requestID)
	}

	req.Status = StatusRejected
	req.RejectReason = reason
	req.ResolvedAt = time.Now()
	req.UpdatedAt = time.Now()

	if req.CurrentStep < len(req.ApprovalChain) {
		req.ApprovalChain[req.CurrentStep].Status = StatusRejected
		req.ApprovalChain[req.CurrentStep].Approver = rejectedBy
		req.ApprovalChain[req.CurrentStep].Comment = reason
	}

	return nil
}

// GetRequest returns a topic request by ID.
func (e *Engine) GetRequest(requestID string) (*TopicRequest, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	req, ok := e.requests[requestID]
	return req, ok
}

// ListRequests returns requests filtered by status.
func (e *Engine) ListRequests(status RequestStatus, limit int) []TopicRequest {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if limit <= 0 {
		limit = 50
	}

	var result []TopicRequest
	for _, req := range e.requests {
		if status == "" || req.Status == status {
			result = append(result, *req)
			if len(result) >= limit {
				break
			}
		}
	}
	return result
}

func (e *Engine) matchesAutoApprove(topicName string) bool {
	for _, re := range e.autoApproveRegex {
		if re.MatchString(topicName) {
			return true
		}
	}
	return false
}
