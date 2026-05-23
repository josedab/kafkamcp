package portal

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/randid"
)

// RequestType identifies the kind of governance request.
type RequestType string

// RequestTopicCreate and related values define constants for this package.
const (
	RequestTopicCreate  RequestType = "topic_create"
	RequestTopicModify  RequestType = "topic_modify"
	RequestTopicDelete  RequestType = "topic_delete"
	RequestSchemaChange RequestType = "schema_change"
	RequestAccessGrant  RequestType = "access_grant"
	RequestAccessRevoke RequestType = "access_revoke"
)

// RequestStatus tracks the lifecycle of a governance request.
type RequestStatus string

// StatusPending and related values define constants for this package.
const (
	StatusPending   RequestStatus = "pending"
	StatusApproved  RequestStatus = "approved"
	StatusRejected  RequestStatus = "rejected"
	StatusExecuting RequestStatus = "executing"
	StatusCompleted RequestStatus = "completed"
	StatusFailed    RequestStatus = "failed"
)

// GovernanceRequest is a self-service governance request.
type GovernanceRequest struct {
	ID            string         `json:"id"`
	Type          RequestType    `json:"type"`
	Status        RequestStatus  `json:"status"`
	Requester     string         `json:"requester"`
	Title         string         `json:"title"`
	Description   string         `json:"description"`
	Parameters    map[string]any `json:"parameters"`
	Cluster       string         `json:"cluster,omitempty"`
	Topic         string         `json:"topic,omitempty"`
	ApprovalChain []ApprovalStep `json:"approval_chain"`
	CreatedAt     string         `json:"created_at"`
	UpdatedAt     string         `json:"updated_at"`
	CompletedAt   string         `json:"completed_at,omitempty"`
}

// ApprovalStep is a step in the approval chain.
type ApprovalStep struct {
	Order      int    `json:"order"`
	Approver   string `json:"approver"`
	Role       string `json:"role"`   // team_lead, admin, security, compliance
	Status     string `json:"status"` // pending, approved, rejected
	Comment    string `json:"comment,omitempty"`
	ReviewedAt string `json:"reviewed_at,omitempty"`
}

// DashboardData aggregates portal metrics for the governance dashboard.
type DashboardData struct {
	TotalRequests     int                 `json:"total_requests"`
	PendingRequests   int                 `json:"pending_requests"`
	ApprovedRequests  int                 `json:"approved_requests"`
	RejectedRequests  int                 `json:"rejected_requests"`
	CompletedRequests int                 `json:"completed_requests"`
	RequestsByType    map[RequestType]int `json:"requests_by_type"`
	RecentRequests    []GovernanceRequest `json:"recent_requests"`
	ComplianceScore   int                 `json:"compliance_score"`
	CostSummary       *CostSummary        `json:"cost_summary,omitempty"`
	GeneratedAt       string              `json:"generated_at"`
}

// CostSummary provides cost overview for the dashboard.
type CostSummary struct {
	TotalCostPerHour float64          `json:"total_cost_per_hour"`
	TopCostTopics    []TopicCostEntry `json:"top_cost_topics"`
	CostTrend        string           `json:"cost_trend"` // increasing, stable, decreasing
}

// TopicCostEntry tracks cost for a single topic.
type TopicCostEntry struct {
	Topic       string  `json:"topic"`
	CostPerHour float64 `json:"cost_per_hour"`
	Percentage  float64 `json:"percentage"`
}

// WorkflowTemplate defines an approval workflow for a request type.
type WorkflowTemplate struct {
	RequestType    RequestType `json:"request_type"`
	ApprovalSteps  []string    `json:"approval_steps"` // roles required
	AutoApprove    bool        `json:"auto_approve"`
	RequireComment bool        `json:"require_comment"`
}

// Engine manages the governance portal data layer.
type Engine struct {
	mu        sync.RWMutex
	requests  map[string]*GovernanceRequest
	workflows map[RequestType]*WorkflowTemplate
	logger    *slog.Logger
}

// NewEngine creates a new governance portal engine.
func NewEngine(logger *slog.Logger) *Engine {
	e := &Engine{
		requests:  make(map[string]*GovernanceRequest),
		workflows: make(map[RequestType]*WorkflowTemplate),
		logger:    logger,
	}
	e.initDefaultWorkflows()
	return e
}

func (e *Engine) initDefaultWorkflows() {
	e.workflows[RequestTopicCreate] = &WorkflowTemplate{
		RequestType:    RequestTopicCreate,
		ApprovalSteps:  []string{"team_lead", "admin"},
		RequireComment: true,
	}
	e.workflows[RequestTopicDelete] = &WorkflowTemplate{
		RequestType:    RequestTopicDelete,
		ApprovalSteps:  []string{"team_lead", "admin", "security"},
		RequireComment: true,
	}
	e.workflows[RequestSchemaChange] = &WorkflowTemplate{
		RequestType:    RequestSchemaChange,
		ApprovalSteps:  []string{"team_lead"},
		RequireComment: true,
	}
	e.workflows[RequestAccessGrant] = &WorkflowTemplate{
		RequestType:    RequestAccessGrant,
		ApprovalSteps:  []string{"security"},
		RequireComment: false,
	}
	e.workflows[RequestAccessRevoke] = &WorkflowTemplate{
		RequestType:   RequestAccessRevoke,
		ApprovalSteps: []string{"security"},
		AutoApprove:   true,
	}
	e.workflows[RequestTopicModify] = &WorkflowTemplate{
		RequestType:    RequestTopicModify,
		ApprovalSteps:  []string{"team_lead"},
		RequireComment: false,
	}
}

// SubmitRequest creates a new governance request with the appropriate approval chain.
func (e *Engine) SubmitRequest(reqType RequestType, requester, title, description, cluster, topic string, params map[string]any) (*GovernanceRequest, error) {
	if requester == "" {
		return nil, fmt.Errorf("requester is required")
	}
	if title == "" {
		return nil, fmt.Errorf("title is required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	workflow, ok := e.workflows[reqType]
	if !ok {
		return nil, fmt.Errorf("no workflow defined for request type %q", reqType)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	req := &GovernanceRequest{
		ID:          randid.New("gov"),
		Type:        reqType,
		Status:      StatusPending,
		Requester:   requester,
		Title:       title,
		Description: description,
		Parameters:  params,
		Cluster:     cluster,
		Topic:       topic,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	// Build approval chain from workflow
	for i, role := range workflow.ApprovalSteps {
		step := ApprovalStep{
			Order:  i + 1,
			Role:   role,
			Status: "pending",
		}
		req.ApprovalChain = append(req.ApprovalChain, step)
	}

	// Auto-approve if workflow allows
	if workflow.AutoApprove {
		req.Status = StatusApproved
		for i := range req.ApprovalChain {
			req.ApprovalChain[i].Status = "approved"
			req.ApprovalChain[i].Comment = "auto-approved"
			req.ApprovalChain[i].ReviewedAt = now
		}
	}

	e.requests[req.ID] = req
	return req, nil
}

// ReviewRequest approves or rejects a specific approval step.
func (e *Engine) ReviewRequest(requestID, approver, decision, comment string) (*GovernanceRequest, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	req, ok := e.requests[requestID]
	if !ok {
		return nil, fmt.Errorf("request %q not found", requestID)
	}
	if req.Status != StatusPending {
		return nil, fmt.Errorf("request %q is %s; cannot review", requestID, req.Status)
	}
	if decision != "approved" && decision != "rejected" {
		return nil, fmt.Errorf("decision must be 'approved' or 'rejected'")
	}

	// Find the next pending approval step
	reviewed := false
	for i := range req.ApprovalChain {
		if req.ApprovalChain[i].Status == "pending" {
			req.ApprovalChain[i].Approver = approver
			req.ApprovalChain[i].Status = decision
			req.ApprovalChain[i].Comment = comment
			req.ApprovalChain[i].ReviewedAt = time.Now().UTC().Format(time.RFC3339)
			reviewed = true
			break
		}
	}
	if !reviewed {
		return nil, fmt.Errorf("no pending approval step found")
	}

	// Update request status
	if decision == "rejected" {
		req.Status = StatusRejected
	} else {
		allApproved := true
		for _, step := range req.ApprovalChain {
			if step.Status == "pending" {
				allApproved = false
				break
			}
		}
		if allApproved {
			req.Status = StatusApproved
		}
	}

	req.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	return req, nil
}

// GetRequest returns a request by ID.
func (e *Engine) GetRequest(id string) (*GovernanceRequest, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	req, ok := e.requests[id]
	if !ok {
		return nil, fmt.Errorf("request %q not found", id)
	}
	return req, nil
}

// ListRequests returns requests filtered by status and type.
func (e *Engine) ListRequests(status RequestStatus, reqType RequestType) []GovernanceRequest {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []GovernanceRequest
	for _, req := range e.requests {
		if status != "" && req.Status != status {
			continue
		}
		if reqType != "" && req.Type != reqType {
			continue
		}
		result = append(result, *req)
	}
	return result
}

// GetDashboard returns aggregate governance dashboard data.
func (e *Engine) GetDashboard() *DashboardData {
	e.mu.RLock()
	defer e.mu.RUnlock()

	dashboard := &DashboardData{
		RequestsByType: make(map[RequestType]int),
		GeneratedAt:    time.Now().UTC().Format(time.RFC3339),
	}

	for _, req := range e.requests {
		dashboard.TotalRequests++
		dashboard.RequestsByType[req.Type]++

		switch req.Status {
		case StatusPending:
			dashboard.PendingRequests++
		case StatusApproved:
			dashboard.ApprovedRequests++
		case StatusRejected:
			dashboard.RejectedRequests++
		case StatusCompleted:
			dashboard.CompletedRequests++
		}
	}

	// Compute compliance score from governance health:
	// 100 = all requests properly processed, deduct for rejected/failed
	dashboard.ComplianceScore = 100
	if dashboard.TotalRequests > 0 {
		processedRate := float64(dashboard.ApprovedRequests+dashboard.CompletedRequests) / float64(dashboard.TotalRequests)
		dashboard.ComplianceScore = int(processedRate * 100)
		if dashboard.ComplianceScore > 100 {
			dashboard.ComplianceScore = 100
		}
	}

	// Recent requests (last 10, sorted by creation time descending)
	var recent []GovernanceRequest
	for _, req := range e.requests {
		recent = append(recent, *req)
	}
	sort.Slice(recent, func(i, j int) bool {
		return recent[i].CreatedAt > recent[j].CreatedAt
	})
	if len(recent) > 10 {
		recent = recent[:10]
	}
	dashboard.RecentRequests = recent

	return dashboard
}

// GetWorkflows returns all configured workflow templates.
func (e *Engine) GetWorkflows() []WorkflowTemplate {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]WorkflowTemplate, 0, len(e.workflows))
	for _, w := range e.workflows {
		result = append(result, *w)
	}
	return result
}
