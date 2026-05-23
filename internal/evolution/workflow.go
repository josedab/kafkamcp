package evolution

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

// SchemaProposal represents a proposed schema change awaiting approval.
type SchemaProposal struct {
	ID          string          `json:"id"`
	Subject     string          `json:"subject"`
	Schema      string          `json:"schema"`
	SchemaType  string          `json:"schema_type"`
	Diff        *SchemaDiff     `json:"diff,omitempty"`
	Impact      *ImpactAnalysis `json:"impact,omitempty"`
	RolloutPlan *RolloutPlan    `json:"rollout_plan,omitempty"`
	Status      string          `json:"status"`
	ProposedBy  string          `json:"proposed_by"`
	ReviewedBy  string          `json:"reviewed_by,omitempty"`
	ReviewNote  string          `json:"review_note,omitempty"`
	ProposedAt  string          `json:"proposed_at"`
	ReviewedAt  string          `json:"reviewed_at,omitempty"`
	AppliedAt   string          `json:"applied_at,omitempty"`
}

// VersionTimeline represents the versioning history for a schema subject.
type VersionTimeline struct {
	Subject  string         `json:"subject"`
	Versions []VersionEntry `json:"versions"`
}

// VersionEntry is a single entry in the version timeline.
type VersionEntry struct {
	Version   int    `json:"version"`
	SchemaID  int    `json:"schema_id,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
	Status    string `json:"status"`
}

// WorkflowEngine manages schema proposals and approval workflows.
type WorkflowEngine struct {
	mu        sync.RWMutex
	proposals map[string]*SchemaProposal
}

// NewWorkflowEngine creates a new workflow engine.
func NewWorkflowEngine() *WorkflowEngine {
	return &WorkflowEngine{
		proposals: make(map[string]*SchemaProposal),
	}
}

// Propose creates a new schema change proposal with auto-generated diff and impact analysis.
func (w *WorkflowEngine) Propose(subject, schema, schemaType, proposedBy string, currentSchema any, currentVersion int, consumerGroups []string, topicsByGroup map[string][]string) (*SchemaProposal, error) {
	if subject == "" {
		return nil, fmt.Errorf("subject is required")
	}
	if schema == "" {
		return nil, fmt.Errorf("schema is required")
	}
	if proposedBy == "" {
		return nil, fmt.Errorf("proposed_by is required")
	}
	if schemaType == "" {
		schemaType = "AVRO"
	}

	id := fmt.Sprintf("prop_%s_%s", subject, randomSuffix())

	proposal := &SchemaProposal{
		ID:         id,
		Subject:    subject,
		Schema:     schema,
		SchemaType: schemaType,
		Status:     "pending",
		ProposedBy: proposedBy,
		ProposedAt: time.Now().UTC().Format(time.RFC3339),
	}

	// Auto-generate diff if current schema exists
	if currentSchema != nil {
		var newSchema any
		if err := json.Unmarshal([]byte(schema), &newSchema); err == nil {
			diff := ComputeDiff(subject, currentVersion, currentVersion+1, currentSchema, newSchema)
			proposal.Diff = diff
			impact := AnalyzeConsumerImpact(diff, consumerGroups, topicsByGroup)
			proposal.Impact = impact
			proposal.RolloutPlan = CreateRolloutPlan(diff, impact)
		}
	}

	w.mu.Lock()
	w.proposals[id] = proposal
	w.mu.Unlock()

	return proposal, nil
}

// Review approves or rejects a proposal.
func (w *WorkflowEngine) Review(proposalID, reviewedBy, action, note string) (*SchemaProposal, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	p, ok := w.proposals[proposalID]
	if !ok {
		return nil, fmt.Errorf("proposal %q not found", proposalID)
	}
	if p.Status != "pending" {
		return nil, fmt.Errorf("proposal %q is already %s", proposalID, p.Status)
	}

	switch action {
	case "approve":
		p.Status = "approved"
	case "reject":
		p.Status = "rejected"
	default:
		return nil, fmt.Errorf("invalid action %q: must be approve or reject", action)
	}

	p.ReviewedBy = reviewedBy
	p.ReviewNote = note
	p.ReviewedAt = time.Now().UTC().Format(time.RFC3339)
	return p, nil
}

// MarkApplied marks a proposal as applied.
func (w *WorkflowEngine) MarkApplied(proposalID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	p, ok := w.proposals[proposalID]
	if !ok {
		return fmt.Errorf("proposal %q not found", proposalID)
	}
	if p.Status != "approved" {
		return fmt.Errorf("proposal %q must be approved before applying (current: %s)", proposalID, p.Status)
	}
	p.Status = "applied"
	p.AppliedAt = time.Now().UTC().Format(time.RFC3339)
	return nil
}

// MarkRolledBack marks a proposal as rolled back.
func (w *WorkflowEngine) MarkRolledBack(proposalID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	p, ok := w.proposals[proposalID]
	if !ok {
		return fmt.Errorf("proposal %q not found", proposalID)
	}
	if p.Status != "applied" {
		return fmt.Errorf("can only roll back applied proposals (current: %s)", p.Status)
	}
	p.Status = "rolled_back"
	return nil
}

// GetProposal returns a proposal by ID.
func (w *WorkflowEngine) GetProposal(id string) (*SchemaProposal, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	p, ok := w.proposals[id]
	if !ok {
		return nil, fmt.Errorf("proposal %q not found", id)
	}
	return p, nil
}

// ListProposals returns all proposals, optionally filtered by status.
func (w *WorkflowEngine) ListProposals(statusFilter string) []SchemaProposal {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var result []SchemaProposal
	for _, p := range w.proposals {
		if statusFilter == "" || p.Status == statusFilter {
			result = append(result, *p)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ProposedAt > result[j].ProposedAt
	})
	return result
}
