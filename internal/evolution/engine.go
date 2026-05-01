package evolution

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
	"github.com/kafkamcp/kafkamcp/internal/randid"
)

// FieldDiff represents a change to a single schema field.
type FieldDiff struct {
	Field     string `json:"field"`
	Action    string `json:"action"` // "added", "removed", "modified", "unchanged"
	OldType   any    `json:"old_type,omitempty"`
	NewType   any    `json:"new_type,omitempty"`
	Breaking  bool   `json:"breaking"`
	Details   string `json:"details,omitempty"`
}

// SchemaDiff is the full diff between two schema versions.
type SchemaDiff struct {
	Subject     string      `json:"subject"`
	VersionFrom int         `json:"version_from"`
	VersionTo   int         `json:"version_to"`
	Fields      []FieldDiff `json:"fields"`
	Summary     DiffSummary `json:"summary"`
	Compatible  bool        `json:"compatible"`
}

// DiffSummary provides a statistical overview of changes.
type DiffSummary struct {
	Added     int `json:"added"`
	Removed   int `json:"removed"`
	Modified  int `json:"modified"`
	Unchanged int `json:"unchanged"`
	Breaking  int `json:"breaking"`
}

// ConsumerImpact describes how a schema change affects a consumer group.
type ConsumerImpact struct {
	GroupID        string   `json:"group_id"`
	Topics         []string `json:"topics"`
	AffectedFields []string `json:"affected_fields"`
	Impact         string   `json:"impact"` // "none", "compatible", "breaking"
	Recommendation string   `json:"recommendation"`
}

// ImpactAnalysis maps schema changes to consumer group impacts.
type ImpactAnalysis struct {
	Subject     string           `json:"subject"`
	Diff        SchemaDiff       `json:"diff"`
	Consumers   []ConsumerImpact `json:"consumers"`
	RiskLevel   string           `json:"risk_level"` // "low", "medium", "high", "critical"
	CanAutoRoll bool             `json:"can_auto_roll"`
}

// RolloutPlan describes a staged rollout for a schema change.
type RolloutPlan struct {
	Subject     string         `json:"subject"`
	Stages      []RolloutStage `json:"stages"`
	CanaryTopic string         `json:"canary_topic,omitempty"`
	RiskLevel   string         `json:"risk_level"`
}

// RolloutStage describes a single stage in the rollout.
type RolloutStage struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Actions     []string `json:"actions"`
	Validation  []string `json:"validation"`
	Rollback    string   `json:"rollback,omitempty"`
}

// ComputeDiff performs a field-by-field diff between two schema definitions.
func ComputeDiff(subject string, versionFrom, versionTo int, schemaFrom, schemaTo any) *SchemaDiff {
	fromFields := extractFields(schemaFrom)
	toFields := extractFields(schemaTo)

	diff := &SchemaDiff{
		Subject:     subject,
		VersionFrom: versionFrom,
		VersionTo:   versionTo,
		Compatible:  true,
	}

	allFields := make(map[string]bool)
	for k := range fromFields {
		allFields[k] = true
	}
	for k := range toFields {
		allFields[k] = true
	}

	sortedFields := make([]string, 0, len(allFields))
	for k := range allFields {
		sortedFields = append(sortedFields, k)
	}
	sort.Strings(sortedFields)

	for _, field := range sortedFields {
		oldType, inOld := fromFields[field]
		newType, inNew := toFields[field]

		var fd FieldDiff
		switch {
		case inOld && !inNew:
			fd = FieldDiff{
				Field:    field,
				Action:   "removed",
				OldType:  oldType,
				Breaking: !hasDefault(schemaFrom, field),
				Details:  "Field removed from schema",
			}
			if fd.Breaking {
				diff.Compatible = false
			}
			diff.Summary.Removed++

		case !inOld && inNew:
			fd = FieldDiff{
				Field:    field,
				Action:   "added",
				NewType:  newType,
				Breaking: !hasDefault(schemaTo, field),
				Details:  "New field added to schema",
			}
			if fd.Breaking {
				diff.Compatible = false
			}
			diff.Summary.Added++

		case !reflect.DeepEqual(oldType, newType):
			breaking := isBreakingTypeChange(oldType, newType)
			fd = FieldDiff{
				Field:    field,
				Action:   "modified",
				OldType:  oldType,
				NewType:  newType,
				Breaking: breaking,
				Details:  fmt.Sprintf("Type changed from %v to %v", oldType, newType),
			}
			if breaking {
				diff.Compatible = false
			}
			diff.Summary.Modified++

		default:
			fd = FieldDiff{
				Field:  field,
				Action: "unchanged",
			}
			diff.Summary.Unchanged++
		}

		diff.Fields = append(diff.Fields, fd)
	}

	diff.Summary.Breaking = countBreaking(diff.Fields)
	return diff
}

// AnalyzeConsumerImpact determines how schema changes affect consumer groups.
func AnalyzeConsumerImpact(diff *SchemaDiff, consumerGroups []string, topicsByGroup map[string][]string) *ImpactAnalysis {
	analysis := &ImpactAnalysis{
		Subject:   diff.Subject,
		Diff:      *diff,
		RiskLevel: "low",
	}

	// Determine which topics use this schema
	subjectTopic := extractTopicFromSubject(diff.Subject)

	for _, groupID := range consumerGroups {
		topics := topicsByGroup[groupID]
		var affected bool
		for _, t := range topics {
			if t == subjectTopic {
				affected = true
				break
			}
		}
		if !affected {
			continue
		}

		impact := ConsumerImpact{
			GroupID: groupID,
			Topics: []string{subjectTopic},
		}

		for _, f := range diff.Fields {
			if f.Action != "unchanged" {
				impact.AffectedFields = append(impact.AffectedFields, f.Field)
			}
		}

		if diff.Compatible {
			impact.Impact = "compatible"
			impact.Recommendation = "Safe to roll out. Consumer will handle changes automatically."
		} else {
			impact.Impact = "breaking"
			impact.Recommendation = "Breaking change detected. Update consumer before schema rollout."
			analysis.RiskLevel = "high"
		}

		analysis.Consumers = append(analysis.Consumers, impact)
	}

	analysis.CanAutoRoll = diff.Compatible && analysis.RiskLevel == "low"

	if len(analysis.Consumers) == 0 {
		analysis.RiskLevel = "low"
		analysis.CanAutoRoll = true
	} else if analysis.RiskLevel != "high" && diff.Summary.Modified > 0 {
		analysis.RiskLevel = "medium"
	}

	return analysis
}

// CreateRolloutPlan generates a staged rollout plan for a schema change.
func CreateRolloutPlan(diff *SchemaDiff, impact *ImpactAnalysis) *RolloutPlan {
	plan := &RolloutPlan{
		Subject:   diff.Subject,
		RiskLevel: impact.RiskLevel,
	}

	// Stage 1: Validation
	plan.Stages = append(plan.Stages, RolloutStage{
		Name:        "validate",
		Description: "Validate schema compatibility and run automated tests",
		Actions: []string{
			"Run schema compatibility check against Schema Registry",
			"Verify all affected consumers have been updated if breaking",
			"Run backward-compatibility test suite",
		},
		Validation: []string{
			"Schema passes compatibility check",
			"All consumer groups acknowledged",
		},
		Rollback: "No changes made yet — safe to abort",
	})

	// Stage 2: Canary (for medium/high risk)
	if impact.RiskLevel != "low" {
		canaryTopic := fmt.Sprintf("%s-canary", extractTopicFromSubject(diff.Subject))
		plan.CanaryTopic = canaryTopic
		plan.Stages = append(plan.Stages, RolloutStage{
			Name:        "canary",
			Description: "Deploy to canary topic and validate with subset of traffic",
			Actions: []string{
				fmt.Sprintf("Create canary topic %q with new schema", canaryTopic),
				"Route 5%% of producer traffic to canary topic",
				"Monitor error rates on canary consumers for 30 minutes",
			},
			Validation: []string{
				"No serialization errors on canary topic",
				"Consumer lag stable on canary consumers",
				"Error rate below 0.1%%",
			},
			Rollback: fmt.Sprintf("Delete canary topic %q and revert producer routing", canaryTopic),
		})
	}

	// Stage 3: Register schema
	plan.Stages = append(plan.Stages, RolloutStage{
		Name:        "register",
		Description: "Register new schema version in Schema Registry",
		Actions: []string{
			"Register new schema version",
			"Verify registration success",
			"Update documentation",
		},
		Validation: []string{
			"New version visible in Schema Registry",
			"Compatibility level maintained",
		},
		Rollback: "Delete the new schema version from Schema Registry",
	})

	// Stage 4: Producer rollout
	plan.Stages = append(plan.Stages, RolloutStage{
		Name:        "producer_rollout",
		Description: "Update producers to use new schema",
		Actions: []string{
			"Deploy producer updates with new schema",
			"Monitor producer error rates",
			"Verify messages are produced with new schema",
		},
		Validation: []string{
			"Producer error rate below threshold",
			"Messages validated against new schema",
		},
		Rollback: "Roll back producer deployment to previous version",
	})

	// Stage 5: Consumer rollout (if breaking)
	if !diff.Compatible {
		plan.Stages = append(plan.Stages, RolloutStage{
			Name:        "consumer_rollout",
			Description: "Update consumers to handle new schema",
			Actions: []string{
				"Deploy consumer updates for affected groups",
				"Monitor consumer lag and error rates",
				"Verify successful message processing",
			},
			Validation: []string{
				"All consumer groups stable",
				"No deserialization errors",
				"Consumer lag returning to normal",
			},
			Rollback: "Roll back consumer deployment, producers will still use old schema",
		})
	}

	return plan
}

// extractFields pulls field names and types from a schema.
func extractFields(schema any) map[string]any {
	fields := make(map[string]any)

	switch s := schema.(type) {
	case map[string]any:
		if fieldsArr, ok := s["fields"].([]any); ok {
			for _, f := range fieldsArr {
				if field, ok := f.(map[string]any); ok {
					name, _ := field["name"].(string)
					if name != "" {
						fields[name] = field["type"]
					}
				}
			}
		}
		if props, ok := s["properties"].(map[string]any); ok {
			for name, prop := range props {
				fields[name] = prop
			}
		}
	case string:
		var parsed map[string]any
		if err := json.Unmarshal([]byte(s), &parsed); err == nil {
			return extractFields(parsed)
		}
	}

	return fields
}

func hasDefault(schema any, field string) bool {
	s, ok := schema.(map[string]any)
	if !ok {
		return false
	}
	fieldsArr, ok := s["fields"].([]any)
	if !ok {
		return false
	}
	for _, f := range fieldsArr {
		fm, ok := f.(map[string]any)
		if !ok {
			continue
		}
		if name, _ := fm["name"].(string); name == field {
			_, hasDefault := fm["default"]
			return hasDefault
		}
	}
	return false
}

// isBreakingTypeChange determines if a type change is backward-incompatible.
func isBreakingTypeChange(oldType, newType any) bool {
	oldStr := fmt.Sprintf("%v", oldType)
	newStr := fmt.Sprintf("%v", newType)

	// Compatible promotions
	promotions := map[string][]string{
		"int":    {"long", "float", "double"},
		"long":   {"float", "double"},
		"float":  {"double"},
		"string": {"bytes"},
		"bytes":  {"string"},
	}

	if compatibles, ok := promotions[oldStr]; ok {
		for _, c := range compatibles {
			if c == newStr {
				return false
			}
		}
	}

	return oldStr != newStr
}

func extractTopicFromSubject(subject string) string {
	for _, suffix := range []string{"-value", "-key", ".value", ".key"} {
		if strings.HasSuffix(subject, suffix) {
			return subject[:len(subject)-len(suffix)]
		}
	}
	return subject
}

func countBreaking(fields []FieldDiff) int {
	count := 0
	for _, f := range fields {
		if f.Breaking {
			count++
		}
	}
	return count
}

func randomSuffix() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return randid.New("ev")
	}
	return hex.EncodeToString(b)
}

// SchemaProposal represents a proposed schema change awaiting approval.
type SchemaProposal struct {
	ID           string          `json:"id"`
	Subject      string          `json:"subject"`
	Schema       string          `json:"schema"`
	SchemaType   string          `json:"schema_type"`
	Diff         *SchemaDiff     `json:"diff,omitempty"`
	Impact       *ImpactAnalysis `json:"impact,omitempty"`
	RolloutPlan  *RolloutPlan    `json:"rollout_plan,omitempty"`
	Status       string          `json:"status"`
	ProposedBy   string          `json:"proposed_by"`
	ReviewedBy   string          `json:"reviewed_by,omitempty"`
	ReviewNote   string          `json:"review_note,omitempty"`
	ProposedAt   string          `json:"proposed_at"`
	ReviewedAt   string          `json:"reviewed_at,omitempty"`
	AppliedAt    string          `json:"applied_at,omitempty"`
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
