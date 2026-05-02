package evolution

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
