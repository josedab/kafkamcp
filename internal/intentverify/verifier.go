package intentverify

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// RiskLevel classifies tool risk.
type RiskLevel string

// RiskRead and related values define constants for this package.
const (
	RiskRead        RiskLevel = "read"
	RiskWrite       RiskLevel = "write"
	RiskDestructive RiskLevel = "destructive"
	RiskAdmin       RiskLevel = "admin"
)

// Weight returns a numeric weight for a risk level.
func (r RiskLevel) Weight() int {
	switch r {
	case RiskRead:
		return 0
	case RiskWrite:
		return 1
	case RiskDestructive:
		return 2
	case RiskAdmin:
		return 3
	default:
		return 0
	}
}

// ToolRiskClassification maps a tool to its risk level and description.
type ToolRiskClassification struct {
	Tool        string    `json:"tool"`
	Risk        RiskLevel `json:"risk"`
	Category    string    `json:"category"`
	Description string    `json:"description"`
}

// IntentMatch represents a comparison between stated intent and requested operation.
type IntentMatch struct {
	Tool            string    `json:"tool"`
	StatedIntent    string    `json:"stated_intent"`
	Risk            RiskLevel `json:"risk"`
	MatchScore      float64   `json:"match_score"` // 0.0 to 1.0
	Aligned         bool      `json:"aligned"`
	Explanation     string    `json:"explanation"`
	RequiresConfirm bool      `json:"requires_confirmation"`
}

// ConfirmationRequest represents a pending confirmation for a risky operation.
type ConfirmationRequest struct {
	ID          string    `json:"id"`
	AgentID     string    `json:"agent_id"`
	Tool        string    `json:"tool"`
	Risk        RiskLevel `json:"risk"`
	Intent      string    `json:"intent"`
	Message     string    `json:"message"`
	RequestedAt time.Time `json:"requested_at"`
	ExpiresAt   time.Time `json:"expires_at"`
	Status      string    `json:"status"` // pending, confirmed, denied, expired
	ResolvedBy  string    `json:"resolved_by,omitempty"`
}

// AgentRiskConfig defines per-agent risk thresholds.
type AgentRiskConfig struct {
	AgentID          string    `json:"agent_id" yaml:"agent_id"`
	MaxRiskLevel     RiskLevel `json:"max_risk_level" yaml:"max_risk_level"`
	RequireConfirmAt RiskLevel `json:"require_confirm_at" yaml:"require_confirm_at"`
	ConfirmTimeout   string    `json:"confirm_timeout" yaml:"confirm_timeout"`
	BlockDestructive bool      `json:"block_destructive" yaml:"block_destructive"`
}

// VerifierConfig configures the intent verifier.
type VerifierConfig struct {
	DefaultMaxRisk   RiskLevel         `json:"default_max_risk" yaml:"default_max_risk"`
	DefaultConfirmAt RiskLevel         `json:"default_confirm_at" yaml:"default_confirm_at"`
	ConfirmTimeout   string            `json:"confirm_timeout" yaml:"confirm_timeout"`
	AgentConfigs     []AgentRiskConfig `json:"agent_configs,omitempty" yaml:"agent_configs,omitempty"`
}

// DefaultVerifierConfig returns sensible defaults.
func DefaultVerifierConfig() VerifierConfig {
	return VerifierConfig{
		DefaultMaxRisk:   RiskAdmin,
		DefaultConfirmAt: RiskDestructive,
		ConfirmTimeout:   "5m",
	}
}

// Verifier handles intent verification and confirmation flows.
type Verifier struct {
	config          VerifierConfig
	mu              sync.RWMutex
	classifications map[string]ToolRiskClassification
	confirmations   map[string]*ConfirmationRequest
	agentConfigs    map[string]*AgentRiskConfig
	nextID          int64
	logger          *slog.Logger
}

// NewVerifier creates a new intent verifier with default tool classifications.
func NewVerifier(cfg VerifierConfig, logger *slog.Logger) *Verifier {
	v := &Verifier{
		config:          cfg,
		classifications: make(map[string]ToolRiskClassification),
		confirmations:   make(map[string]*ConfirmationRequest),
		agentConfigs:    make(map[string]*AgentRiskConfig),
		logger:          logger,
	}
	v.seedClassifications()
	for i := range cfg.AgentConfigs {
		v.agentConfigs[cfg.AgentConfigs[i].AgentID] = &cfg.AgentConfigs[i]
	}
	return v
}

func (v *Verifier) seedClassifications() {
	// Read operations
	readTools := []string{
		"kafka_list_topics", "kafka_describe_topic", "kafka_consume",
		"kafka_list_consumer_groups", "kafka_consumer_lag", "kafka_cluster_info",
		"kafka_schema", "kafka_health_score", "kafka_audit_trail",
		"kafka_search", "kafka_timeseries_query", "kafka_knowledge_graph",
		"kafka_anomaly_detect", "kafka_lag_diagnosis", "kafka_cost_analysis",
	}
	for _, tool := range readTools {
		v.classifications[tool] = ToolRiskClassification{Tool: tool, Risk: RiskRead, Category: "query"}
	}

	// Write operations
	writeTools := []string{
		"kafka_produce", "kafka_schema_register", "kafka_commit_offsets",
		"kafka_create_topic", "kafka_webhook", "kafka_configure_topic",
	}
	for _, tool := range writeTools {
		v.classifications[tool] = ToolRiskClassification{Tool: tool, Risk: RiskWrite, Category: "write"}
	}

	// Destructive operations
	destructiveTools := []string{
		"kafka_delete_topic", "kafka_reset_offsets", "kafka_delete_records",
		"kafka_purge_topic", "kafka_remove_consumer_group",
	}
	for _, tool := range destructiveTools {
		v.classifications[tool] = ToolRiskClassification{Tool: tool, Risk: RiskDestructive, Category: "destructive",
			Description: "Permanently deletes data or resets state"}
	}

	// Admin operations
	adminTools := []string{
		"kafka_alter_configs", "kafka_reassign_partitions", "kafka_acl_manage",
		"kafka_proxy", "kafka_chaos_experiment", "kafka_migration",
	}
	for _, tool := range adminTools {
		v.classifications[tool] = ToolRiskClassification{Tool: tool, Risk: RiskAdmin, Category: "admin",
			Description: "Modifies cluster configuration or access control"}
	}
}

// ClassifyTool returns the risk classification for a tool.
func (v *Verifier) ClassifyTool(tool string) ToolRiskClassification {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if c, ok := v.classifications[tool]; ok {
		return c
	}
	return ToolRiskClassification{Tool: tool, Risk: RiskRead, Category: "unknown"}
}

// ListClassifications returns all tool classifications.
func (v *Verifier) ListClassifications(riskLevel RiskLevel) []ToolRiskClassification {
	v.mu.RLock()
	defer v.mu.RUnlock()
	var result []ToolRiskClassification
	for _, c := range v.classifications {
		if riskLevel == "" || c.Risk == riskLevel {
			result = append(result, c)
		}
	}
	return result
}

// VerifyIntent checks if an agent's stated intent aligns with the requested tool.
func (v *Verifier) VerifyIntent(agentID, tool, statedIntent string) *IntentMatch {
	classification := v.ClassifyTool(tool)

	match := &IntentMatch{
		Tool:         tool,
		StatedIntent: statedIntent,
		Risk:         classification.Risk,
	}

	// Pattern-based intent matching
	match.MatchScore = v.computeIntentScore(tool, statedIntent, classification)
	match.Aligned = match.MatchScore >= 0.5

	if !match.Aligned {
		match.Explanation = fmt.Sprintf("Stated intent %q does not appear to match %s operation %q (risk: %s)",
			statedIntent, classification.Category, tool, classification.Risk)
	} else {
		match.Explanation = fmt.Sprintf("Intent %q aligns with %s operation %q", statedIntent, classification.Category, tool)
	}

	// Check if confirmation is required
	v.mu.RLock()
	agentCfg, hasAgentCfg := v.agentConfigs[agentID]
	v.mu.RUnlock()

	confirmAt := v.config.DefaultConfirmAt
	if hasAgentCfg {
		confirmAt = agentCfg.RequireConfirmAt
		if agentCfg.BlockDestructive && classification.Risk == RiskDestructive {
			match.RequiresConfirm = true
			match.Explanation = "Agent is blocked from destructive operations"
			return match
		}
	}

	if classification.Risk.Weight() >= confirmAt.Weight() {
		match.RequiresConfirm = true
	}
	if !match.Aligned && classification.Risk.Weight() >= RiskWrite.Weight() {
		match.RequiresConfirm = true
	}

	return match
}

// RequestConfirmation creates a confirmation request for a risky operation.
func (v *Verifier) RequestConfirmation(agentID, tool, intent string) *ConfirmationRequest {
	v.mu.Lock()
	defer v.mu.Unlock()

	classification := v.classifications[tool]
	timeout := 5 * time.Minute
	if t, err := time.ParseDuration(v.config.ConfirmTimeout); err == nil {
		timeout = t
	}

	v.nextID++
	req := &ConfirmationRequest{
		ID:          fmt.Sprintf("confirm_%d", v.nextID),
		AgentID:     agentID,
		Tool:        tool,
		Risk:        classification.Risk,
		Intent:      intent,
		Message:     fmt.Sprintf("Agent %q wants to execute %s (risk: %s). Intent: %q. Approve?", agentID, tool, classification.Risk, intent),
		RequestedAt: time.Now(),
		ExpiresAt:   time.Now().Add(timeout),
		Status:      "pending",
	}

	v.confirmations[req.ID] = req
	return req
}

// ResolveConfirmation approves or denies a confirmation request.
func (v *Verifier) ResolveConfirmation(confirmID, resolvedBy string, approved bool) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	req, ok := v.confirmations[confirmID]
	if !ok {
		return fmt.Errorf("confirmation %q not found", confirmID)
	}
	if req.Status != "pending" {
		return fmt.Errorf("confirmation %q is already %s", confirmID, req.Status)
	}
	if time.Now().After(req.ExpiresAt) {
		req.Status = "expired"
		return fmt.Errorf("confirmation %q has expired", confirmID)
	}

	if approved {
		req.Status = "confirmed"
	} else {
		req.Status = "denied"
	}
	req.ResolvedBy = resolvedBy
	return nil
}

// GetConfirmation returns a confirmation request by ID.
func (v *Verifier) GetConfirmation(id string) (*ConfirmationRequest, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	req, ok := v.confirmations[id]
	if !ok {
		return nil, fmt.Errorf("confirmation %q not found", id)
	}
	return req, nil
}

// ListPendingConfirmations returns all pending confirmations for an agent.
func (v *Verifier) ListPendingConfirmations(agentID string) []ConfirmationRequest {
	v.mu.RLock()
	defer v.mu.RUnlock()
	var result []ConfirmationRequest
	for _, req := range v.confirmations {
		if req.Status == "pending" && (agentID == "" || req.AgentID == agentID) {
			result = append(result, *req)
		}
	}
	return result
}

// SetAgentConfig sets risk thresholds for a specific agent.
func (v *Verifier) SetAgentConfig(cfg AgentRiskConfig) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.agentConfigs[cfg.AgentID] = &cfg
}

// Stats returns verifier statistics.
func (v *Verifier) Stats() map[string]any {
	v.mu.RLock()
	defer v.mu.RUnlock()

	pending := 0
	for _, req := range v.confirmations {
		if req.Status == "pending" {
			pending++
		}
	}

	riskCounts := make(map[string]int)
	for _, c := range v.classifications {
		riskCounts[string(c.Risk)]++
	}

	return map[string]any{
		"classified_tools":      len(v.classifications),
		"risk_distribution":     riskCounts,
		"pending_confirmations": pending,
		"total_confirmations":   len(v.confirmations),
		"agent_configs":         len(v.agentConfigs),
	}
}

// computeIntentScore checks if stated intent keywords match the tool's purpose.
func (v *Verifier) computeIntentScore(tool, intent string, classification ToolRiskClassification) float64 {
	if intent == "" {
		return 0.3
	}
	intentLower := strings.ToLower(intent)

	// Keyword matching for common intent patterns
	intentKeywords := map[string][]string{
		"read":   {"read", "list", "view", "check", "get", "describe", "query", "search", "find", "show", "monitor"},
		"write":  {"write", "produce", "send", "publish", "create", "add", "insert"},
		"delete": {"delete", "remove", "purge", "drop", "clean", "wipe", "destroy"},
		"admin":  {"configure", "manage", "alter", "modify", "reassign", "migrate", "admin"},
		"reset":  {"reset", "clear", "restart", "rebalance"},
	}

	score := 0.0
	matches := 0

	// Check if intent mentions the tool category
	switch classification.Risk {
	case RiskRead:
		for _, kw := range intentKeywords["read"] {
			if strings.Contains(intentLower, kw) {
				matches++
			}
		}
	case RiskWrite:
		for _, kw := range intentKeywords["write"] {
			if strings.Contains(intentLower, kw) {
				matches++
			}
		}
	case RiskDestructive:
		for _, kw := range intentKeywords["delete"] {
			if strings.Contains(intentLower, kw) {
				matches++
			}
		}
		for _, kw := range intentKeywords["reset"] {
			if strings.Contains(intentLower, kw) {
				matches++
			}
		}
	case RiskAdmin:
		for _, kw := range intentKeywords["admin"] {
			if strings.Contains(intentLower, kw) {
				matches++
			}
		}
	}

	if matches > 0 {
		score = 0.7 + float64(matches)*0.1
		if score > 1.0 {
			score = 1.0
		}
	} else {
		// Check for misaligned intent (e.g., intent says "read" but tool is destructive)
		for _, kw := range intentKeywords["read"] {
			if strings.Contains(intentLower, kw) && classification.Risk == RiskDestructive {
				return 0.1
			}
		}
		score = 0.4
	}

	return score
}
