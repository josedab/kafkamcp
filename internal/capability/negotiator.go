// Package capability implements dynamic capability negotiation for MCP tools,
// reducing the tool surface to a contextual subset based on agent role,
// permissions, and conversation context.
package capability

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
)

// Role defines an agent's access role.
type Role string

const (
	RoleViewer   Role = "viewer"
	RoleOperator Role = "operator"
	RoleAdmin    Role = "admin"
	RoleAnalyst  Role = "analyst"
	RoleSRE      Role = "sre"
	RoleSecurity Role = "security"
)

// ToolCategory groups related tools.
type ToolCategory string

const (
	CatDiscovery    ToolCategory = "discovery"
	CatMonitoring   ToolCategory = "monitoring"
	CatAdmin        ToolCategory = "admin"
	CatStreaming     ToolCategory = "streaming"
	CatSchema       ToolCategory = "schema"
	CatAnalytics    ToolCategory = "analytics"
	CatGovernance   ToolCategory = "governance"
	CatOrchestration ToolCategory = "orchestration"
	CatPlatform     ToolCategory = "platform"
	CatChat         ToolCategory = "chat"
)

// ToolInfo describes a tool for capability advertisement.
type ToolInfo struct {
	Name        string       `json:"name"`
	Category    ToolCategory `json:"category"`
	Description string       `json:"description"`
	Tier        int          `json:"tier"`
	ReadOnly    bool         `json:"read_only"`
}

// AgentProfile stores the agent's negotiated capabilities.
type AgentProfile struct {
	AgentID       string         `json:"agent_id"`
	Role          Role           `json:"role"`
	AllowedTools  map[string]bool `json:"allowed_tools"`
	Context       []string       `json:"context"`
	ExpandedTiers map[int]bool   `json:"expanded_tiers"`
}

// NegotiationResult is the outcome of capability negotiation.
type NegotiationResult struct {
	AgentID        string     `json:"agent_id"`
	Role           Role       `json:"role"`
	AvailableTools []ToolInfo `json:"available_tools"`
	TotalTools     int        `json:"total_tools"`
	FilteredTools  int        `json:"filtered_tools"`
	Categories     []string   `json:"categories"`
	ExpandHint     string     `json:"expand_hint,omitempty"`
}

// DiscoverResult from the meta-tool for explicit expansion.
type DiscoverResult struct {
	Category     string     `json:"category"`
	Tools        []ToolInfo `json:"tools"`
	Total        int        `json:"total"`
	AlreadyAvail int        `json:"already_available"`
	NewTools     int        `json:"new_tools"`
}

// Negotiator manages capability negotiation.
type Negotiator struct {
	mu       sync.RWMutex
	profiles map[string]*AgentProfile
	tools    []ToolInfo
	roleMap  map[Role][]ToolCategory
	logger   *slog.Logger
}

// NewNegotiator creates a new capability negotiator.
func NewNegotiator(logger *slog.Logger) *Negotiator {
	n := &Negotiator{
		profiles: make(map[string]*AgentProfile),
		tools:    defaultToolRegistry(),
		logger:   logger,
	}
	n.roleMap = defaultRoleMapping()
	return n
}

// Negotiate performs capability negotiation for an agent.
func (n *Negotiator) Negotiate(agentID string, role Role, context []string) *NegotiationResult {
	n.mu.Lock()
	defer n.mu.Unlock()

	if role == "" {
		role = RoleViewer
	}

	allowedCategories := n.roleMap[role]
	if allowedCategories == nil {
		allowedCategories = n.roleMap[RoleViewer]
	}

	catSet := make(map[ToolCategory]bool)
	for _, c := range allowedCategories {
		catSet[c] = true
	}

	// Context-aware pruning
	contextCategories := n.inferContextCategories(context)
	for _, c := range contextCategories {
		catSet[c] = true
	}

	var available []ToolInfo
	allowed := make(map[string]bool)
	catNames := make(map[string]bool)

	for _, tool := range n.tools {
		if catSet[tool.Category] {
			available = append(available, tool)
			allowed[tool.Name] = true
			catNames[string(tool.Category)] = true
		}
	}

	profile := &AgentProfile{
		AgentID:       agentID,
		Role:          role,
		AllowedTools:  allowed,
		Context:       context,
		ExpandedTiers: make(map[int]bool),
	}
	n.profiles[agentID] = profile

	var categories []string
	for c := range catNames {
		categories = append(categories, c)
	}

	result := &NegotiationResult{
		AgentID:        agentID,
		Role:           role,
		AvailableTools: available,
		TotalTools:     len(n.tools),
		FilteredTools:  len(available),
		Categories:     categories,
	}

	if len(available) < len(n.tools) {
		result.ExpandHint = "Use kafka_discover_tools to explore additional tool categories"
	}

	return result
}

// DiscoverTools reveals tools in a specific category for progressive disclosure.
func (n *Negotiator) DiscoverTools(agentID, category string) *DiscoverResult {
	n.mu.Lock()
	defer n.mu.Unlock()

	profile := n.profiles[agentID]

	cat := ToolCategory(category)
	var tools []ToolInfo
	alreadyAvail := 0
	newTools := 0

	for _, tool := range n.tools {
		if tool.Category == cat {
			tools = append(tools, tool)
			if profile != nil && profile.AllowedTools[tool.Name] {
				alreadyAvail++
			} else {
				newTools++
				// Expand the profile
				if profile != nil {
					profile.AllowedTools[tool.Name] = true
				}
			}
		}
	}

	return &DiscoverResult{
		Category:     category,
		Tools:        tools,
		Total:        len(tools),
		AlreadyAvail: alreadyAvail,
		NewTools:     newTools,
	}
}

// IsToolAllowed checks if a tool is available for an agent.
func (n *Negotiator) IsToolAllowed(agentID, toolName string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	profile, ok := n.profiles[agentID]
	if !ok {
		return true // no profile = no filtering
	}
	return profile.AllowedTools[toolName]
}

// GetProfile returns the agent's capability profile.
func (n *Negotiator) GetProfile(agentID string) (*AgentProfile, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	p, ok := n.profiles[agentID]
	if !ok {
		return nil, fmt.Errorf("no profile for agent %q", agentID)
	}
	return p, nil
}

// ListCategories returns available tool categories.
func (n *Negotiator) ListCategories() []map[string]any {
	catCounts := make(map[ToolCategory]int)
	for _, t := range n.tools {
		catCounts[t.Category]++
	}

	var result []map[string]any
	for cat, count := range catCounts {
		result = append(result, map[string]any{
			"category": string(cat),
			"tools":    count,
		})
	}
	return result
}

func (n *Negotiator) inferContextCategories(context []string) []ToolCategory {
	var extra []ToolCategory
	for _, ctx := range context {
		lower := strings.ToLower(ctx)
		switch {
		case strings.Contains(lower, "lag") || strings.Contains(lower, "consumer"):
			extra = append(extra, CatMonitoring)
		case strings.Contains(lower, "schema"):
			extra = append(extra, CatSchema)
		case strings.Contains(lower, "incident") || strings.Contains(lower, "alert"):
			extra = append(extra, CatMonitoring, CatAnalytics)
		case strings.Contains(lower, "cost"):
			extra = append(extra, CatAnalytics)
		case strings.Contains(lower, "deploy") || strings.Contains(lower, "provision"):
			extra = append(extra, CatPlatform)
		}
	}
	return extra
}

func defaultRoleMapping() map[Role][]ToolCategory {
	return map[Role][]ToolCategory{
		RoleViewer:   {CatDiscovery, CatChat},
		RoleAnalyst:  {CatDiscovery, CatMonitoring, CatAnalytics, CatChat},
		RoleOperator: {CatDiscovery, CatMonitoring, CatStreaming, CatSchema, CatChat},
		RoleSRE:      {CatDiscovery, CatMonitoring, CatStreaming, CatAnalytics, CatAdmin, CatChat},
		RoleSecurity: {CatDiscovery, CatMonitoring, CatGovernance, CatChat},
		RoleAdmin:    {CatDiscovery, CatMonitoring, CatStreaming, CatSchema, CatAnalytics, CatGovernance, CatOrchestration, CatPlatform, CatAdmin, CatChat},
	}
}

func defaultToolRegistry() []ToolInfo {
	return []ToolInfo{
		// Discovery (Tier 0)
		{Name: "kafka_list_topics", Category: CatDiscovery, Tier: 0, ReadOnly: true, Description: "List Kafka topics"},
		{Name: "kafka_cluster_info", Category: CatDiscovery, Tier: 0, ReadOnly: true, Description: "Get cluster information"},
		{Name: "kafka_topic_describe", Category: CatDiscovery, Tier: 0, ReadOnly: true, Description: "Describe topic details"},
		// Monitoring (Tier 0)
		{Name: "kafka_consumer_groups", Category: CatMonitoring, Tier: 0, ReadOnly: true, Description: "List consumer groups"},
		{Name: "kafka_lag_diagnosis", Category: CatMonitoring, Tier: 0, ReadOnly: true, Description: "Diagnose consumer lag"},
		{Name: "kafka_selfheal_status", Category: CatMonitoring, Tier: 0, ReadOnly: true, Description: "Self-healing status"},
		// Streaming (Tier 1)
		{Name: "kafka_consume", Category: CatStreaming, Tier: 1, ReadOnly: true, Description: "Consume messages"},
		{Name: "kafka_produce", Category: CatStreaming, Tier: 1, ReadOnly: false, Description: "Produce messages"},
		{Name: "kafka_search", Category: CatStreaming, Tier: 1, ReadOnly: true, Description: "Search messages"},
		{Name: "kafka_watch", Category: CatStreaming, Tier: 1, ReadOnly: true, Description: "Watch for events"},
		// Schema (Tier 1)
		{Name: "kafka_list_schemas", Category: CatSchema, Tier: 1, ReadOnly: true, Description: "List schemas"},
		{Name: "kafka_schema_versions", Category: CatSchema, Tier: 1, ReadOnly: true, Description: "Get schema versions"},
		{Name: "kafka_schema_compatibility", Category: CatSchema, Tier: 1, ReadOnly: true, Description: "Check schema compatibility"},
		// Analytics (Tier 2)
		{Name: "kafka_anomaly_detect", Category: CatAnalytics, Tier: 2, ReadOnly: true, Description: "Detect anomalies"},
		{Name: "kafka_forecast_topic", Category: CatAnalytics, Tier: 2, ReadOnly: true, Description: "Forecast capacity"},
		{Name: "kafka_cost_analyze", Category: CatAnalytics, Tier: 2, ReadOnly: true, Description: "Analyze costs"},
		{Name: "kafka_timeseries_query", Category: CatAnalytics, Tier: 2, ReadOnly: true, Description: "Query time-series"},
		// Admin (Tier 2)
		{Name: "kafka_create_topic", Category: CatAdmin, Tier: 2, ReadOnly: false, Description: "Create topic"},
		{Name: "kafka_delete_topic", Category: CatAdmin, Tier: 2, ReadOnly: false, Description: "Delete topic"},
		{Name: "kafka_alter_topic", Category: CatAdmin, Tier: 2, ReadOnly: false, Description: "Alter topic config"},
		{Name: "kafka_reset_offsets", Category: CatAdmin, Tier: 2, ReadOnly: false, Description: "Reset consumer offsets"},
		// Governance (Tier 2)
		{Name: "kafka_lineage_trace", Category: CatGovernance, Tier: 2, ReadOnly: true, Description: "Trace data lineage"},
		{Name: "kafka_audit_log", Category: CatGovernance, Tier: 2, ReadOnly: true, Description: "View audit log"},
		{Name: "kafka_data_contracts", Category: CatGovernance, Tier: 2, ReadOnly: true, Description: "Manage data contracts"},
		// Orchestration (Tier 3)
		{Name: "kafka_orchestration_create", Category: CatOrchestration, Tier: 3, ReadOnly: false, Description: "Create orchestration"},
		{Name: "kafka_smart_route", Category: CatOrchestration, Tier: 3, ReadOnly: true, Description: "Smart tool routing"},
		// Platform (Tier 3)
		{Name: "kafka_controlplane_org", Category: CatPlatform, Tier: 3, ReadOnly: false, Description: "Manage organizations"},
		{Name: "kafka_controlplane_instance", Category: CatPlatform, Tier: 3, ReadOnly: false, Description: "Manage instances"},
		// Chat (Tier 0)
		{Name: "kafka_chat_session", Category: CatChat, Tier: 0, ReadOnly: false, Description: "Chat sessions"},
		{Name: "kafka_chat_message", Category: CatChat, Tier: 0, ReadOnly: false, Description: "Chat messages"},
		{Name: "kafka_discover_tools", Category: CatDiscovery, Tier: 0, ReadOnly: true, Description: "Discover additional tools"},
	}
}
