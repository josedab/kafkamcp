package cloud

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Workspace represents a team workspace in KafkaMCP Cloud.
type Workspace struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	OwnerEmail   string            `json:"owner_email"`
	Plan         string            `json:"plan"` // free, pro, enterprise
	Clusters     []ClusterConnection `json:"clusters"`
	Members      []Member          `json:"members"`
	Usage        UsageStats        `json:"usage"`
	CreatedAt    time.Time         `json:"created_at"`
	Status       string            `json:"status"` // active, suspended, deleted
	Settings     map[string]string `json:"settings,omitempty"`
}

// ClusterConnection represents a managed Kafka cluster connection.
type ClusterConnection struct {
	ID               string `json:"id"`
	Name             string `json:"name"`
	BootstrapServers string `json:"bootstrap_servers"`
	SecurityProtocol string `json:"security_protocol"`
	Region           string `json:"region,omitempty"`
	Status           string `json:"status"` // connected, disconnected, error
	LastPingAt       *time.Time `json:"last_ping_at,omitempty"`
}

// Member represents a workspace member.
type Member struct {
	Email    string `json:"email"`
	Role     string `json:"role"` // owner, admin, member, viewer
	JoinedAt time.Time `json:"joined_at"`
}

// UsageStats tracks workspace usage.
type UsageStats struct {
	ToolCalls        int64     `json:"tool_calls"`
	MessagesConsumed int64     `json:"messages_consumed"`
	MessagesProduced int64     `json:"messages_produced"`
	ActiveAgents     int       `json:"active_agents"`
	PeriodStart      time.Time `json:"period_start"`
}

// PlanLimits defines limits for each plan tier.
type PlanLimits struct {
	MaxClusters      int   `json:"max_clusters"`
	MaxMembers       int   `json:"max_members"`
	MaxToolCalls     int64 `json:"max_tool_calls_month"`
	MaxMessages      int64 `json:"max_messages_day"`
	MaxAgents        int   `json:"max_agents"`
	SSO              bool  `json:"sso"`
	AuditLog         bool  `json:"audit_log"`
	CustomBranding   bool  `json:"custom_branding"`
}

// GetPlanLimits returns the limits for a given plan.
func GetPlanLimits(plan string) PlanLimits {
	switch plan {
	case "free":
		return PlanLimits{
			MaxClusters:  3,
			MaxMembers:   5,
			MaxToolCalls: 50000,
			MaxMessages:  10000,
			MaxAgents:    5,
		}
	case "pro":
		return PlanLimits{
			MaxClusters:  20,
			MaxMembers:   50,
			MaxToolCalls: 5000000,
			MaxMessages:  1000000,
			MaxAgents:    50,
			AuditLog:     true,
		}
	case "enterprise":
		return PlanLimits{
			MaxClusters:  -1, // unlimited
			MaxMembers:   -1,
			MaxToolCalls: -1,
			MaxMessages:  -1,
			MaxAgents:    -1,
			SSO:          true,
			AuditLog:     true,
			CustomBranding: true,
		}
	default:
		return GetPlanLimits("free")
	}
}

// BillingInfo tracks billing state.
type BillingInfo struct {
	Plan           string    `json:"plan"`
	StripeCustomer string    `json:"stripe_customer_id,omitempty"`
	BillingCycle   string    `json:"billing_cycle"` // monthly, yearly
	NextBillingAt  time.Time `json:"next_billing_at"`
	AmountCents    int64     `json:"amount_cents"`
}

// CloudService manages the KafkaMCP Cloud platform.
type CloudService struct {
	workspaces map[string]*Workspace
	apiKeys    map[string]string // key hash -> workspace ID
	mu         sync.RWMutex
	logger     *slog.Logger
}

// NewCloudService creates a new cloud service.
func NewCloudService(logger *slog.Logger) *CloudService {
	return &CloudService{
		workspaces: make(map[string]*Workspace),
		apiKeys:    make(map[string]string),
		logger:     logger,
	}
}

// CreateWorkspace creates a new workspace.
func (cs *CloudService) CreateWorkspace(name, ownerEmail, plan string) (*Workspace, string, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if name == "" || ownerEmail == "" {
		return nil, "", fmt.Errorf("name and owner email are required")
	}
	if plan == "" {
		plan = "free"
	}

	wsID, err := generateID("ws")
	if err != nil {
		return nil, "", fmt.Errorf("generate workspace ID: %w", err)
	}

	ws := &Workspace{
		ID:         wsID,
		Name:       name,
		OwnerEmail: ownerEmail,
		Plan:       plan,
		Members:    []Member{{Email: ownerEmail, Role: "owner", JoinedAt: time.Now()}},
		Usage:      UsageStats{PeriodStart: time.Now()},
		CreatedAt:  time.Now(),
		Status:     "active",
	}

	// Generate API key
	apiKey, err := generateAPIKey()
	if err != nil {
		return nil, "", fmt.Errorf("generate API key: %w", err)
	}
	keyHash := hashKey(apiKey)
	cs.apiKeys[keyHash] = ws.ID
	cs.workspaces[ws.ID] = ws

	return ws, apiKey, nil
}

// GetWorkspace retrieves a workspace by ID.
func (cs *CloudService) GetWorkspace(id string) (*Workspace, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	ws, ok := cs.workspaces[id]
	if !ok {
		return nil, fmt.Errorf("workspace %q not found", id)
	}
	return ws, nil
}

// AddCluster adds a cluster connection to a workspace.
func (cs *CloudService) AddCluster(workspaceID string, conn ClusterConnection) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	ws, ok := cs.workspaces[workspaceID]
	if !ok {
		return fmt.Errorf("workspace %q not found", workspaceID)
	}

	limits := GetPlanLimits(ws.Plan)
	if limits.MaxClusters > 0 && len(ws.Clusters) >= limits.MaxClusters {
		return fmt.Errorf("cluster limit reached for %s plan (max %d)", ws.Plan, limits.MaxClusters)
	}

	clID, err := generateID("cl")
	if err != nil {
		return fmt.Errorf("generate cluster ID: %w", err)
	}
	conn.ID = clID
	conn.Status = "connected"
	ws.Clusters = append(ws.Clusters, conn)
	return nil
}

// AuthenticateAPIKey validates an API key and returns the workspace ID.
func (cs *CloudService) AuthenticateAPIKey(apiKey string) (string, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	keyHash := hashKey(apiKey)
	wsID, ok := cs.apiKeys[keyHash]
	if !ok {
		return "", fmt.Errorf("invalid API key")
	}
	return wsID, nil
}

// RecordUsage increments usage counters for a workspace.
func (cs *CloudService) RecordUsage(workspaceID string, toolCalls, consumed, produced int64) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	ws, ok := cs.workspaces[workspaceID]
	if !ok {
		return fmt.Errorf("workspace %q not found", workspaceID)
	}

	ws.Usage.ToolCalls += toolCalls
	ws.Usage.MessagesConsumed += consumed
	ws.Usage.MessagesProduced += produced

	// Check limits
	limits := GetPlanLimits(ws.Plan)
	if limits.MaxToolCalls > 0 && ws.Usage.ToolCalls > limits.MaxToolCalls {
		return fmt.Errorf("tool call limit exceeded for %s plan", ws.Plan)
	}

	return nil
}

// ListWorkspaces returns all workspaces (for admin).
func (cs *CloudService) ListWorkspaces() []*Workspace {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	var result []*Workspace
	for _, ws := range cs.workspaces {
		result = append(result, ws)
	}
	return result
}

// DeleteWorkspace marks a workspace as deleted.
func (cs *CloudService) DeleteWorkspace(id string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	ws, ok := cs.workspaces[id]
	if !ok {
		return fmt.Errorf("workspace %q not found", id)
	}
	ws.Status = "deleted"
	return nil
}

// AddMember adds a member to a workspace.
func (cs *CloudService) AddMember(workspaceID, email, role string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	ws, ok := cs.workspaces[workspaceID]
	if !ok {
		return fmt.Errorf("workspace %q not found", workspaceID)
	}

	if email == "" {
		return fmt.Errorf("email is required")
	}
	if role == "" {
		role = "member"
	}

	limits := GetPlanLimits(ws.Plan)
	if limits.MaxMembers > 0 && len(ws.Members) >= limits.MaxMembers {
		return fmt.Errorf("member limit reached for %s plan (max %d)", ws.Plan, limits.MaxMembers)
	}

	for _, m := range ws.Members {
		if m.Email == email {
			return fmt.Errorf("member %q already exists", email)
		}
	}

	ws.Members = append(ws.Members, Member{
		Email:    email,
		Role:     role,
		JoinedAt: time.Now(),
	})
	return nil
}

// UpgradePlan upgrades a workspace's plan.
func (cs *CloudService) UpgradePlan(workspaceID, newPlan string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	ws, ok := cs.workspaces[workspaceID]
	if !ok {
		return fmt.Errorf("workspace %q not found", workspaceID)
	}

	validPlans := map[string]bool{"free": true, "pro": true, "enterprise": true}
	if !validPlans[newPlan] {
		return fmt.Errorf("invalid plan: %s", newPlan)
	}

	ws.Plan = newPlan
	cs.logger.Info("workspace plan upgraded", "workspace", workspaceID, "plan", newPlan)
	return nil
}

// GetDashboardData returns data for the workspace dashboard.
func (cs *CloudService) GetDashboardData(workspaceID string) (map[string]any, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	ws, ok := cs.workspaces[workspaceID]
	if !ok {
		return nil, fmt.Errorf("workspace %q not found", workspaceID)
	}

	limits := GetPlanLimits(ws.Plan)

	return map[string]any{
		"workspace":      ws,
		"plan_limits":    limits,
		"clusters_count": len(ws.Clusters),
		"members_count":  len(ws.Members),
		"usage": map[string]any{
			"tool_calls":        ws.Usage.ToolCalls,
			"tool_calls_limit":  limits.MaxToolCalls,
			"tool_calls_pct":    usagePct(ws.Usage.ToolCalls, limits.MaxToolCalls),
			"messages_consumed": ws.Usage.MessagesConsumed,
			"messages_produced": ws.Usage.MessagesProduced,
		},
	}, nil
}

func usagePct(used, limit int64) float64 {
	if limit <= 0 {
		return 0
	}
	return float64(used) / float64(limit) * 100
}

func generateID(prefix string) (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand failed: %w", err)
	}
	return fmt.Sprintf("%s_%s", prefix, hex.EncodeToString(b)), nil
}

func generateAPIKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand failed: %w", err)
	}
	return fmt.Sprintf("kmcp_%s", hex.EncodeToString(b)), nil
}

func hashKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}
