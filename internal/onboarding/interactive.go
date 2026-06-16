package onboarding

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// OnboardingSession tracks a user's interactive onboarding progress.
type OnboardingSession struct {
	ID          string            `json:"id"`
	AgentID     string            `json:"agent_id"`
	CurrentStep int               `json:"current_step"`
	Steps       []InteractiveStep `json:"steps"`
	Context     SessionContext    `json:"context"`
	StartedAt   time.Time         `json:"started_at"`
	CompletedAt *time.Time        `json:"completed_at,omitempty"`
	Status      string            `json:"status"` // active, completed, abandoned
}

// InteractiveStep is an onboarding step with contextual guidance.
type InteractiveStep struct {
	Number      int      `json:"number"`
	Title       string   `json:"title"`
	Description string   `json:"description"`
	Guidance    string   `json:"guidance"`
	ToolToUse   string   `json:"tool_to_use,omitempty"`
	ToolArgs    string   `json:"tool_args,omitempty"`
	Validation  string   `json:"validation,omitempty"`
	Status      string   `json:"status"` // pending, active, completed, skipped, failed
	Result      string   `json:"result,omitempty"`
	Tips        []string `json:"tips,omitempty"`
}

// SessionContext holds discovered information during onboarding.
type SessionContext struct {
	ClusterFound      bool     `json:"cluster_found"`
	ClusterAddress    string   `json:"cluster_address,omitempty"`
	TopicsFound       int      `json:"topics_found"`
	TopicNames        []string `json:"topic_names,omitempty"`
	SchemaRegistry    bool     `json:"schema_registry"`
	ConsumerGroups    int      `json:"consumer_groups"`
	HealthCheckPassed bool     `json:"health_check_passed"`
	Capabilities      []string `json:"capabilities,omitempty"`
}

// StepAdvanceResult is returned when advancing through onboarding.
type StepAdvanceResult struct {
	Session     *OnboardingSession `json:"session"`
	NextStep    *InteractiveStep   `json:"next_step,omitempty"`
	Message     string             `json:"message"`
	Suggestions []string           `json:"suggestions,omitempty"`
}

// InteractiveOnboarder manages AI-guided onboarding sessions.
type InteractiveOnboarder struct {
	mu       sync.RWMutex
	sessions map[string]*OnboardingSession
	engine   *Engine
	nextID   int64
}

// NewInteractiveOnboarder creates a new interactive onboarder.
func NewInteractiveOnboarder(engine *Engine) *InteractiveOnboarder {
	return &InteractiveOnboarder{
		sessions: make(map[string]*OnboardingSession),
		engine:   engine,
	}
}

// StartSession creates a new onboarding session tailored to the agent's environment.
func (io *InteractiveOnboarder) StartSession(ctx context.Context, agentID string) *OnboardingSession {
	// Auto-discover environment outside the lock (involves network I/O)
	discovery := io.engine.DiscoverClusters(ctx)
	sessionCtx := SessionContext{}
	if len(discovery.Clusters) > 0 {
		sessionCtx.ClusterFound = true
		sessionCtx.ClusterAddress = discovery.Clusters[0].BootstrapServers
	}

	steps := io.buildSteps(sessionCtx)

	io.mu.Lock()
	defer io.mu.Unlock()

	io.nextID++
	sessionID := fmt.Sprintf("onboard_%d", io.nextID)

	session := &OnboardingSession{
		ID:          sessionID,
		AgentID:     agentID,
		CurrentStep: 0,
		Steps:       steps,
		Context:     sessionCtx,
		StartedAt:   time.Now(),
		Status:      "active",
	}

	if len(steps) > 0 {
		steps[0].Status = "active"
	}

	io.sessions[sessionID] = session
	return session
}

func (io *InteractiveOnboarder) buildSteps(ctx SessionContext) []InteractiveStep {
	var steps []InteractiveStep

	// Step 1: Cluster connectivity
	step1 := InteractiveStep{
		Number:      1,
		Title:       "Connect to Kafka",
		Description: "Verify connectivity to your Kafka cluster",
		Status:      "pending",
	}
	if ctx.ClusterFound {
		step1.Guidance = fmt.Sprintf("✅ Found Kafka at %s! Let's verify connectivity.", ctx.ClusterAddress)
		step1.ToolToUse = "kafka_list_topics"
		step1.ToolArgs = `{"cluster": "default"}`
		step1.Tips = []string{
			"KafkaMCP auto-discovered your cluster",
			"If this isn't the right cluster, update kafkamcp.yaml",
		}
	} else {
		step1.Guidance = "No Kafka cluster found automatically. Let's configure one."
		step1.ToolToUse = "kafka_onboard"
		step1.ToolArgs = `{"action": "discover"}`
		step1.Tips = []string{
			"Check if Kafka is running: docker ps | grep kafka",
			"Common local ports: 9092, 19092, 29092",
			"Set KAFKA_BOOTSTRAP_SERVERS environment variable",
		}
	}
	steps = append(steps, step1)

	// Step 2: Explore topics
	steps = append(steps, InteractiveStep{
		Number:      2,
		Title:       "Explore Your Topics",
		Description: "Discover what topics exist and understand your data",
		Guidance:    "Let's see what topics are in your cluster and pick one to explore.",
		ToolToUse:   "kafka_list_topics",
		ToolArgs:    `{"cluster": "default"}`,
		Status:      "pending",
		Tips: []string{
			"Look for topics with names that suggest business events",
			"Topics starting with '__' are internal Kafka topics",
			"If no topics exist, we'll create a test topic",
		},
	})

	// Step 3: Read some messages
	steps = append(steps, InteractiveStep{
		Number:      3,
		Title:       "Read Your First Messages",
		Description: "Consume messages from a topic to understand the data format",
		Guidance:    "Pick a topic from the previous step and read some recent messages.",
		ToolToUse:   "kafka_consume",
		ToolArgs:    `{"topic": "<your_topic>", "offset": "latest", "limit": 5}`,
		Status:      "pending",
		Tips: []string{
			"Use 'offset: latest' to see the most recent messages",
			"Check the message format — is it JSON, Avro, Protobuf?",
			"Note the key patterns — they determine partitioning",
		},
	})

	// Step 4: Produce a test message
	steps = append(steps, InteractiveStep{
		Number:      4,
		Title:       "Produce a Test Message",
		Description: "Send your first message through KafkaMCP",
		Guidance:    "Let's produce a test message to verify write access.",
		ToolToUse:   "kafka_produce",
		ToolArgs:    `{"topic": "kafkamcp-test", "key": "test-1", "value": "{\"message\": \"Hello from KafkaMCP!\", \"timestamp\": \"now\"}"}`,
		Status:      "pending",
		Tips: []string{
			"This creates a test topic if it doesn't exist",
			"You can consume this message back to verify round-trip",
			"In production, use your real topic names",
		},
	})

	// Step 5: Monitor consumer groups
	steps = append(steps, InteractiveStep{
		Number:      5,
		Title:       "Monitor Consumer Groups",
		Description: "Check the health of consumer groups consuming from your topics",
		Guidance:    "Consumer group monitoring helps you detect lag and unhealthy consumers.",
		ToolToUse:   "kafka_list_consumer_groups",
		ToolArgs:    `{"cluster": "default"}`,
		Status:      "pending",
		Tips: []string{
			"Watch for consumer groups with high lag",
			"Empty consumer groups may indicate stale configurations",
			"Use kafka_describe_consumer_group for detailed lag info",
		},
	})

	// Step 6: Set up RBAC
	steps = append(steps, InteractiveStep{
		Number:      6,
		Title:       "Configure Security",
		Description: "Set up role-based access control for your agents",
		Guidance:    "Good security practices start from day one. Let's configure RBAC.",
		ToolToUse:   "kafka_onboard",
		ToolArgs:    `{"action": "suggest_rbac"}`,
		Status:      "pending",
		Tips: []string{
			"Start with least-privilege access",
			"Create separate agent IDs for different use cases",
			"Enable audit logging to track all agent actions",
		},
	})

	// Step 7: Graduation
	steps = append(steps, InteractiveStep{
		Number:      7,
		Title:       "🎓 Onboarding Complete!",
		Description: "You're ready to use KafkaMCP with your AI agents",
		Guidance:    "Congratulations! Here's what you can do next.",
		Status:      "pending",
		Tips: []string{
			"Bounded consume: kafka_consume",
			"Search messages: kafka_search",
			"Diagnose lag: kafka_diagnose_lag",
			"Discover tools: kafka_discover_tools",
		},
	})

	return steps
}

// AdvanceStep marks the current step as completed and returns the next step.
func (io *InteractiveOnboarder) AdvanceStep(sessionID, result string) (*StepAdvanceResult, error) {
	io.mu.Lock()
	defer io.mu.Unlock()

	session, ok := io.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %q not found", sessionID)
	}
	if session.Status != "active" {
		return nil, fmt.Errorf("session is not active (status: %s)", session.Status)
	}

	if session.CurrentStep < len(session.Steps) {
		session.Steps[session.CurrentStep].Status = "completed"
		session.Steps[session.CurrentStep].Result = result
	}

	session.CurrentStep++

	advResult := &StepAdvanceResult{Session: session}

	if session.CurrentStep >= len(session.Steps) {
		now := time.Now()
		session.Status = "completed"
		session.CompletedAt = &now
		advResult.Message = "🎉 Onboarding complete! You're all set to use KafkaMCP."
		advResult.Suggestions = []string{
			"Discover all available tools with kafka_discover_tools",
			"Analyze dead-letter queues with kafka_analyze_dlq",
			"Set up monitoring dashboards",
		}
	} else {
		session.Steps[session.CurrentStep].Status = "active"
		advResult.NextStep = &session.Steps[session.CurrentStep]
		advResult.Message = fmt.Sprintf("Step %d completed! Moving to: %s",
			session.CurrentStep, session.Steps[session.CurrentStep].Title)
	}

	return advResult, nil
}

// SkipStep skips the current step.
func (io *InteractiveOnboarder) SkipStep(sessionID string) (*StepAdvanceResult, error) {
	io.mu.Lock()
	defer io.mu.Unlock()

	session, ok := io.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %q not found", sessionID)
	}

	if session.CurrentStep < len(session.Steps) {
		session.Steps[session.CurrentStep].Status = "skipped"
	}
	session.CurrentStep++

	advResult := &StepAdvanceResult{Session: session}
	if session.CurrentStep < len(session.Steps) {
		session.Steps[session.CurrentStep].Status = "active"
		advResult.NextStep = &session.Steps[session.CurrentStep]
		advResult.Message = fmt.Sprintf("Skipped. Now on: %s", session.Steps[session.CurrentStep].Title)
	}
	return advResult, nil
}

// GetSession returns a session by ID.
func (io *InteractiveOnboarder) GetSession(sessionID string) (*OnboardingSession, error) {
	io.mu.RLock()
	defer io.mu.RUnlock()
	session, ok := io.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %q not found", sessionID)
	}
	return session, nil
}

// GetCurrentStep returns the current step with contextual guidance.
func (io *InteractiveOnboarder) GetCurrentStep(sessionID string) (*InteractiveStep, error) {
	io.mu.RLock()
	defer io.mu.RUnlock()
	session, ok := io.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %q not found", sessionID)
	}
	if session.CurrentStep >= len(session.Steps) {
		return nil, fmt.Errorf("onboarding is complete")
	}
	return &session.Steps[session.CurrentStep], nil
}

// UpdateContext updates the session context with discovered information.
func (io *InteractiveOnboarder) UpdateContext(sessionID string, update func(*SessionContext)) error {
	io.mu.Lock()
	defer io.mu.Unlock()
	session, ok := io.sessions[sessionID]
	if !ok {
		return fmt.Errorf("session %q not found", sessionID)
	}
	update(&session.Context)
	return nil
}

// GetProgress returns a progress summary.
func (io *InteractiveOnboarder) GetProgress(sessionID string) map[string]any {
	io.mu.RLock()
	defer io.mu.RUnlock()
	session, ok := io.sessions[sessionID]
	if !ok {
		return map[string]any{"error": "session not found"}
	}

	completed := 0
	skipped := 0
	for _, s := range session.Steps {
		if s.Status == "completed" {
			completed++
		}
		if s.Status == "skipped" {
			skipped++
		}
	}

	return map[string]any{
		"session_id":   session.ID,
		"status":       session.Status,
		"current_step": session.CurrentStep + 1,
		"total_steps":  len(session.Steps),
		"completed":    completed,
		"skipped":      skipped,
		"progress_pct": float64(completed+skipped) / float64(len(session.Steps)) * 100,
		"duration":     time.Since(session.StartedAt).String(),
	}
}

// GenerateContextualHelp provides help based on the current step and context.
func GenerateContextualHelp(step InteractiveStep, ctx SessionContext) string {
	var parts []string
	parts = append(parts, fmt.Sprintf("## Step %d: %s\n", step.Number, step.Title))
	parts = append(parts, step.Guidance+"\n")

	if step.ToolToUse != "" {
		parts = append(parts, fmt.Sprintf("**Try this:** Use `%s` with:\n```json\n%s\n```\n", step.ToolToUse, step.ToolArgs))
	}

	if len(step.Tips) > 0 {
		parts = append(parts, "**Tips:**")
		for _, tip := range step.Tips {
			parts = append(parts, "- "+tip)
		}
	}

	return strings.Join(parts, "\n")
}
