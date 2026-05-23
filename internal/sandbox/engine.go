// Package sandbox provides isolated sandbox environments for safe experimentation
// with Kafka operations including ephemeral topics, resource quotas, and auto-cleanup.
package sandbox

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/randid"
)

// Sandbox represents an isolated environment for safe experimentation.
type Sandbox struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Owner       string            `json:"owner"`
	Status      string            `json:"status"` // active, paused, expired, destroyed
	Topics      []string          `json:"topics"`
	Schemas     []string          `json:"schemas,omitempty"`
	Quotas      *ResourceQuotas   `json:"quotas"`
	Usage       *ResourceUsage    `json:"usage"`
	CreatedAt   time.Time         `json:"created_at"`
	ExpiresAt   time.Time         `json:"expires_at"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	Operations  []SandboxOp       `json:"operations,omitempty"`
}

// ResourceQuotas defines resource limits for a sandbox.
type ResourceQuotas struct {
	MaxTopics       int   `json:"max_topics"`
	MaxPartitions   int   `json:"max_partitions_per_topic"`
	MaxMessages     int64 `json:"max_messages"`
	MaxStorageBytes int64 `json:"max_storage_bytes"`
	MaxDurationSec  int64 `json:"max_duration_sec"`
}

// ResourceUsage tracks actual resource consumption.
type ResourceUsage struct {
	TopicCount       int   `json:"topic_count"`
	TotalPartitions  int   `json:"total_partitions"`
	MessagesProduced int64 `json:"messages_produced"`
	MessagesConsumed int64 `json:"messages_consumed"`
	StorageBytesUsed int64 `json:"storage_bytes_used"`
	OperationCount   int   `json:"operation_count"`
}

// SandboxOp records an operation performed in a sandbox.
type SandboxOp struct {
	Tool      string         `json:"tool"`
	Args      map[string]any `json:"args,omitempty"`
	Status    string         `json:"status"` // success, error
	Result    any            `json:"result,omitempty"`
	Error     string         `json:"error,omitempty"`
	Duration  string         `json:"duration"`
	Timestamp time.Time      `json:"timestamp"`
}

// PromotionReport summarizes sandbox usage for promotion decisions.
type PromotionReport struct {
	SandboxID     string         `json:"sandbox_id"`
	SandboxName   string         `json:"sandbox_name"`
	Owner         string         `json:"owner"`
	Duration      string         `json:"duration"`
	Usage         *ResourceUsage `json:"usage"`
	TopOperations map[string]int `json:"top_operations"`
	Topics        []string       `json:"topics"`
	Schemas       []string       `json:"schemas"`
	Ready         bool           `json:"ready_for_promotion"`
	Warnings      []string       `json:"warnings,omitempty"`
}

// Engine manages sandbox lifecycle and operations.
type Engine struct {
	mu          sync.RWMutex
	sandboxes   map[string]*Sandbox
	logger      *slog.Logger
	stopCleanup chan struct{}
}

// NewEngine creates a new sandbox engine.
func NewEngine(logger *slog.Logger) *Engine {
	e := &Engine{
		sandboxes:   make(map[string]*Sandbox),
		logger:      logger,
		stopCleanup: make(chan struct{}),
	}
	go e.cleanupLoop()
	return e
}

// cleanupLoop periodically removes destroyed/expired sandboxes older than 1 hour.
func (e *Engine) cleanupLoop() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			e.purgeStale()
		case <-e.stopCleanup:
			return
		}
	}
}

// purgeStale removes sandboxes that have been destroyed or expired for over 1 hour.
func (e *Engine) purgeStale() {
	e.mu.Lock()
	defer e.mu.Unlock()

	cutoff := time.Now().Add(-1 * time.Hour)
	for id, sb := range e.sandboxes {
		if (sb.Status == "destroyed" || sb.Status == "expired") && sb.ExpiresAt.Before(cutoff) {
			delete(e.sandboxes, id)
			e.logger.Debug("purged stale sandbox", "id", id, "status", sb.Status)
		}
	}
}

// Close stops the cleanup goroutine.
func (e *Engine) Close() {
	close(e.stopCleanup)
}

// Create creates a new sandbox environment.
func (e *Engine) Create(name, description, owner string, durationSec int64, quotas *ResourceQuotas) (*Sandbox, error) {
	if name == "" || owner == "" {
		return nil, fmt.Errorf("name and owner are required")
	}

	if durationSec <= 0 {
		durationSec = 3600 // 1 hour default
	}
	if durationSec > 86400 {
		durationSec = 86400 // max 24 hours
	}

	if quotas == nil {
		quotas = &ResourceQuotas{
			MaxTopics:       10,
			MaxPartitions:   3,
			MaxMessages:     10000,
			MaxStorageBytes: 100 * 1024 * 1024, // 100MB
			MaxDurationSec:  durationSec,
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	id := randid.New("sandbox")

	sb := &Sandbox{
		ID:          id,
		Name:        name,
		Description: description,
		Owner:       owner,
		Status:      "active",
		Topics:      make([]string, 0),
		Quotas:      quotas,
		Usage:       &ResourceUsage{},
		CreatedAt:   time.Now(),
		ExpiresAt:   time.Now().Add(time.Duration(durationSec) * time.Second),
		Metadata:    make(map[string]string),
	}

	e.sandboxes[id] = sb
	e.logger.Info("sandbox created", "id", id, "name", name, "owner", owner)
	return sb, nil
}

// Exec records an operation executed within a sandbox.
func (e *Engine) Exec(sandboxID, tool string, args map[string]any, result any, errMsg string, duration time.Duration) (*SandboxOp, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	sb, ok := e.sandboxes[sandboxID]
	if !ok {
		return nil, fmt.Errorf("sandbox %q not found", sandboxID)
	}
	if sb.Status != "active" {
		return nil, fmt.Errorf("sandbox %q is %s", sandboxID, sb.Status)
	}
	if time.Now().After(sb.ExpiresAt) {
		sb.Status = "expired"
		return nil, fmt.Errorf("sandbox %q has expired", sandboxID)
	}

	status := "success"
	if errMsg != "" {
		status = "error"
	}

	op := SandboxOp{
		Tool:      tool,
		Args:      redactSensitiveArgs(args),
		Status:    status,
		Result:    result,
		Error:     errMsg,
		Duration:  duration.String(),
		Timestamp: time.Now(),
	}

	sb.Operations = append(sb.Operations, op)
	sb.Usage.OperationCount++

	// Track topic creation
	if tool == "kafka_create_topic" {
		if topicName, ok := args["topic"].(string); ok {
			sb.Topics = append(sb.Topics, topicName)
			sb.Usage.TopicCount++
		}
	}

	// Track messages
	if tool == "kafka_produce" {
		sb.Usage.MessagesProduced++
	}
	if tool == "kafka_consume" {
		sb.Usage.MessagesConsumed++
	}

	// Check quotas
	if sb.Usage.TopicCount > sb.Quotas.MaxTopics {
		return nil, fmt.Errorf("sandbox quota exceeded: max %d topics", sb.Quotas.MaxTopics)
	}
	if sb.Usage.MessagesProduced > sb.Quotas.MaxMessages {
		return nil, fmt.Errorf("sandbox quota exceeded: max %d messages", sb.Quotas.MaxMessages)
	}

	return &op, nil
}

// Destroy marks a sandbox as destroyed.
func (e *Engine) Destroy(sandboxID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	sb, ok := e.sandboxes[sandboxID]
	if !ok {
		return fmt.Errorf("sandbox %q not found", sandboxID)
	}
	sb.Status = "destroyed"
	return nil
}

// Get returns a sandbox by ID.
func (e *Engine) Get(sandboxID string) (*Sandbox, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	sb, ok := e.sandboxes[sandboxID]
	if !ok {
		return nil, fmt.Errorf("sandbox %q not found", sandboxID)
	}
	return sb, nil
}

// List returns all sandboxes, optionally filtered by owner.
func (e *Engine) List(owner string) []Sandbox {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []Sandbox
	for _, sb := range e.sandboxes {
		if owner != "" && sb.Owner != owner {
			continue
		}
		result = append(result, *sb)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.After(result[j].CreatedAt)
	})
	return result
}

// GeneratePromotionReport generates a report for promoting sandbox resources to production.
func (e *Engine) GeneratePromotionReport(sandboxID string) (*PromotionReport, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	sb, ok := e.sandboxes[sandboxID]
	if !ok {
		return nil, fmt.Errorf("sandbox %q not found", sandboxID)
	}

	report := &PromotionReport{
		SandboxID:     sb.ID,
		SandboxName:   sb.Name,
		Owner:         sb.Owner,
		Duration:      time.Since(sb.CreatedAt).String(),
		Usage:         sb.Usage,
		TopOperations: make(map[string]int),
		Topics:        sb.Topics,
		Schemas:       sb.Schemas,
	}

	for _, op := range sb.Operations {
		report.TopOperations[op.Tool]++
	}

	// Check readiness
	report.Ready = true
	if sb.Usage.OperationCount == 0 {
		report.Ready = false
		report.Warnings = append(report.Warnings, "No operations performed in sandbox")
	}

	errorCount := 0
	for _, op := range sb.Operations {
		if op.Status == "error" {
			errorCount++
		}
	}
	if errorCount > 0 {
		errRate := float64(errorCount) / float64(len(sb.Operations)) * 100
		if errRate > 20 {
			report.Ready = false
			report.Warnings = append(report.Warnings, fmt.Sprintf("High error rate: %.1f%%", errRate))
		}
	}

	return report, nil
}

// CleanExpired marks expired sandboxes and returns count.
func (e *Engine) CleanExpired() int {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()
	cleaned := 0
	for _, sb := range e.sandboxes {
		if sb.Status == "active" && now.After(sb.ExpiresAt) {
			sb.Status = "expired"
			cleaned++
		}
	}
	return cleaned
}

// sensitiveKeyPatterns contains substrings that indicate a parameter key holds sensitive data.
var sensitiveKeyPatterns = []string{"password", "secret", "token", "key", "credential", "api_key", "apikey", "sasl", "bearer", "jwt", "auth", "connection_string"}

// redactSensitiveArgs returns a shallow copy of args with sensitive keys redacted.
func redactSensitiveArgs(args map[string]any) map[string]any {
	if args == nil {
		return nil
	}
	redacted := make(map[string]any, len(args))
	for k, v := range args {
		lower := strings.ToLower(k)
		isSensitive := false
		for _, pat := range sensitiveKeyPatterns {
			if strings.Contains(lower, pat) {
				isSensitive = true
				break
			}
		}
		if isSensitive {
			redacted[k] = "**REDACTED**"
		} else if nested, ok := v.(map[string]any); ok {
			redacted[k] = redactSensitiveArgs(nested)
		} else {
			redacted[k] = v
		}
	}
	return redacted
}
