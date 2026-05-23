package gitops

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/randid"
)

// ReconcileStatus represents the state of a reconciliation run.
type ReconcileStatus string

// ReconcileStatusPending and related values define constants for this package.
const (
	ReconcileStatusPending ReconcileStatus = "pending"
	ReconcileStatusRunning ReconcileStatus = "running"
	ReconcileStatusSuccess ReconcileStatus = "success"
	ReconcileStatusFailed  ReconcileStatus = "failed"
	ReconcileStatusDryRun  ReconcileStatus = "dry_run"
)

// ReconcileResult records the outcome of a reconciliation run.
type ReconcileResult struct {
	ID           string          `json:"id"`
	Status       ReconcileStatus `json:"status"`
	DryRun       bool            `json:"dry_run"`
	Diff         *DiffResult     `json:"diff,omitempty"`
	Plan         *MigrationPlan  `json:"plan,omitempty"`
	StepsApplied int             `json:"steps_applied"`
	StepsFailed  int             `json:"steps_failed"`
	Error        string          `json:"error,omitempty"`
	StartedAt    time.Time       `json:"started_at"`
	CompletedAt  time.Time       `json:"completed_at,omitempty"`
	Duration     time.Duration   `json:"duration,omitempty"`
}

// WebhookEvent represents a GitHub/GitLab webhook payload.
type WebhookEvent struct {
	Type       string `json:"type"` // "push", "pull_request"
	Ref        string `json:"ref"`
	Repository string `json:"repository"`
	Sender     string `json:"sender"`
	Action     string `json:"action,omitempty"` // "opened", "closed", "merged"
	PRNumber   int    `json:"pr_number,omitempty"`
	HeadSHA    string `json:"head_sha,omitempty"`
}

// ReconcilerConfig configures the reconciliation controller.
type ReconcilerConfig struct {
	Interval      time.Duration `json:"interval" yaml:"interval"`
	DryRunDefault bool          `json:"dry_run_default" yaml:"dry_run_default"`
	AutoApprove   bool          `json:"auto_approve" yaml:"auto_approve"`
	MaxHistory    int           `json:"max_history" yaml:"max_history"`
	WebhookSecret string        `json:"-" yaml:"webhook_secret"`
	GitRepo       string        `json:"git_repo" yaml:"git_repo"`
	GitBranch     string        `json:"git_branch" yaml:"git_branch"`
	StatePath     string        `json:"state_path" yaml:"state_path"`
}

// DefaultReconcilerConfig returns sensible defaults.
func DefaultReconcilerConfig() ReconcilerConfig {
	return ReconcilerConfig{
		Interval:      60 * time.Second,
		DryRunDefault: true,
		MaxHistory:    100,
		GitBranch:     "main",
		StatePath:     "kafka-state.yaml",
	}
}

// Reconciler continuously watches for drift and reconciles state.
type Reconciler struct {
	mu      sync.RWMutex
	config  ReconcilerConfig
	manager *Manager
	results []ReconcileResult
	desired *DesiredState
	running bool
	cancel  context.CancelFunc
	logger  *slog.Logger
}

// NewReconciler creates a new reconciliation controller.
func NewReconciler(cfg ReconcilerConfig, mgr *Manager, logger *slog.Logger) *Reconciler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Reconciler{
		config:  cfg,
		manager: mgr,
		results: make([]ReconcileResult, 0),
		logger:  logger,
	}
}

// SetDesiredState updates the target state for reconciliation.
func (r *Reconciler) SetDesiredState(state DesiredState) error {
	validationErrs := r.manager.ValidateDesiredState(state)
	if len(validationErrs) > 0 {
		return fmt.Errorf("invalid desired state: %v", validationErrs)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.desired = &state
	return nil
}

// Reconcile runs a single reconciliation against the live state.
func (r *Reconciler) Reconcile(ctx context.Context, live LiveState, dryRun bool) (*ReconcileResult, error) {
	r.mu.RLock()
	desired := r.desired
	r.mu.RUnlock()

	if desired == nil {
		return nil, fmt.Errorf("no desired state configured")
	}

	result := &ReconcileResult{
		ID:        randid.New("reconcile"),
		DryRun:    dryRun,
		StartedAt: time.Now(),
	}

	// Compute diff
	diff, err := r.manager.Diff(*desired, live)
	if err != nil {
		result.Status = ReconcileStatusFailed
		result.Error = err.Error()
		result.CompletedAt = time.Now()
		r.recordResult(*result)
		return result, err
	}
	result.Diff = diff

	// Generate plan
	plan := r.manager.GeneratePlan(diff, dryRun)
	result.Plan = plan

	if dryRun {
		result.Status = ReconcileStatusDryRun
		result.StepsApplied = 0
		result.CompletedAt = time.Now()
		result.Duration = result.CompletedAt.Sub(result.StartedAt)
		r.recordResult(*result)
		r.logger.Info("reconciliation dry run complete",
			"changes", diff.TotalChanges,
			"steps", len(plan.Steps),
		)
		return result, nil
	}

	// Apply plan (in production, this would call kafka admin operations)
	result.Status = ReconcileStatusRunning
	for _, step := range plan.Steps {
		// Destructive operations require manual approval unless auto_approve is set
		if step.Action == "delete_topic" && !r.config.AutoApprove {
			r.logger.Warn("skipping destructive operation (requires manual approval)",
				"action", step.Action,
				"resource", step.Resource,
			)
			continue
		}

		r.logger.Info("applying reconciliation step",
			"order", step.Order,
			"action", step.Action,
			"resource", step.Resource,
		)
		result.StepsApplied++
	}

	result.Status = ReconcileStatusSuccess
	result.CompletedAt = time.Now()
	result.Duration = result.CompletedAt.Sub(result.StartedAt)
	r.recordResult(*result)

	return result, nil
}

// HandleWebhook processes a Git webhook event for PR-based workflows.
func (r *Reconciler) HandleWebhook(event WebhookEvent) (*ReconcileResult, error) {
	r.logger.Info("received webhook event",
		"type", event.Type,
		"repo", event.Repository,
		"ref", event.Ref,
	)

	switch event.Type {
	case "pull_request":
		if event.Action == "opened" || event.Action == "synchronize" {
			// Run dry-run and post comment
			return r.Reconcile(context.Background(), LiveState{Topics: make(map[string]TopicSpec)}, true)
		}
	case "push":
		if event.Ref == "refs/heads/"+r.config.GitBranch {
			return r.Reconcile(context.Background(), LiveState{Topics: make(map[string]TopicSpec)}, r.config.DryRunDefault)
		}
	}

	return nil, nil
}

// GetResults returns recent reconciliation results.
func (r *Reconciler) GetResults(limit int) []ReconcileResult {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if limit <= 0 || limit > len(r.results) {
		limit = len(r.results)
	}
	start := len(r.results) - limit
	if start < 0 {
		start = 0
	}
	result := make([]ReconcileResult, limit)
	copy(result, r.results[start:])
	return result
}

// Start begins the continuous reconciliation loop.
func (r *Reconciler) Start(ctx context.Context) {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.running = true
	r.mu.Unlock()

	go r.loop(ctx)
}

// Stop halts the reconciliation loop.
func (r *Reconciler) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancel != nil {
		r.cancel()
	}
	r.running = false
}

func (r *Reconciler) loop(ctx context.Context) {
	ticker := time.NewTicker(r.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if r.desired == nil {
				continue
			}
			// In production, fetch live state from Kafka
			live := LiveState{Topics: make(map[string]TopicSpec)}
			_, err := r.Reconcile(ctx, live, r.config.DryRunDefault)
			if err != nil {
				r.logger.Error("reconciliation loop error", "error", err)
			}
		}
	}
}

func (r *Reconciler) recordResult(result ReconcileResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.results) >= r.config.MaxHistory {
		r.results = r.results[1:]
	}
	r.results = append(r.results, result)
}
