package selfheal

import (
	"fmt"
	"sync"
	"time"
)

// GroupHealth represents the health assessment of a consumer group.
type GroupHealth struct {
	GroupID     string             `json:"group_id"`
	HealthScore int                `json:"health_score"`
	Status      string             `json:"status"`
	Issues      []HealthIssue      `json:"issues"`
	Metrics     GroupMetrics       `json:"metrics"`
	Remediation *RemediationAction `json:"remediation,omitempty"`
	Timestamp   string             `json:"timestamp"`
}

// HealthIssue describes a specific problem detected in a consumer group.
type HealthIssue struct {
	Type               string  `json:"type"`
	Severity           string  `json:"severity"`
	Description        string  `json:"description"`
	AffectedPartitions []int32 `json:"affected_partitions,omitempty"`
}

// GroupMetrics holds operational metrics for a consumer group.
type GroupMetrics struct {
	TotalLag       int64   `json:"total_lag"`
	LagTrend       string  `json:"lag_trend"`
	CommitRate     float64 `json:"commit_rate"`
	RebalanceCount int     `json:"rebalance_count"`
	MemberCount    int     `json:"member_count"`
	StableMembers  int     `json:"stable_members"`
}

// RemediationAction describes a suggested or automatic remediation step.
type RemediationAction struct {
	Action      string `json:"action"`
	Reason      string `json:"reason"`
	AutoExecute bool   `json:"auto_execute"`
}

// Policy defines health thresholds and remediation behavior for consumer groups.
type Policy struct {
	GroupPattern     string   `json:"group_pattern"`
	MaxLag           int64    `json:"max_lag"`
	MaxRebalanceRate int      `json:"max_rebalance_rate"`
	AutoRemediate    bool     `json:"auto_remediate"`
	Actions          []string `json:"actions"`
}

const maxHistoryPerGroup = 50

// Monitor tracks consumer group health and applies policies.
type Monitor struct {
	mu            sync.RWMutex
	policies      []Policy
	healthHistory map[string][]GroupHealth
}

// NewMonitor creates a new Monitor.
func NewMonitor() *Monitor {
	return &Monitor{
		healthHistory: make(map[string][]GroupHealth),
	}
}

// AddPolicy appends a policy to the monitor.
func (m *Monitor) AddPolicy(p Policy) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.policies = append(m.policies, p)
}

// EvaluateHealth assesses the health of a consumer group and records the result.
func (m *Monitor) EvaluateHealth(groupID string, lag int64, memberCount int, commitRate float64, rebalanceCount int) *GroupHealth {
	score := m.computeHealthScore(lag, commitRate, rebalanceCount, memberCount)
	issues := m.detectIssues(lag, commitRate, rebalanceCount, memberCount)

	status := "healthy"
	switch {
	case score < 25:
		status = "dead"
	case score < 50:
		status = "critical"
	case score < 76:
		status = "degraded"
	}

	gh := &GroupHealth{
		GroupID:     groupID,
		HealthScore: score,
		Status:      status,
		Issues:      issues,
		Metrics: GroupMetrics{
			TotalLag:       lag,
			LagTrend:       m.computeLagTrend(groupID, lag),
			CommitRate:     commitRate,
			RebalanceCount: rebalanceCount,
			MemberCount:    memberCount,
			StableMembers:  memberCount,
		},
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	gh.Remediation = m.suggestRemediation(score, issues)

	m.mu.Lock()
	history := m.healthHistory[groupID]
	history = append(history, *gh)
	if len(history) > maxHistoryPerGroup {
		history = history[len(history)-maxHistoryPerGroup:]
	}
	m.healthHistory[groupID] = history
	m.mu.Unlock()

	return gh
}

func (m *Monitor) computeHealthScore(lag int64, commitRate float64, rebalanceCount, memberCount int) int {
	score := 100

	// Lag penalty
	switch {
	case lag > 1_000_000:
		score -= 50
	case lag > 100_000:
		score -= 30
	case lag > 10_000:
		score -= 15
	}

	// Commit rate penalty
	if commitRate <= 0 {
		score -= 30
	} else if commitRate < 1.0 {
		score -= 15
	}

	// Rebalance penalty
	if rebalanceCount > 10 {
		score -= 25
	} else if rebalanceCount > 5 {
		score -= 15
	} else if rebalanceCount > 2 {
		score -= 5
	}

	// Member count penalty
	if memberCount == 0 {
		score -= 40
	}

	if score < 0 {
		score = 0
	}
	return score
}

func (m *Monitor) detectIssues(lag int64, commitRate float64, rebalanceCount, memberCount int) []HealthIssue {
	var issues []HealthIssue

	if lag > 100_000 {
		severity := "warning"
		if lag > 1_000_000 {
			severity = "critical"
		}
		issues = append(issues, HealthIssue{
			Type:        "high_lag",
			Severity:    severity,
			Description: fmt.Sprintf("consumer group lag is %d", lag),
		})
	}

	if commitRate <= 0 && memberCount > 0 {
		issues = append(issues, HealthIssue{
			Type:        "no_commits",
			Severity:    "critical",
			Description: "consumer group has members but no offset commits",
		})
	}

	if rebalanceCount > 10 {
		issues = append(issues, HealthIssue{
			Type:        "rebalance_storm",
			Severity:    "critical",
			Description: fmt.Sprintf("excessive rebalances detected: %d", rebalanceCount),
		})
	} else if rebalanceCount > 5 {
		issues = append(issues, HealthIssue{
			Type:        "rebalance_storm",
			Severity:    "warning",
			Description: fmt.Sprintf("elevated rebalance rate: %d", rebalanceCount),
		})
	}

	if memberCount == 0 {
		issues = append(issues, HealthIssue{
			Type:        "zombie_consumer",
			Severity:    "critical",
			Description: "consumer group has no active members",
		})
	}

	if commitRate > 0 && commitRate < 0.5 && lag > 10_000 {
		issues = append(issues, HealthIssue{
			Type:        "stuck_partition",
			Severity:    "warning",
			Description: "low commit rate with significant lag suggests stuck partitions",
		})
	}

	return issues
}

func (m *Monitor) suggestRemediation(score int, issues []HealthIssue) *RemediationAction {
	if len(issues) == 0 {
		return nil
	}

	for _, issue := range issues {
		if issue.Type == "rebalance_storm" && issue.Severity == "critical" {
			return &RemediationAction{
				Action:      "restart_consumer",
				Reason:      "rebalance storm detected; restarting consumers may stabilize the group",
				AutoExecute: false,
			}
		}
	}

	for _, issue := range issues {
		if issue.Type == "zombie_consumer" {
			return &RemediationAction{
				Action:      "force_rebalance",
				Reason:      "no active members; forcing a rebalance to reassign partitions",
				AutoExecute: false,
			}
		}
	}

	if score < 50 {
		for _, issue := range issues {
			if issue.Type == "no_commits" {
				return &RemediationAction{
					Action:      "reset_offsets",
					Reason:      "no commits with critical health; resetting offsets may recover the group",
					AutoExecute: false,
				}
			}
		}
	}

	return &RemediationAction{
		Action:      "alert_only",
		Reason:      "issues detected but no automatic remediation recommended",
		AutoExecute: false,
	}
}

func (m *Monitor) computeLagTrend(groupID string, currentLag int64) string {
	m.mu.RLock()
	history := m.healthHistory[groupID]
	m.mu.RUnlock()

	if len(history) == 0 {
		return "stable"
	}

	prevLag := history[len(history)-1].Metrics.TotalLag
	switch {
	case currentLag > prevLag:
		return "growing"
	case currentLag < prevLag:
		return "shrinking"
	default:
		return "stable"
	}
}

// GetHealthHistory returns the recorded health history for a group.
func (m *Monitor) GetHealthHistory(groupID string) []GroupHealth {
	m.mu.RLock()
	defer m.mu.RUnlock()

	history := m.healthHistory[groupID]
	out := make([]GroupHealth, len(history))
	copy(out, history)
	return out
}

// ListPolicies returns a copy of all configured policies.
func (m *Monitor) ListPolicies() []Policy {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]Policy, len(m.policies))
	copy(out, m.policies)
	return out
}

// HealingResult describes the outcome of an auto-healing action.
type HealingResult struct {
	GroupID   string         `json:"group_id"`
	Action    string         `json:"action"`
	Success   bool           `json:"success"`
	Message   string         `json:"message"`
	Health    *GroupHealth   `json:"health"`
	Diagnosis *RootCause     `json:"diagnosis,omitempty"`
	Playbook  []PlaybookStep `json:"playbook"`
	Timestamp string         `json:"timestamp"`
}

// RootCause represents the diagnosed root cause of a consumer group issue.
type RootCause struct {
	Category    string   `json:"category"`   // "stuck_partition", "slow_consumer", "gc_pressure", "network_timeout", "rebalance_storm", "no_members"
	Confidence  float64  `json:"confidence"` // 0.0-1.0
	Description string   `json:"description"`
	Evidence    []string `json:"evidence"`
}

// PlaybookStep is a single step in a remediation playbook.
type PlaybookStep struct {
	Step        int    `json:"step"`
	Action      string `json:"action"`
	Description string `json:"description"`
	AutoExecute bool   `json:"auto_execute"`
	Status      string `json:"status"` // "pending", "executed", "skipped"
}

// DiagnoseRootCause performs composite root cause analysis on a consumer group.
func (m *Monitor) DiagnoseRootCause(groupID string, lag int64, commitRate float64, rebalanceCount, memberCount int) *RootCause {
	if memberCount == 0 {
		return &RootCause{
			Category:    "no_members",
			Confidence:  0.95,
			Description: "Consumer group has no active members",
			Evidence:    []string{"member_count=0", "group may have been terminated or crashed"},
		}
	}

	if rebalanceCount > 10 {
		return &RootCause{
			Category:    "rebalance_storm",
			Confidence:  0.9,
			Description: "Excessive rebalancing detected — consumers are thrashing",
			Evidence: []string{
				fmt.Sprintf("rebalance_count=%d (threshold=10)", rebalanceCount),
				"Possible causes: session timeout too low, consumer crashes, deployment rollout",
			},
		}
	}

	if commitRate <= 0 && lag > 10000 {
		return &RootCause{
			Category:    "stuck_partition",
			Confidence:  0.85,
			Description: "Partitions appear stuck — no commits despite significant lag",
			Evidence: []string{
				fmt.Sprintf("commit_rate=%.2f, lag=%d", commitRate, lag),
				"Consumer may be blocked on processing, GC pause, or deadlock",
			},
		}
	}

	if commitRate > 0 && commitRate < 0.5 && lag > 100000 {
		return &RootCause{
			Category:    "slow_consumer",
			Confidence:  0.8,
			Description: "Consumer processing is too slow to keep up with production rate",
			Evidence: []string{
				fmt.Sprintf("commit_rate=%.2f (low), lag=%d (high)", commitRate, lag),
				"Consider scaling consumers or optimizing processing logic",
			},
		}
	}

	if lag > 50000 && commitRate > 1.0 {
		return &RootCause{
			Category:    "gc_pressure",
			Confidence:  0.6,
			Description: "Intermittent lag spikes suggest GC pauses or resource contention",
			Evidence: []string{
				fmt.Sprintf("lag=%d with reasonable commit_rate=%.2f", lag, commitRate),
				"Check JVM GC logs or system resource utilization",
			},
		}
	}

	return &RootCause{
		Category:    "network_timeout",
		Confidence:  0.5,
		Description: "Lag may be caused by network issues or broker slowness",
		Evidence: []string{
			fmt.Sprintf("lag=%d, commit_rate=%.2f, rebalances=%d", lag, commitRate, rebalanceCount),
			"Monitor network latency and broker request queue times",
		},
	}
}

// GeneratePlaybook creates a remediation playbook based on the root cause.
func (m *Monitor) GeneratePlaybook(rootCause *RootCause, autoExecute bool) []PlaybookStep {
	switch rootCause.Category {
	case "no_members":
		return []PlaybookStep{
			{Step: 1, Action: "check_deployment", Description: "Verify consumer application is deployed and running", AutoExecute: false, Status: "pending"},
			{Step: 2, Action: "force_rebalance", Description: "Trigger group rebalance to reassign partitions", AutoExecute: autoExecute, Status: "pending"},
			{Step: 3, Action: "alert_oncall", Description: "Alert on-call team if no members rejoin within 5 minutes", AutoExecute: false, Status: "pending"},
		}
	case "rebalance_storm":
		return []PlaybookStep{
			{Step: 1, Action: "increase_session_timeout", Description: "Increase consumer session.timeout.ms to reduce spurious rebalances", AutoExecute: false, Status: "pending"},
			{Step: 2, Action: "suspend_unstable_members", Description: "Identify and suspend frequently disconnecting consumers", AutoExecute: autoExecute, Status: "pending"},
			{Step: 3, Action: "enable_static_membership", Description: "Configure static group membership to avoid unnecessary rebalances", AutoExecute: false, Status: "pending"},
		}
	case "stuck_partition":
		return []PlaybookStep{
			{Step: 1, Action: "identify_stuck_partitions", Description: "Identify which partitions have zero progress", AutoExecute: true, Status: "pending"},
			{Step: 2, Action: "restart_stuck_consumers", Description: "Restart consumers assigned to stuck partitions", AutoExecute: autoExecute, Status: "pending"},
			{Step: 3, Action: "skip_poison_message", Description: "If stuck on a specific offset, consider skipping or sending to DLQ", AutoExecute: false, Status: "pending"},
		}
	case "slow_consumer":
		return []PlaybookStep{
			{Step: 1, Action: "analyze_processing_time", Description: "Profile consumer processing latency per message", AutoExecute: true, Status: "pending"},
			{Step: 2, Action: "scale_consumers", Description: "Add more consumer instances to the group", AutoExecute: false, Status: "pending"},
			{Step: 3, Action: "apply_backpressure", Description: "Reduce producer throughput or enable consumer flow control", AutoExecute: autoExecute, Status: "pending"},
		}
	case "gc_pressure":
		return []PlaybookStep{
			{Step: 1, Action: "check_gc_logs", Description: "Review JVM GC logs for long pause times", AutoExecute: false, Status: "pending"},
			{Step: 2, Action: "increase_heap", Description: "Consider increasing consumer heap size", AutoExecute: false, Status: "pending"},
			{Step: 3, Action: "reduce_batch_size", Description: "Reduce max.poll.records to lower memory pressure", AutoExecute: false, Status: "pending"},
		}
	default:
		return []PlaybookStep{
			{Step: 1, Action: "check_network", Description: "Verify network connectivity between consumers and brokers", AutoExecute: false, Status: "pending"},
			{Step: 2, Action: "check_broker_health", Description: "Verify broker request queue times and disk I/O", AutoExecute: true, Status: "pending"},
			{Step: 3, Action: "restart_consumer", Description: "Restart consumer application as a last resort", AutoExecute: false, Status: "pending"},
		}
	}
}

// Heal performs full auto-healing: evaluate health, diagnose root cause, generate playbook.
func (m *Monitor) Heal(groupID string, lag int64, memberCount int, commitRate float64, rebalanceCount int, autoExecute bool) *HealingResult {
	health := m.EvaluateHealth(groupID, lag, memberCount, commitRate, rebalanceCount)
	diagnosis := m.DiagnoseRootCause(groupID, lag, commitRate, rebalanceCount, memberCount)
	playbook := m.GeneratePlaybook(diagnosis, autoExecute)

	result := &HealingResult{
		GroupID:   groupID,
		Action:    "heal",
		Health:    health,
		Diagnosis: diagnosis,
		Playbook:  playbook,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	if health.Status == "healthy" {
		result.Success = true
		result.Message = "Consumer group is healthy — no remediation needed"
	} else {
		result.Success = false
		result.Message = fmt.Sprintf("Consumer group is %s — %s (confidence: %.0f%%)", health.Status, diagnosis.Description, diagnosis.Confidence*100)
	}

	return result
}
