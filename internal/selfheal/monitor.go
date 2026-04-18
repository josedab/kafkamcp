package selfheal

import (
	"fmt"
	"sync"
	"time"
)

// GroupHealth represents the health assessment of a consumer group.
type GroupHealth struct {
	GroupID     string             `json:"group_id"`
	HealthScore int               `json:"health_score"`
	Status     string             `json:"status"`
	Issues     []HealthIssue      `json:"issues"`
	Metrics    GroupMetrics        `json:"metrics"`
	Remediation *RemediationAction `json:"remediation,omitempty"`
	Timestamp  string             `json:"timestamp"`
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

// Policy defines health thresholds and remediation behaviour for consumer groups.
type Policy struct {
	GroupPattern    string   `json:"group_pattern"`
	MaxLag          int64    `json:"max_lag"`
	MaxRebalanceRate int     `json:"max_rebalance_rate"`
	AutoRemediate   bool     `json:"auto_remediate"`
	Actions         []string `json:"actions"`
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
	case score < 75:
		status = "degraded"
	}

	gh := &GroupHealth{
		GroupID:     groupID,
		HealthScore: score,
		Status:     status,
		Issues:     issues,
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
				Reason:      "rebalance storm detected; restarting consumers may stabilise the group",
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
