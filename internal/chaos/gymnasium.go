package chaos

import (
	"fmt"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/randid"
)

// DifficultyLevel represents the difficulty tier for gymnasium challenges.
type DifficultyLevel string

const (
	DifficultyBeginner     DifficultyLevel = "beginner"
	DifficultyIntermediate DifficultyLevel = "intermediate"
	DifficultyAdvanced     DifficultyLevel = "advanced"
	DifficultyExpert       DifficultyLevel = "expert"
)

// Challenge defines a structured chaos training exercise.
type Challenge struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Difficulty  DifficultyLevel `json:"difficulty"`
	Category    string          `json:"category"`
	Objectives  []Objective     `json:"objectives"`
	Hints       []string        `json:"hints,omitempty"`
	MaxScore    int             `json:"max_score"`
	TimeLimit   time.Duration   `json:"time_limit"`
	PreReqs     []string        `json:"prerequisites,omitempty"`
}

// Objective is a measurable goal within a challenge.
type Objective struct {
	ID          string `json:"id"`
	Description string `json:"description"`
	Points      int    `json:"points"`
	Type        string `json:"type"` // detect, mitigate, prevent, configure
}

// ChallengeAttempt tracks an agent's attempt at a challenge.
type ChallengeAttempt struct {
	ID              string               `json:"id"`
	ChallengeID     string               `json:"challenge_id"`
	ChallengeName   string               `json:"challenge_name"`
	AgentID         string               `json:"agent_id"`
	Status          string               `json:"status"` // active, completed, failed, timed_out
	Score           int                  `json:"score"`
	MaxScore        int                  `json:"max_score"`
	CompletedObjs   []string             `json:"completed_objectives"`
	ExperimentID    string               `json:"experiment_id,omitempty"`
	StartedAt       time.Time            `json:"started_at"`
	CompletedAt     *time.Time           `json:"completed_at,omitempty"`
	Duration        string               `json:"duration,omitempty"`
	Feedback        []string             `json:"feedback,omitempty"`
}

// AgentProgress tracks an agent's overall gymnasium progress.
type AgentProgress struct {
	AgentID          string                     `json:"agent_id"`
	TotalScore       int                        `json:"total_score"`
	ChallengesTotal  int                        `json:"challenges_completed"`
	CurrentLevel     DifficultyLevel            `json:"current_level"`
	Badges           []Badge                    `json:"badges"`
	CompletedIDs     []string                   `json:"completed_challenge_ids"`
	CategoryScores   map[string]int             `json:"category_scores"`
	StreakDays       int                        `json:"streak_days"`
	LastActivityAt   time.Time                  `json:"last_activity_at"`
}

// Badge is an achievement earned through the gymnasium.
type Badge struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	EarnedAt    time.Time `json:"earned_at"`
}

// Leaderboard holds ranked agent scores.
type Leaderboard struct {
	Entries   []LeaderboardEntry `json:"entries"`
	UpdatedAt time.Time          `json:"updated_at"`
}

// LeaderboardEntry is a single leaderboard row.
type LeaderboardEntry struct {
	Rank    int    `json:"rank"`
	AgentID string `json:"agent_id"`
	Score   int    `json:"score"`
	Level   DifficultyLevel `json:"level"`
	Badges  int    `json:"badges"`
}

// Gymnasium manages gamified chaos training.
type Gymnasium struct {
	mu         sync.RWMutex
	challenges map[string]*Challenge
	attempts   map[string]*ChallengeAttempt
	progress   map[string]*AgentProgress // key: agentID
}

// NewGymnasium creates a gymnasium with built-in challenges.
func NewGymnasium() *Gymnasium {
	g := &Gymnasium{
		challenges: make(map[string]*Challenge),
		attempts:   make(map[string]*ChallengeAttempt),
		progress:   make(map[string]*AgentProgress),
	}
	g.initChallenges()
	return g
}

func (g *Gymnasium) initChallenges() {
	challenges := []Challenge{
		{
			ID: "ch-broker-down", Name: "Broker Down!", Difficulty: DifficultyBeginner,
			Description: "A broker has gone offline. Identify affected topics, check replication status, and verify consumer groups can still operate.",
			Category: "infrastructure", MaxScore: 100, TimeLimit: 10 * time.Minute,
			Objectives: []Objective{
				{ID: "detect-affected", Description: "List topics with under-replicated partitions", Points: 30, Type: "detect"},
				{ID: "check-replication", Description: "Verify min.insync.replicas settings", Points: 30, Type: "detect"},
				{ID: "verify-consumers", Description: "Check consumer group health", Points: 40, Type: "detect"},
			},
			Hints: []string{"Use kafka_describe_topic to check partition replicas", "Check consumer group lag after broker failure"},
		},
		{
			ID: "ch-consumer-lag", Name: "Lag Spike Investigation", Difficulty: DifficultyBeginner,
			Description: "Consumer lag is spiking. Diagnose the root cause and recommend fixes.",
			Category: "consumer", MaxScore: 100, TimeLimit: 10 * time.Minute,
			Objectives: []Objective{
				{ID: "measure-lag", Description: "Measure current consumer lag across partitions", Points: 25, Type: "detect"},
				{ID: "identify-slow", Description: "Identify the slowest partition", Points: 25, Type: "detect"},
				{ID: "check-throughput", Description: "Analyze producer throughput vs consumer capacity", Points: 25, Type: "detect"},
				{ID: "recommend-fix", Description: "Propose a scaling or configuration fix", Points: 25, Type: "mitigate"},
			},
		},
		{
			ID: "ch-rebalance-storm", Name: "Rebalance Storm", Difficulty: DifficultyIntermediate,
			Description: "Consumer groups are caught in continuous rebalances. Stabilize the cluster.",
			Category: "consumer", MaxScore: 150, TimeLimit: 15 * time.Minute,
			PreReqs: []string{"ch-consumer-lag"},
			Objectives: []Objective{
				{ID: "detect-storm", Description: "Identify groups in rebalance loop", Points: 30, Type: "detect"},
				{ID: "find-cause", Description: "Determine root cause (session timeout, heartbeat)", Points: 40, Type: "detect"},
				{ID: "stabilize", Description: "Apply configuration to stop rebalance storm", Points: 50, Type: "mitigate"},
				{ID: "prevent", Description: "Recommend static membership or session tuning", Points: 30, Type: "prevent"},
			},
		},
		{
			ID: "ch-schema-break", Name: "Schema Evolution Crisis", Difficulty: DifficultyIntermediate,
			Description: "A breaking schema change was deployed. Fix deserialization failures and prevent future occurrences.",
			Category: "data", MaxScore: 150, TimeLimit: 15 * time.Minute,
			Objectives: []Objective{
				{ID: "detect-failures", Description: "Find topics with deserialization errors", Points: 30, Type: "detect"},
				{ID: "identify-change", Description: "Identify the incompatible schema change", Points: 40, Type: "detect"},
				{ID: "fix-consumers", Description: "Configure DLQ for failing consumers", Points: 40, Type: "mitigate"},
				{ID: "add-compat", Description: "Enable compatibility enforcement", Points: 40, Type: "prevent"},
			},
		},
		{
			ID: "ch-partition-hotspot", Name: "Partition Hotspot", Difficulty: DifficultyAdvanced,
			Description: "One partition is receiving disproportionate traffic. Diagnose key distribution issues.",
			Category: "producer", MaxScore: 200, TimeLimit: 20 * time.Minute,
			PreReqs: []string{"ch-broker-down"},
			Objectives: []Objective{
				{ID: "detect-skew", Description: "Identify partitions with skewed message distribution", Points: 40, Type: "detect"},
				{ID: "analyze-keys", Description: "Analyze message key distribution patterns", Points: 50, Type: "detect"},
				{ID: "fix-strategy", Description: "Recommend partitioning strategy changes", Points: 60, Type: "mitigate"},
				{ID: "verify-balance", Description: "Validate improved distribution after fix", Points: 50, Type: "detect"},
			},
		},
		{
			ID: "ch-network-partition", Name: "Network Partition Recovery", Difficulty: DifficultyAdvanced,
			Description: "A network partition has split the cluster. Manage split-brain recovery.",
			Category: "network", MaxScore: 200, TimeLimit: 20 * time.Minute,
			PreReqs: []string{"ch-broker-down", "ch-rebalance-storm"},
			Objectives: []Objective{
				{ID: "detect-split", Description: "Detect ISR shrinkage indicating partition", Points: 40, Type: "detect"},
				{ID: "assess-impact", Description: "Identify at-risk topics and data loss potential", Points: 50, Type: "detect"},
				{ID: "configure-safety", Description: "Set unclean.leader.election.enable=false", Points: 50, Type: "configure"},
				{ID: "verify-recovery", Description: "Verify all replicas rejoin ISR after recovery", Points: 60, Type: "detect"},
			},
		},
		{
			ID: "ch-cascading-failure", Name: "Cascading Failure", Difficulty: DifficultyExpert,
			Description: "A producer flood is causing broker disk exhaustion, triggering consumer failures. Prevent complete cluster meltdown.",
			Category: "infrastructure", MaxScore: 300, TimeLimit: 30 * time.Minute,
			PreReqs: []string{"ch-network-partition", "ch-partition-hotspot"},
			Objectives: []Objective{
				{ID: "detect-flood", Description: "Identify the flood source and rate", Points: 40, Type: "detect"},
				{ID: "throttle", Description: "Apply producer quotas to stop the flood", Points: 60, Type: "mitigate"},
				{ID: "free-disk", Description: "Adjust retention to free disk space", Points: 50, Type: "mitigate"},
				{ID: "stabilize-consumers", Description: "Restore consumer group stability", Points: 50, Type: "mitigate"},
				{ID: "post-mortem", Description: "Document timeline and prevention measures", Points: 50, Type: "prevent"},
				{ID: "add-alerts", Description: "Set up monitoring for early detection", Points: 50, Type: "prevent"},
			},
		},
	}

	for i := range challenges {
		ch := challenges[i]
		g.challenges[ch.ID] = &ch
	}
}

// ListChallenges returns challenges, optionally filtered by difficulty or category.
func (g *Gymnasium) ListChallenges(difficulty, category string) []Challenge {
	g.mu.RLock()
	defer g.mu.RUnlock()

	var result []Challenge
	for _, ch := range g.challenges {
		if difficulty != "" && string(ch.Difficulty) != difficulty {
			continue
		}
		if category != "" && ch.Category != category {
			continue
		}
		result = append(result, *ch)
	}
	return result
}

// GetChallenge returns a specific challenge.
func (g *Gymnasium) GetChallenge(id string) (*Challenge, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	ch, ok := g.challenges[id]
	if !ok {
		return nil, fmt.Errorf("challenge %q not found", id)
	}
	return ch, nil
}

// StartChallenge begins a challenge attempt for an agent.
func (g *Gymnasium) StartChallenge(challengeID, agentID string) (*ChallengeAttempt, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	ch, ok := g.challenges[challengeID]
	if !ok {
		return nil, fmt.Errorf("challenge %q not found", challengeID)
	}

	// Check prerequisites
	progress := g.getOrCreateProgress(agentID)
	for _, preReq := range ch.PreReqs {
		found := false
		for _, completed := range progress.CompletedIDs {
			if completed == preReq {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("prerequisite challenge %q not completed", preReq)
		}
	}

	attempt := &ChallengeAttempt{
		ID:            randid.New("attempt"),
		ChallengeID:   challengeID,
		ChallengeName: ch.Name,
		AgentID:       agentID,
		Status:        "active",
		MaxScore:      ch.MaxScore,
		StartedAt:     time.Now(),
	}

	g.attempts[attempt.ID] = attempt
	return attempt, nil
}

// CompleteObjective marks an objective as completed in an attempt.
func (g *Gymnasium) CompleteObjective(attemptID, objectiveID string) (*ChallengeAttempt, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	attempt, ok := g.attempts[attemptID]
	if !ok {
		return nil, fmt.Errorf("attempt %q not found", attemptID)
	}
	if attempt.Status != "active" {
		return nil, fmt.Errorf("attempt %q is not active (status: %s)", attemptID, attempt.Status)
	}

	ch, ok := g.challenges[attempt.ChallengeID]
	if !ok {
		return nil, fmt.Errorf("challenge %q not found", attempt.ChallengeID)
	}

	// Check time limit
	if time.Since(attempt.StartedAt) > ch.TimeLimit {
		attempt.Status = "timed_out"
		attempt.Feedback = append(attempt.Feedback, "Time limit exceeded")
		return attempt, nil
	}

	// Check already completed
	for _, oid := range attempt.CompletedObjs {
		if oid == objectiveID {
			return attempt, nil
		}
	}

	// Find objective and score
	found := false
	for _, obj := range ch.Objectives {
		if obj.ID == objectiveID {
			attempt.CompletedObjs = append(attempt.CompletedObjs, objectiveID)
			attempt.Score += obj.Points
			attempt.Feedback = append(attempt.Feedback, fmt.Sprintf("✓ %s (+%d pts)", obj.Description, obj.Points))
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("objective %q not found in challenge %q", objectiveID, attempt.ChallengeID)
	}

	// Check if all objectives complete
	if len(attempt.CompletedObjs) == len(ch.Objectives) {
		now := time.Now()
		attempt.Status = "completed"
		attempt.CompletedAt = &now
		attempt.Duration = now.Sub(attempt.StartedAt).String()

		// Time bonus: completing early earns extra points
		remaining := ch.TimeLimit - now.Sub(attempt.StartedAt)
		if remaining > 0 {
			bonus := int(remaining.Minutes()) * 5
			attempt.Score += bonus
			attempt.Feedback = append(attempt.Feedback, fmt.Sprintf("🎯 Time bonus: +%d pts", bonus))
		}

		g.updateProgress(attempt, ch)
	}

	return attempt, nil
}

// FinishAttempt ends an active attempt and calculates final score.
func (g *Gymnasium) FinishAttempt(attemptID string) (*ChallengeAttempt, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	attempt, ok := g.attempts[attemptID]
	if !ok {
		return nil, fmt.Errorf("attempt %q not found", attemptID)
	}
	if attempt.Status != "active" {
		return attempt, nil
	}

	now := time.Now()
	attempt.CompletedAt = &now
	attempt.Duration = now.Sub(attempt.StartedAt).String()

	ch, ok := g.challenges[attempt.ChallengeID]
	if !ok {
		return nil, fmt.Errorf("challenge %q not found", attempt.ChallengeID)
	}
	if float64(attempt.Score) >= float64(ch.MaxScore)*0.5 {
		attempt.Status = "completed"
		g.updateProgress(attempt, ch)
	} else {
		attempt.Status = "failed"
		attempt.Feedback = append(attempt.Feedback, fmt.Sprintf("Score %d/%d — need at least 50%% to pass", attempt.Score, ch.MaxScore))
	}

	return attempt, nil
}

// GetAttempt returns an attempt by ID.
func (g *Gymnasium) GetAttempt(attemptID string) (*ChallengeAttempt, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	a, ok := g.attempts[attemptID]
	if !ok {
		return nil, fmt.Errorf("attempt %q not found", attemptID)
	}
	return a, nil
}

// GetProgress returns an agent's overall progress.
func (g *Gymnasium) GetProgress(agentID string) *AgentProgress {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if p, ok := g.progress[agentID]; ok {
		return p
	}
	return &AgentProgress{AgentID: agentID, CurrentLevel: DifficultyBeginner, CategoryScores: make(map[string]int)}
}

// GetLeaderboard returns the ranked leaderboard.
func (g *Gymnasium) GetLeaderboard(limit int) *Leaderboard {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if limit <= 0 {
		limit = 20
	}

	var entries []LeaderboardEntry
	for _, p := range g.progress {
		entries = append(entries, LeaderboardEntry{
			AgentID: p.AgentID, Score: p.TotalScore, Level: p.CurrentLevel, Badges: len(p.Badges),
		})
	}

	// Sort by score descending
	for i := 0; i < len(entries); i++ {
		for j := i + 1; j < len(entries); j++ {
			if entries[j].Score > entries[i].Score {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}

	for i := range entries {
		entries[i].Rank = i + 1
	}

	if len(entries) > limit {
		entries = entries[:limit]
	}

	return &Leaderboard{Entries: entries, UpdatedAt: time.Now()}
}

func (g *Gymnasium) getOrCreateProgress(agentID string) *AgentProgress {
	p, ok := g.progress[agentID]
	if !ok {
		p = &AgentProgress{
			AgentID:        agentID,
			CurrentLevel:   DifficultyBeginner,
			CategoryScores: make(map[string]int),
		}
		g.progress[agentID] = p
	}
	return p
}

func (g *Gymnasium) updateProgress(attempt *ChallengeAttempt, ch *Challenge) {
	progress := g.getOrCreateProgress(attempt.AgentID)
	progress.TotalScore += attempt.Score
	progress.ChallengesTotal++
	progress.CategoryScores[ch.Category] += attempt.Score
	progress.LastActivityAt = time.Now()

	// Only add challenge ID if not already completed
	alreadyCompleted := false
	for _, cid := range progress.CompletedIDs {
		if cid == attempt.ChallengeID {
			alreadyCompleted = true
			break
		}
	}
	if !alreadyCompleted {
		progress.CompletedIDs = append(progress.CompletedIDs, attempt.ChallengeID)
	}

	// Level progression
	switch {
	case progress.TotalScore >= 800:
		progress.CurrentLevel = DifficultyExpert
	case progress.TotalScore >= 400:
		progress.CurrentLevel = DifficultyAdvanced
	case progress.TotalScore >= 150:
		progress.CurrentLevel = DifficultyIntermediate
	}

	// Award badges
	if progress.ChallengesTotal == 1 {
		progress.Badges = append(progress.Badges, Badge{
			ID: "first-challenge", Name: "First Steps", Description: "Completed first chaos challenge", EarnedAt: time.Now(),
		})
	}
	if progress.ChallengesTotal >= 5 {
		hasBadge := false
		for _, b := range progress.Badges {
			if b.ID == "seasoned" {
				hasBadge = true
				break
			}
		}
		if !hasBadge {
			progress.Badges = append(progress.Badges, Badge{
				ID: "seasoned", Name: "Chaos Veteran", Description: "Completed 5 chaos challenges", EarnedAt: time.Now(),
			})
		}
	}
	// Perfect badge: all objectives completed (time bonus doesn't disqualify)
	if len(attempt.CompletedObjs) == len(ch.Objectives) {
		progress.Badges = append(progress.Badges, Badge{
			ID: "perfect-" + ch.ID, Name: "Perfect: " + ch.Name, Description: "Completed all objectives", EarnedAt: time.Now(),
		})
	}
}
