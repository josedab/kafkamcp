package chaos

import (
	"testing"
)

func TestGymnasium_ListChallenges(t *testing.T) {
	t.Parallel()
	g := NewGymnasium()
	all := g.ListChallenges("", "")
	if len(all) < 5 {
		t.Errorf("expected at least 5 challenges, got %d", len(all))
	}
}

func TestGymnasium_ListChallenges_Filter(t *testing.T) {
	t.Parallel()
	g := NewGymnasium()
	beginners := g.ListChallenges("beginner", "")
	for _, ch := range beginners {
		if ch.Difficulty != DifficultyBeginner {
			t.Errorf("expected beginner difficulty, got %s", ch.Difficulty)
		}
	}
	consumers := g.ListChallenges("", "consumer")
	for _, ch := range consumers {
		if ch.Category != "consumer" {
			t.Errorf("expected consumer category, got %s", ch.Category)
		}
	}
}

func TestGymnasium_StartChallenge(t *testing.T) {
	t.Parallel()
	g := NewGymnasium()
	attempt, err := g.StartChallenge("ch-broker-down", "agent-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempt.Status != "active" {
		t.Errorf("expected active status, got %s", attempt.Status)
	}
	if attempt.AgentID != "agent-1" {
		t.Errorf("expected agent-1, got %s", attempt.AgentID)
	}
}

func TestGymnasium_StartChallenge_Prerequisites(t *testing.T) {
	t.Parallel()
	g := NewGymnasium()
	// ch-rebalance-storm requires ch-consumer-lag
	_, err := g.StartChallenge("ch-rebalance-storm", "agent-1")
	if err == nil {
		t.Fatal("expected prerequisite error")
	}
}

func TestGymnasium_CompleteObjective(t *testing.T) {
	t.Parallel()
	g := NewGymnasium()
	attempt, _ := g.StartChallenge("ch-broker-down", "agent-1")

	updated, err := g.CompleteObjective(attempt.ID, "detect-affected")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if updated.Score != 30 {
		t.Errorf("expected score 30, got %d", updated.Score)
	}
	if len(updated.CompletedObjs) != 1 {
		t.Errorf("expected 1 completed objective, got %d", len(updated.CompletedObjs))
	}
}

func TestGymnasium_CompleteAllObjectives(t *testing.T) {
	t.Parallel()
	g := NewGymnasium()
	attempt, _ := g.StartChallenge("ch-broker-down", "agent-1")

	_, _ = g.CompleteObjective(attempt.ID, "detect-affected")
	_, _ = g.CompleteObjective(attempt.ID, "check-replication")
	result, _ := g.CompleteObjective(attempt.ID, "verify-consumers")

	if result.Status != "completed" {
		t.Errorf("expected completed, got %s", result.Status)
	}
	if result.Score < 100 {
		t.Errorf("expected at least 100 score, got %d", result.Score)
	}
}

func TestGymnasium_FinishAttempt_Pass(t *testing.T) {
	t.Parallel()
	g := NewGymnasium()
	attempt, _ := g.StartChallenge("ch-broker-down", "agent-1")
	// Complete enough for >50%
	_, _ = g.CompleteObjective(attempt.ID, "detect-affected")
	_, _ = g.CompleteObjective(attempt.ID, "check-replication")
	result, err := g.FinishAttempt(attempt.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != "completed" {
		t.Errorf("expected completed, got %s", result.Status)
	}
}

func TestGymnasium_FinishAttempt_Fail(t *testing.T) {
	t.Parallel()
	g := NewGymnasium()
	attempt, _ := g.StartChallenge("ch-broker-down", "agent-1")
	// Only 30 points out of 100 = 30% < 50%
	_, _ = g.CompleteObjective(attempt.ID, "detect-affected")
	result, _ := g.FinishAttempt(attempt.ID)
	if result.Status != "failed" {
		t.Errorf("expected failed, got %s", result.Status)
	}
}

func TestGymnasium_Progress(t *testing.T) {
	t.Parallel()
	g := NewGymnasium()
	attempt, _ := g.StartChallenge("ch-broker-down", "agent-1")
	_, _ = g.CompleteObjective(attempt.ID, "detect-affected")
	_, _ = g.CompleteObjective(attempt.ID, "check-replication")
	_, _ = g.CompleteObjective(attempt.ID, "verify-consumers")

	progress := g.GetProgress("agent-1")
	if progress.ChallengesTotal != 1 {
		t.Errorf("expected 1 completed challenge, got %d", progress.ChallengesTotal)
	}
	if progress.TotalScore < 100 {
		t.Errorf("expected at least 100 total score, got %d", progress.TotalScore)
	}
	if len(progress.Badges) == 0 {
		t.Error("expected at least one badge")
	}
}

func TestGymnasium_Leaderboard(t *testing.T) {
	t.Parallel()
	g := NewGymnasium()

	// Two agents complete challenges
	a1, _ := g.StartChallenge("ch-broker-down", "agent-1")
	_, _ = g.CompleteObjective(a1.ID, "detect-affected")
	_, _ = g.CompleteObjective(a1.ID, "check-replication")
	_, _ = g.CompleteObjective(a1.ID, "verify-consumers")

	a2, _ := g.StartChallenge("ch-broker-down", "agent-2")
	_, _ = g.CompleteObjective(a2.ID, "detect-affected")
	_, _ = g.FinishAttempt(a2.ID)

	lb := g.GetLeaderboard(10)
	if len(lb.Entries) != 2 {
		t.Errorf("expected 2 leaderboard entries, got %d", len(lb.Entries))
	}
	if lb.Entries[0].Score < lb.Entries[1].Score {
		t.Error("leaderboard should be sorted by score descending")
	}
}

func TestGymnasium_GetChallenge_NotFound(t *testing.T) {
	t.Parallel()
	g := NewGymnasium()
	_, err := g.GetChallenge("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent challenge")
	}
}

func TestGymnasium_DuplicateObjective(t *testing.T) {
	t.Parallel()
	g := NewGymnasium()
	attempt, _ := g.StartChallenge("ch-broker-down", "agent-1")
	_, _ = g.CompleteObjective(attempt.ID, "detect-affected")
	result, _ := g.CompleteObjective(attempt.ID, "detect-affected")
	if result.Score != 30 {
		t.Errorf("duplicate objective should not add score, got %d", result.Score)
	}
}

func TestGymnasium_InvalidObjective(t *testing.T) {
	t.Parallel()
	g := NewGymnasium()
	attempt, _ := g.StartChallenge("ch-broker-down", "agent-1")
	_, err := g.CompleteObjective(attempt.ID, "nonexistent-objective")
	if err == nil {
		t.Fatal("expected error for nonexistent objective")
	}
}

func TestGymnasium_PerfectBadgeWithTimeBonus(t *testing.T) {
	t.Parallel()
	g := NewGymnasium()
	attempt, _ := g.StartChallenge("ch-broker-down", "agent-1")
	_, _ = g.CompleteObjective(attempt.ID, "detect-affected")
	_, _ = g.CompleteObjective(attempt.ID, "check-replication")
	_, _ = g.CompleteObjective(attempt.ID, "verify-consumers")

	progress := g.GetProgress("agent-1")
	hasPerfect := false
	for _, b := range progress.Badges {
		if b.ID == "perfect-ch-broker-down" {
			hasPerfect = true
			break
		}
	}
	if !hasPerfect {
		t.Error("expected perfect badge even with time bonus")
	}
}

func TestGymnasium_DuplicateChallengeCompletion(t *testing.T) {
	t.Parallel()
	g := NewGymnasium()
	// Complete same challenge twice
	a1, _ := g.StartChallenge("ch-broker-down", "agent-1")
	_, _ = g.CompleteObjective(a1.ID, "detect-affected")
	_, _ = g.CompleteObjective(a1.ID, "check-replication")
	_, _ = g.CompleteObjective(a1.ID, "verify-consumers")

	a2, _ := g.StartChallenge("ch-broker-down", "agent-1")
	_, _ = g.CompleteObjective(a2.ID, "detect-affected")
	_, _ = g.CompleteObjective(a2.ID, "check-replication")
	_, _ = g.CompleteObjective(a2.ID, "verify-consumers")

	progress := g.GetProgress("agent-1")
	// CompletedIDs should not have duplicates
	count := 0
	for _, cid := range progress.CompletedIDs {
		if cid == "ch-broker-down" {
			count++
		}
	}
	if count > 1 {
		t.Errorf("expected challenge ID to appear once in CompletedIDs, appeared %d times", count)
	}
}
