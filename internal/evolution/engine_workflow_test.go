package evolution

import (
	"strings"
	"testing"
)

// ---------- Propose ----------

func TestWorkflowEnginePropose(t *testing.T) {
	t.Parallel()
	t.Run("valid proposal without current schema", func(t *testing.T) {
		w := NewWorkflowEngine()
		p, err := w.Propose("user-value", `{"fields":[]}`, "AVRO", "alice", nil, 0, nil, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if p.Status != "pending" {
			t.Errorf("expected status pending, got %s", p.Status)
		}
		if p.ProposedBy != "alice" {
			t.Errorf("expected proposed_by alice, got %s", p.ProposedBy)
		}
		if p.Subject != "user-value" {
			t.Errorf("expected subject user-value, got %s", p.Subject)
		}
		if p.Diff != nil {
			t.Errorf("expected nil diff when no current schema, got %+v", p.Diff)
		}
		if p.ProposedAt == "" {
			t.Error("expected ProposedAt to be set")
		}
		// Verify stored
		got, err := w.GetProposal(p.ID)
		if err != nil {
			t.Fatalf("proposal not stored: %v", err)
		}
		if got.ID != p.ID {
			t.Errorf("stored ID mismatch")
		}
	})

	t.Run("valid proposal with current schema generates diff and impact", func(t *testing.T) {
		w := NewWorkflowEngine()
		currentSchema := map[string]any{
			"fields": []any{
				map[string]any{"name": "id", "type": "string"},
			},
		}
		newSchema := `{"fields":[{"name":"id","type":"string"},{"name":"email","type":"string","default":""}]}`
		groups := []string{"group1"}
		topicsByGroup := map[string][]string{"group1": {"user"}}

		p, err := w.Propose("user-value", newSchema, "AVRO", "bob", currentSchema, 1, groups, topicsByGroup)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if p.Diff == nil {
			t.Fatal("expected diff to be generated")
		}
		if p.Impact == nil {
			t.Fatal("expected impact to be generated")
		}
		if p.RolloutPlan == nil {
			t.Fatal("expected rollout plan to be generated")
		}
		if p.Diff.Summary.Added != 1 {
			t.Errorf("expected 1 added field, got %d", p.Diff.Summary.Added)
		}
	})

	t.Run("defaults schema type to AVRO", func(t *testing.T) {
		w := NewWorkflowEngine()
		p, err := w.Propose("test-value", `{}`, "", "alice", nil, 0, nil, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if p.SchemaType != "AVRO" {
			t.Errorf("expected default schema type AVRO, got %s", p.SchemaType)
		}
	})

	t.Run("missing subject", func(t *testing.T) {
		w := NewWorkflowEngine()
		_, err := w.Propose("", `{}`, "AVRO", "alice", nil, 0, nil, nil)
		if err == nil {
			t.Fatal("expected error for missing subject")
		}
		if !strings.Contains(err.Error(), "subject") {
			t.Errorf("expected error about subject, got: %v", err)
		}
	})

	t.Run("missing schema", func(t *testing.T) {
		w := NewWorkflowEngine()
		_, err := w.Propose("user-value", "", "AVRO", "alice", nil, 0, nil, nil)
		if err == nil {
			t.Fatal("expected error for missing schema")
		}
		if !strings.Contains(err.Error(), "schema") {
			t.Errorf("expected error about schema, got: %v", err)
		}
	})

	t.Run("missing proposed_by", func(t *testing.T) {
		w := NewWorkflowEngine()
		_, err := w.Propose("user-value", `{}`, "AVRO", "", nil, 0, nil, nil)
		if err == nil {
			t.Fatal("expected error for missing proposed_by")
		}
		if !strings.Contains(err.Error(), "proposed_by") {
			t.Errorf("expected error about proposed_by, got: %v", err)
		}
	})
}

// ---------- Review ----------

func TestWorkflowEngineReview(t *testing.T) {
	t.Parallel()
	setup := func(t *testing.T) (*WorkflowEngine, string) {
		t.Helper()
		w := NewWorkflowEngine()
		p, err := w.Propose("test-value", `{}`, "AVRO", "alice", nil, 0, nil, nil)
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}
		return w, p.ID
	}

	t.Run("approve", func(t *testing.T) {
		w, id := setup(t)
		p, err := w.Review(id, "bob", "approve", "looks good")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if p.Status != "approved" {
			t.Errorf("expected approved, got %s", p.Status)
		}
		if p.ReviewedBy != "bob" {
			t.Errorf("expected reviewer bob, got %s", p.ReviewedBy)
		}
		if p.ReviewNote != "looks good" {
			t.Errorf("expected note 'looks good', got %s", p.ReviewNote)
		}
		if p.ReviewedAt == "" {
			t.Error("expected ReviewedAt to be set")
		}
	})

	t.Run("reject", func(t *testing.T) {
		w, id := setup(t)
		p, err := w.Review(id, "carol", "reject", "needs work")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if p.Status != "rejected" {
			t.Errorf("expected rejected, got %s", p.Status)
		}
	})

	t.Run("invalid action", func(t *testing.T) {
		w, id := setup(t)
		_, err := w.Review(id, "bob", "maybe", "")
		if err == nil {
			t.Fatal("expected error for invalid action")
		}
		if !strings.Contains(err.Error(), "invalid action") {
			t.Errorf("expected invalid action error, got: %v", err)
		}
	})

	t.Run("proposal not found", func(t *testing.T) {
		w := NewWorkflowEngine()
		_, err := w.Review("nonexistent", "bob", "approve", "")
		if err == nil {
			t.Fatal("expected error for nonexistent proposal")
		}
		if !strings.Contains(err.Error(), "not found") {
			t.Errorf("expected not found error, got: %v", err)
		}
	})

	t.Run("already reviewed", func(t *testing.T) {
		w, id := setup(t)
		_, err := w.Review(id, "bob", "approve", "")
		if err != nil {
			t.Fatalf("first review failed: %v", err)
		}
		_, err = w.Review(id, "carol", "reject", "")
		if err == nil {
			t.Fatal("expected error for already-reviewed proposal")
		}
		if !strings.Contains(err.Error(), "already") {
			t.Errorf("expected already-reviewed error, got: %v", err)
		}
	})
}

// ---------- MarkApplied ----------

func TestWorkflowEngineApply(t *testing.T) {
	t.Parallel()
	t.Run("mark approved proposal as applied", func(t *testing.T) {
		w := NewWorkflowEngine()
		p, _ := w.Propose("test-value", `{}`, "AVRO", "alice", nil, 0, nil, nil)
		_, _ = w.Review(p.ID, "bob", "approve", "")

		err := w.MarkApplied(p.ID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got, _ := w.GetProposal(p.ID)
		if got.Status != "applied" {
			t.Errorf("expected applied, got %s", got.Status)
		}
		if got.AppliedAt == "" {
			t.Error("expected AppliedAt to be set")
		}
	})

	t.Run("wrong state - pending", func(t *testing.T) {
		w := NewWorkflowEngine()
		p, _ := w.Propose("test-value", `{}`, "AVRO", "alice", nil, 0, nil, nil)

		err := w.MarkApplied(p.ID)
		if err == nil {
			t.Fatal("expected error for non-approved proposal")
		}
		if !strings.Contains(err.Error(), "approved") {
			t.Errorf("expected error mentioning approved, got: %v", err)
		}
	})

	t.Run("not found", func(t *testing.T) {
		w := NewWorkflowEngine()
		err := w.MarkApplied("nonexistent")
		if err == nil {
			t.Fatal("expected error for nonexistent proposal")
		}
		if !strings.Contains(err.Error(), "not found") {
			t.Errorf("expected not found error, got: %v", err)
		}
	})
}

// ---------- MarkRolledBack ----------

func TestWorkflowEngineRollback(t *testing.T) {
	t.Parallel()
	t.Run("roll back applied proposal", func(t *testing.T) {
		w := NewWorkflowEngine()
		p, _ := w.Propose("test-value", `{}`, "AVRO", "alice", nil, 0, nil, nil)
		_, _ = w.Review(p.ID, "bob", "approve", "")
		_ = w.MarkApplied(p.ID)

		err := w.MarkRolledBack(p.ID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got, _ := w.GetProposal(p.ID)
		if got.Status != "rolled_back" {
			t.Errorf("expected rolled_back, got %s", got.Status)
		}
	})

	t.Run("wrong state - pending", func(t *testing.T) {
		w := NewWorkflowEngine()
		p, _ := w.Propose("test-value", `{}`, "AVRO", "alice", nil, 0, nil, nil)

		err := w.MarkRolledBack(p.ID)
		if err == nil {
			t.Fatal("expected error for non-applied proposal")
		}
		if !strings.Contains(err.Error(), "applied") {
			t.Errorf("expected error mentioning applied, got: %v", err)
		}
	})

	t.Run("wrong state - approved but not applied", func(t *testing.T) {
		w := NewWorkflowEngine()
		p, _ := w.Propose("test-value", `{}`, "AVRO", "alice", nil, 0, nil, nil)
		_, _ = w.Review(p.ID, "bob", "approve", "")

		err := w.MarkRolledBack(p.ID)
		if err == nil {
			t.Fatal("expected error for approved-but-not-applied proposal")
		}
	})

	t.Run("not found", func(t *testing.T) {
		w := NewWorkflowEngine()
		err := w.MarkRolledBack("nonexistent")
		if err == nil {
			t.Fatal("expected error for nonexistent proposal")
		}
	})
}

// ---------- ListProposals ----------

func TestWorkflowEngineListProposals(t *testing.T) {
	t.Parallel()
	t.Run("list all proposals", func(t *testing.T) {
		w := NewWorkflowEngine()
		_, _ = w.Propose("a-value", `{}`, "AVRO", "alice", nil, 0, nil, nil)
		_, _ = w.Propose("b-value", `{}`, "AVRO", "bob", nil, 0, nil, nil)

		all := w.ListProposals("")
		if len(all) != 2 {
			t.Errorf("expected 2 proposals, got %d", len(all))
		}
	})

	t.Run("filter by status", func(t *testing.T) {
		w := NewWorkflowEngine()
		p1, _ := w.Propose("a-value", `{}`, "AVRO", "alice", nil, 0, nil, nil)
		_, _ = w.Propose("b-value", `{}`, "AVRO", "bob", nil, 0, nil, nil)
		_, _ = w.Review(p1.ID, "carol", "approve", "")

		pending := w.ListProposals("pending")
		if len(pending) != 1 {
			t.Errorf("expected 1 pending, got %d", len(pending))
		}

		approved := w.ListProposals("approved")
		if len(approved) != 1 {
			t.Errorf("expected 1 approved, got %d", len(approved))
		}
	})

	t.Run("empty results", func(t *testing.T) {
		w := NewWorkflowEngine()
		result := w.ListProposals("")
		if len(result) != 0 {
			t.Errorf("expected 0 proposals, got %d", len(result))
		}
	})

	t.Run("filter returns empty when no match", func(t *testing.T) {
		w := NewWorkflowEngine()
		_, _ = w.Propose("a-value", `{}`, "AVRO", "alice", nil, 0, nil, nil)

		result := w.ListProposals("approved")
		if len(result) != 0 {
			t.Errorf("expected 0 proposals for approved filter, got %d", len(result))
		}
	})
}

// ---------- Additional Edge Cases ----------

func TestWorkflowEnginePropose_ConcurrentSameSubject(t *testing.T) {
	t.Parallel()
	w := NewWorkflowEngine()
	p1, err := w.Propose("user-value", `{"fields":[]}`, "AVRO", "alice", nil, 0, nil, nil)
	if err != nil {
		t.Fatalf("first proposal failed: %v", err)
	}
	p2, err := w.Propose("user-value", `{"fields":[{"name":"id","type":"string"}]}`, "AVRO", "bob", nil, 0, nil, nil)
	if err != nil {
		t.Fatalf("second proposal failed: %v", err)
	}
	if p1.ID == p2.ID {
		t.Error("concurrent proposals should have unique IDs")
	}
	all := w.ListProposals("")
	if len(all) != 2 {
		t.Errorf("expected 2 proposals, got %d", len(all))
	}
}

func TestWorkflowEnginePropose_EmptyDiff(t *testing.T) {
	t.Parallel()
	w := NewWorkflowEngine()
	currentSchema := map[string]any{
		"fields": []any{
			map[string]any{"name": "id", "type": "string"},
		},
	}
	// Propose same schema as current
	p, err := w.Propose("user-value", `{"fields":[{"name":"id","type":"string"}]}`, "AVRO", "alice", currentSchema, 1, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Diff == nil {
		t.Fatal("expected diff even when schemas are identical")
	}
	if p.Diff.Summary.Added != 0 && p.Diff.Summary.Removed != 0 && p.Diff.Summary.Modified != 0 {
		t.Error("expected no additions, removals, or modifications for identical schemas")
	}
}

func TestWorkflowEngineReview_RejectAfterApply(t *testing.T) {
	t.Parallel()
	w := NewWorkflowEngine()
	p, _ := w.Propose("test-value", `{}`, "AVRO", "alice", nil, 0, nil, nil)
	_, _ = w.Review(p.ID, "bob", "approve", "")
	_ = w.MarkApplied(p.ID)

	_, err := w.Review(p.ID, "carol", "reject", "too late")
	if err == nil {
		t.Fatal("expected error when rejecting an already-applied proposal")
	}
	if !strings.Contains(err.Error(), "already") {
		t.Errorf("expected 'already' in error, got: %v", err)
	}
}

func TestWorkflowEngineGetProposal_NotFound(t *testing.T) {
	t.Parallel()
	w := NewWorkflowEngine()
	_, err := w.GetProposal("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent proposal")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected not found error, got: %v", err)
	}
}

func TestWorkflowEngine_FullLifecycle(t *testing.T) {
	t.Parallel()
	w := NewWorkflowEngine()

	// propose → approve → apply → rollback
	p, err := w.Propose("orders-value", `{"fields":[]}`, "AVRO", "alice", nil, 0, nil, nil)
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}
	if p.Status != "pending" {
		t.Errorf("expected pending, got %s", p.Status)
	}

	_, err = w.Review(p.ID, "bob", "approve", "lgtm")
	if err != nil {
		t.Fatalf("review failed: %v", err)
	}

	err = w.MarkApplied(p.ID)
	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}

	err = w.MarkRolledBack(p.ID)
	if err != nil {
		t.Fatalf("rollback failed: %v", err)
	}

	got, _ := w.GetProposal(p.ID)
	if got.Status != "rolled_back" {
		t.Errorf("expected rolled_back, got %s", got.Status)
	}
}

func TestWorkflowEngineApply_AlreadyApplied(t *testing.T) {
	t.Parallel()
	w := NewWorkflowEngine()
	p, _ := w.Propose("test-value", `{}`, "AVRO", "alice", nil, 0, nil, nil)
	_, _ = w.Review(p.ID, "bob", "approve", "")
	_ = w.MarkApplied(p.ID)

	err := w.MarkApplied(p.ID)
	if err == nil {
		t.Fatal("expected error for double-apply")
	}
	if !strings.Contains(err.Error(), "approved") {
		t.Errorf("expected error about approved status, got: %v", err)
	}
}

func TestWorkflowEngineRollback_AlreadyRolledBack(t *testing.T) {
	t.Parallel()
	w := NewWorkflowEngine()
	p, _ := w.Propose("test-value", `{}`, "AVRO", "alice", nil, 0, nil, nil)
	_, _ = w.Review(p.ID, "bob", "approve", "")
	_ = w.MarkApplied(p.ID)
	_ = w.MarkRolledBack(p.ID)

	err := w.MarkRolledBack(p.ID)
	if err == nil {
		t.Fatal("expected error for double-rollback")
	}
	if !strings.Contains(err.Error(), "applied") {
		t.Errorf("expected error about applied status, got: %v", err)
	}
}

func TestWorkflowEngineListProposals_SortedByTime(t *testing.T) {
	t.Parallel()
	w := NewWorkflowEngine()
	_, _ = w.Propose("a-value", `{}`, "AVRO", "alice", nil, 0, nil, nil)
	_, _ = w.Propose("b-value", `{}`, "AVRO", "bob", nil, 0, nil, nil)
	_, _ = w.Propose("c-value", `{}`, "AVRO", "carol", nil, 0, nil, nil)

	all := w.ListProposals("")
	if len(all) != 3 {
		t.Fatalf("expected 3 proposals, got %d", len(all))
	}
	// Should be sorted by ProposedAt descending
	for i := 0; i < len(all)-1; i++ {
		if all[i].ProposedAt < all[i+1].ProposedAt {
			t.Errorf("proposals not sorted descending by time at index %d", i)
		}
	}
}

func TestWorkflowEnginePropose_BreakingChange(t *testing.T) {
	t.Parallel()
	w := NewWorkflowEngine()
	currentSchema := map[string]any{
		"fields": []any{
			map[string]any{"name": "id", "type": "string"},
			map[string]any{"name": "email", "type": "string"},
		},
	}
	// Remove email field (breaking)
	newSchema := `{"fields":[{"name":"id","type":"string"}]}`
	groups := []string{"consumer-1"}
	topicsByGroup := map[string][]string{"consumer-1": {"test"}}

	p, err := w.Propose("test-value", newSchema, "AVRO", "alice", currentSchema, 1, groups, topicsByGroup)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Diff == nil {
		t.Fatal("expected diff")
	}
	if p.Diff.Summary.Removed == 0 {
		t.Error("expected at least one removed field")
	}
	if p.Diff.Compatible {
		t.Error("expected incompatible diff for removed field without default")
	}
	if p.Impact == nil {
		t.Fatal("expected impact analysis")
	}
	if p.Impact.RiskLevel != "high" {
		t.Errorf("expected high risk for breaking change, got %s", p.Impact.RiskLevel)
	}
}
