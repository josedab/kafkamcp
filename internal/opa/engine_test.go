package opa

import (
	"log/slog"
	"testing"
)

func newTestEngine() *PolicyEngine {
	return NewPolicyEngine(slog.Default())
}

func TestDefaultPolicies(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	policies := pe.ListPolicies()
	if len(policies) < 3 {
		t.Fatalf("expected at least 3 default policies, got %d", len(policies))
	}
}

func TestEvaluateReadAllowed(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	d := pe.Evaluate(PolicyInput{
		Agent:  "any-agent",
		Action: "read",
		Resource: "topic",
	})

	if !d.Allowed {
		t.Fatal("expected read to be allowed by default policy")
	}
}

func TestEvaluateDeleteDenied(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	d := pe.Evaluate(PolicyInput{
		Agent:  "some-agent",
		Action: "delete",
		Resource: "topic",
	})

	if d.Allowed {
		t.Fatal("expected delete to be denied for non-admin")
	}
}

func TestEvaluateAnonymousWriteDenied(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	d := pe.Evaluate(PolicyInput{
		Agent:  "anonymous",
		Action: "write",
		Resource: "topic",
	})

	if d.Allowed {
		t.Fatal("expected anonymous write to be denied")
	}
}

func TestRegisterPolicy(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	err := pe.RegisterPolicy(Policy{
		ID:   "custom-allow-write",
		Name: "Allow writes",
		Rego: `package kafkamcp.authz
allow {
    input.action == "write"
    input.agent == "writer-agent"
}`,
		Enabled: true,
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	d := pe.Evaluate(PolicyInput{
		Agent:  "writer-agent",
		Action: "write",
		Resource: "topic",
	})
	if !d.Allowed {
		t.Fatal("expected write to be allowed for writer-agent")
	}
}

func TestSimulate(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	result := pe.Simulate(PolicyInput{
		Agent:  "test-agent",
		Action: "read",
		Resource: "topic",
	})

	if len(result.Decisions) == 0 {
		t.Fatal("expected simulation decisions")
	}
}

func TestDeletePolicy(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	err := pe.DeletePolicy("default-allow-read")
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	err = pe.DeletePolicy("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent policy")
	}
}
