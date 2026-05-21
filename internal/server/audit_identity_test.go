package server

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/audit"
	"github.com/kafkamcp/kafkamcp/internal/auth/identity"
	"github.com/kafkamcp/kafkamcp/internal/config"
	"github.com/kafkamcp/kafkamcp/internal/metrics"
)

// TestLogAudit_EnrichesIdentityFromContext verifies that the identity
// attached to a request context by the auth middleware propagates into
// the audit log entry. This is the FU1 closing test for ADR-007.
func TestLogAudit_EnrichesIdentityFromContext(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		ident          *identity.AgentIdentity
		wantMethod     string
		wantIssuer     string
		wantSubject    string
		wantAgentID    string
	}{
		{
			name:        "none provider — no identity attached",
			ident:       nil,
			wantMethod:  "",
			wantIssuer:  "",
			wantSubject: "",
			wantAgentID: "anonymous",
		},
		{
			name: "static provider",
			ident: &identity.AgentIdentity{
				ID:         "ci-runner",
				Method:     "static",
				Subject:    "ci-runner",
				VerifiedAt: time.Now(),
			},
			wantMethod:  "static",
			wantIssuer:  "",
			wantSubject: "ci-runner",
			wantAgentID: "ci-runner",
		},
		{
			name: "oidc provider",
			ident: &identity.AgentIdentity{
				ID:         "agent-42",
				Method:     "oidc",
				Subject:    "auth0|abc123",
				Issuer:     "https://auth.example.com/",
				Email:      "agent@example.com",
				VerifiedAt: time.Now(),
			},
			wantMethod:  "oidc",
			wantIssuer:  "https://auth.example.com/",
			wantSubject: "auth0|abc123",
			wantAgentID: "agent-42",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newAuditTestServer(t)

			ctx := context.Background()
			if tc.ident != nil {
				ctx = identity.WithIdentity(ctx, tc.ident)
			}

			srv.logAudit(ctx, tc.wantAgentID, "kafka_test_tool", nil, "", "success", 1, nil, time.Now())

			recent := srv.auditLogger.RecentEntries(1)
			if len(recent) == 0 {
				t.Fatal("expected one audit entry, got none")
			}
			got := recent[0]
			if got.IdentityMethod != tc.wantMethod {
				t.Errorf("identity_method: got %q want %q", got.IdentityMethod, tc.wantMethod)
			}
			if got.Issuer != tc.wantIssuer {
				t.Errorf("issuer: got %q want %q", got.Issuer, tc.wantIssuer)
			}
			if got.Subject != tc.wantSubject {
				t.Errorf("subject: got %q want %q", got.Subject, tc.wantSubject)
			}
			if got.AgentID != tc.wantAgentID {
				t.Errorf("agent_id: got %q want %q", got.AgentID, tc.wantAgentID)
			}
		})
	}
}

// TestLogAudit_BackwardCompatibleWithBackgroundContext verifies that
// passing context.Background() (no identity attached) leaves the
// identity fields empty, preserving the legacy behaviour for STDIO
// transport / `provider: none` deployments.
func TestLogAudit_BackwardCompatibleWithBackgroundContext(t *testing.T) {
	t.Parallel()
	srv := newAuditTestServer(t)
	srv.logAudit(context.Background(), "legacy-agent", "kafka_topic_list", nil, "", "success", 0, nil, time.Now())
	recent := srv.auditLogger.RecentEntries(1)
	if len(recent) == 0 {
		t.Fatal("expected one audit entry, got none")
	}
	got := recent[0]
	if got.IdentityMethod != "" || got.Issuer != "" || got.Subject != "" {
		t.Errorf("expected empty identity fields, got method=%q issuer=%q subject=%q",
			got.IdentityMethod, got.Issuer, got.Subject)
	}
	if got.AgentID != "legacy-agent" {
		t.Errorf("agent_id: got %q want %q", got.AgentID, "legacy-agent")
	}
}

// newAuditTestServer constructs a minimal Server with audit logging
// enabled so RecentEntries returns the entries we record. It is
// deliberately narrower than newTestServer to avoid pulling in the
// full subsystem graph for an audit-only test.
func newAuditTestServer(t *testing.T) *Server {
	t.Helper()
	cfg := &config.Config{
		Server: config.ServerConfig{Transport: "stdio"},
		Audit:  config.AuditConfig{Enabled: true, MaxEntries: 16},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	auditLogger, err := audit.NewLogger(context.Background(), true, 16, "", logger)
	if err != nil {
		t.Fatalf("audit logger: %v", err)
	}
	return &Server{
		config:  cfg,
		metrics: metrics.New(),
		logger:  logger,
		Governance: Governance{
			auditLogger: auditLogger,
		},
	}
}
