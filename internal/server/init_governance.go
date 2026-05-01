package server

import (
	"context"
	"log/slog"

	"github.com/kafkamcp/kafkamcp/internal/agentprofile"
	"github.com/kafkamcp/kafkamcp/internal/audit"
	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/compliance"
	"github.com/kafkamcp/kafkamcp/internal/config"
	"github.com/kafkamcp/kafkamcp/internal/contracts"
	"github.com/kafkamcp/kafkamcp/internal/masking"
	"github.com/kafkamcp/kafkamcp/internal/persona"
	"github.com/kafkamcp/kafkamcp/internal/ratelimit"
	"github.com/kafkamcp/kafkamcp/internal/retention"
	"github.com/kafkamcp/kafkamcp/internal/tenant"
)

// Governance groups policy, audit, security, and compliance subsystems.
type Governance struct {
	policyEngine     *auth.PolicyEngine
	auditLogger      *audit.Logger
	rateLimiter      *ratelimit.Limiter
	maskingEngine    *masking.Engine           // PII redaction engine
	contractEngine   *contracts.Engine         // schema-aware data contracts
	personaEngine    *persona.Engine           // declarative agent personas
	tenantManager    *tenant.Manager           // multi-tenant management
	usageTracker     *tenant.UsageTracker      // usage analytics tracking
	complianceEngine *compliance.Engine        // compliance & regulatory reporting
	threatDetector   *agentprofile.Detector    // agent behavioral threat detection
	retentionEngine  *retention.Engine         // intelligent retention policy
}

// newGovernance initializes all governance subsystems.
func newGovernance(cfg *config.Config, logger *slog.Logger) (Governance, error) {
	policyEngine := auth.NewPolicyEngine(cfg.Policies)

	auditLogger, err := audit.NewLogger(context.Background(), cfg.Audit.Enabled, cfg.Audit.MaxEntries, cfg.Audit.LogFile, logger)
	if err != nil {
		return Governance{}, err
	}

	rl := ratelimit.New(0)
	for _, agent := range cfg.Policies.Agents {
		if agent.RateLimit.RequestsPerMinute > 0 {
			rl.SetAgentLimit(agent.ID, agent.RateLimit.RequestsPerMinute)
		}
	}

	return Governance{
		policyEngine:     policyEngine,
		auditLogger:      auditLogger,
		rateLimiter:      rl,
		maskingEngine:    masking.NewEngine(cfg.Masking, logger),
		contractEngine:   contracts.NewEngine(),
		personaEngine:    persona.NewEngine(),
		tenantManager:    tenant.NewManager(),
		usageTracker:     tenant.NewUsageTracker(),
		complianceEngine: compliance.NewEngine(logger),
		threatDetector:   agentprofile.NewDetector(agentprofile.DefaultDetectorConfig(), logger),
		retentionEngine:  retention.NewEngine(logger),
	}, nil
}
