package server

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/kafkamcp/kafkamcp/internal/agentprofile"
	"github.com/kafkamcp/kafkamcp/internal/audit"
	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/auth/identity"
	"github.com/kafkamcp/kafkamcp/internal/compliance"
	"github.com/kafkamcp/kafkamcp/internal/config"
	"github.com/kafkamcp/kafkamcp/internal/contracts"
	enginebackend "github.com/kafkamcp/kafkamcp/internal/engine/backend"
	"github.com/kafkamcp/kafkamcp/internal/masking"
	"github.com/kafkamcp/kafkamcp/internal/persona"
	"github.com/kafkamcp/kafkamcp/internal/ratelimit"
	"github.com/kafkamcp/kafkamcp/internal/retention"
	"github.com/kafkamcp/kafkamcp/internal/tenant"
)

// Governance groups policy, audit, security, and compliance subsystems.
type Governance struct {
	policyEngine     *auth.PolicyEngine
	identityProvider identity.Provider
	auditLogger      *audit.Logger
	rateLimiter      *ratelimit.Limiter
	maskingEngine    *masking.Engine           // PII redaction engine
	contractEngine   *contracts.Engine         // schema-aware data contracts
	contractTester   *contracts.ContractTester // producer/consumer contract testing
	personaEngine    *persona.Engine           // declarative agent personas
	tenantManager    *tenant.Manager           // multi-tenant management
	usageTracker     *tenant.UsageTracker      // usage analytics tracking
	complianceEngine *compliance.Engine        // compliance & regulatory reporting
	threatDetector   *agentprofile.Detector    // agent behavioral threat detection
	retentionEngine  *retention.Engine         // intelligent retention policy
}

// newGovernance initializes all governance subsystems. be is the
// shared persistence backend; passing nil keeps audit + rate-limiting
// state in-memory (legacy behavior).
func newGovernance(cfg *config.Config, logger *slog.Logger, be enginebackend.Backend) (Governance, error) {
	policyEngine := auth.NewPolicyEngine(cfg.Policies)

	auditLogger, err := audit.NewLoggerWithBackend(context.Background(), cfg.Audit.Enabled, cfg.Audit.MaxEntries, cfg.Audit.LogFile, logger, be)
	if err != nil {
		return Governance{}, err
	}

	identityProvider, err := buildIdentityProvider(cfg, logger)
	if err != nil {
		return Governance{}, fmt.Errorf("building identity provider: %w", err)
	}

	rl, err := ratelimit.NewWithBackend(context.Background(), cfg.Policies.GlobalRateLimitPerMinute, be, logger)
	if err != nil {
		return Governance{}, fmt.Errorf("creating rate limiter: %w", err)
	}
	for _, agent := range cfg.Policies.Agents {
		if agent.RateLimit.RequestsPerMinute > 0 {
			rl.SetAgentLimit(agent.ID, agent.RateLimit.RequestsPerMinute)
		}
	}

	return Governance{
		policyEngine:     policyEngine,
		identityProvider: identityProvider,
		auditLogger:      auditLogger,
		rateLimiter:      rl,
		maskingEngine:    masking.NewEngine(cfg.Masking, logger),
		contractEngine:   contracts.NewEngine(),
		contractTester:   contracts.NewContractTester(),
		personaEngine:    persona.NewEngine(),
		tenantManager:    tenant.NewManager(),
		usageTracker:     tenant.NewUsageTracker(),
		complianceEngine: compliance.NewEngine(logger),
		threatDetector:   agentprofile.NewDetector(agentprofile.DefaultDetectorConfig(), logger),
		retentionEngine:  retention.NewEngine(logger),
	}, nil
}

// buildIdentityProvider converts the operator-facing IdentityConfig
// into a concrete identity.Provider. Returns the no-op provider when
// nothing is configured.
func buildIdentityProvider(cfg *config.Config, logger *slog.Logger) (identity.Provider, error) {
	idCfg := identity.Config{Provider: cfg.Policies.Identity.Provider}
	for _, t := range cfg.Policies.Identity.Static.Tokens {
		idCfg.Static.Tokens = append(idCfg.Static.Tokens, identity.StaticToken{
			Token:   t.Token,
			AgentID: t.AgentID,
			Groups:  t.Groups,
		})
	}
	idCfg.OIDC = identity.OIDCConfig{
		IssuerURL:         cfg.Policies.Identity.OIDC.IssuerURL,
		ClientID:          cfg.Policies.Identity.OIDC.ClientID,
		AgentIDClaim:      cfg.Policies.Identity.OIDC.AgentIDClaim,
		GroupsClaim:       cfg.Policies.Identity.OIDC.GroupsClaim,
		EmailClaim:        cfg.Policies.Identity.OIDC.EmailClaim,
		SkipExpiryCheck:   cfg.Policies.Identity.OIDC.SkipExpiry,
		SkipClientIDCheck: cfg.Policies.Identity.OIDC.SkipAudience,
	}
	idCfg.MTLS = identity.MTLSConfig{
		AllowedSPIFFEIDs:    cfg.Policies.Identity.MTLS.AllowedSPIFFEIDs,
		AllowedTrustDomains: cfg.Policies.Identity.MTLS.AllowedTrustDomains,
		GroupsFromOUs:       cfg.Policies.Identity.MTLS.GroupsFromOUs,
		AllowCNFallback:     cfg.Policies.Identity.MTLS.AllowCNFallback,
	}
	prov, err := identity.Build(context.Background(), idCfg)
	if err != nil {
		return nil, err
	}
	switch prov.Name() {
	case "none":
		// Don't log anything; this is the default and matches historical
		// behavior.
	default:
		logger.Info("agent identity verification enabled", "provider", prov.Name())
	}
	return prov, nil
}
