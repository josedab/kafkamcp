package server

import (
	"log/slog"

	"github.com/josedab/kafkamcp/internal/aclmgmt"
	enginebackend "github.com/josedab/kafkamcp/internal/engine/backend"
	"github.com/josedab/kafkamcp/internal/onboarding"
	"github.com/josedab/kafkamcp/internal/opa"
	"github.com/josedab/kafkamcp/internal/telemetry"
)

// Platform groups infrastructure, orchestration, and extensibility subsystems
// required by the supported tool surface.
type Platform struct {
	otelProvider     *telemetry.Provider // OpenTelemetry trace provider
	onboardingEngine *onboarding.Engine  // zero-config cluster discovery (kafka_discover_clusters)
	opaPolicyEngine  *opa.PolicyEngine   // OPA policy engine (kafka_policy_evaluate/simulate)
	aclMgmtEngine    *aclmgmt.Engine     // ACL diff/audit computation (kafka_acl_manage)
}

// newPlatform initializes the platform subsystems needed by the supported
// tools. backend is accepted for signature stability with the other
// subsystems; no platform engine currently requires durable state.
func newPlatform(logger *slog.Logger, _ enginebackend.Backend) Platform {
	return Platform{
		otelProvider:     nil,
		onboardingEngine: onboarding.NewEngine(logger),
		opaPolicyEngine:  opa.NewPolicyEngine(logger),
		aclMgmtEngine:    aclmgmt.NewEngine(logger),
	}
}
