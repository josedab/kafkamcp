package server

import (
	"github.com/josedab/kafkamcp/internal/audit"
	"github.com/josedab/kafkamcp/internal/kafka"
	"github.com/josedab/kafkamcp/internal/lagdiag"
	"github.com/josedab/kafkamcp/internal/masking"
	"github.com/josedab/kafkamcp/internal/metrics"
	"github.com/josedab/kafkamcp/internal/onboarding"
	"github.com/josedab/kafkamcp/internal/opa"
	"github.com/josedab/kafkamcp/internal/ratelimit"
)

// Option configures a Server during construction.
type Option func(*Server)

// WithOperationProvider replaces the native Kafka operation provider. The
// provider still satisfies KafkaManager, so all existing handlers remain behind
// the same policy/approval/audit boundary.
func WithOperationProvider(provider kafka.OperationProvider) Option {
	return func(s *Server) {
		if provider == nil {
			return
		}
		s.operationProvider = provider
		s.kafkaManager = provider
	}
}

// WithAuditLogger sets a custom audit logger.
func WithAuditLogger(a *audit.Logger) Option {
	return func(s *Server) { s.auditLogger = a }
}

// WithRateLimiter sets a custom rate limiter.
func WithRateLimiter(rl *ratelimit.Limiter) Option {
	return func(s *Server) { s.rateLimiter = rl }
}

// WithMetrics sets a custom metrics collector.
func WithMetrics(m *metrics.Metrics) Option {
	return func(s *Server) { s.metrics = m }
}

// WithMaskingEngine sets a custom masking engine.
func WithMaskingEngine(m *masking.Engine) Option {
	return func(s *Server) { s.maskingEngine = m }
}

// WithLagAnalyzer sets a custom lag analyzer.
func WithLagAnalyzer(l *lagdiag.Analyzer) Option {
	return func(s *Server) { s.lagAnalyzer = l }
}

// WithOnboardingEngine sets a custom onboarding engine.
func WithOnboardingEngine(o *onboarding.Engine) Option {
	return func(s *Server) { s.onboardingEngine = o }
}

// WithOPAPolicyEngine sets a custom OPA policy engine.
func WithOPAPolicyEngine(o *opa.PolicyEngine) Option {
	return func(s *Server) { s.opaPolicyEngine = o }
}
