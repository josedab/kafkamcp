package server

import (
	"testing"

	"github.com/josedab/kafkamcp/internal/audit"
	"github.com/josedab/kafkamcp/internal/lagdiag"
	"github.com/josedab/kafkamcp/internal/masking"
	"github.com/josedab/kafkamcp/internal/metrics"
	"github.com/josedab/kafkamcp/internal/onboarding"
	"github.com/josedab/kafkamcp/internal/opa"
	"github.com/josedab/kafkamcp/internal/ratelimit"
)

func TestWithOptions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		option Option
		verify func(s *Server) bool
	}{
		{
			name:   "WithAuditLogger",
			option: WithAuditLogger(&audit.Logger{}),
			verify: func(s *Server) bool { return s.auditLogger != nil },
		},
		{
			name:   "WithRateLimiter",
			option: WithRateLimiter(&ratelimit.Limiter{}),
			verify: func(s *Server) bool { return s.rateLimiter != nil },
		},
		{
			name:   "WithMetrics",
			option: WithMetrics(&metrics.Metrics{}),
			verify: func(s *Server) bool { return s.metrics != nil },
		},
		{
			name:   "WithMaskingEngine",
			option: WithMaskingEngine(&masking.Engine{}),
			verify: func(s *Server) bool { return s.maskingEngine != nil },
		},
		{
			name:   "WithLagAnalyzer",
			option: WithLagAnalyzer(&lagdiag.Analyzer{}),
			verify: func(s *Server) bool { return s.lagAnalyzer != nil },
		},
		{
			name:   "WithOnboardingEngine",
			option: WithOnboardingEngine(&onboarding.Engine{}),
			verify: func(s *Server) bool { return s.onboardingEngine != nil },
		},
		{
			name:   "WithOPAPolicyEngine",
			option: WithOPAPolicyEngine(&opa.PolicyEngine{}),
			verify: func(s *Server) bool { return s.opaPolicyEngine != nil },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{}
			tt.option(s)
			if !tt.verify(s) {
				t.Errorf("%s did not set the expected field", tt.name)
			}
		})
	}
}

func TestWithOptions_Composable(t *testing.T) {
	t.Parallel()
	s := &Server{}
	opts := []Option{
		WithAuditLogger(&audit.Logger{}),
		WithRateLimiter(&ratelimit.Limiter{}),
		WithMetrics(&metrics.Metrics{}),
	}

	for _, opt := range opts {
		opt(s)
	}

	if s.auditLogger == nil {
		t.Error("auditLogger not set after chaining")
	}
	if s.rateLimiter == nil {
		t.Error("rateLimiter not set after chaining")
	}
	if s.metrics == nil {
		t.Error("metrics not set after chaining")
	}
}

func TestWithOptions_OverridesExisting(t *testing.T) {
	t.Parallel()
	s := &Server{}
	first := &audit.Logger{}
	second := &audit.Logger{}

	WithAuditLogger(first)(s)
	WithAuditLogger(second)(s)

	if s.auditLogger != second {
		t.Error("expected second value to override first")
	}
}
