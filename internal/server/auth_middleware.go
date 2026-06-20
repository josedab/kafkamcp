package server

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	otelTrace "go.opentelemetry.io/otel/trace"

	"github.com/josedab/kafkamcp/internal/auth"
	"github.com/josedab/kafkamcp/internal/auth/identity"
	"github.com/josedab/kafkamcp/internal/metrics"
)

// withIdentity returns a derived context carrying the verified
// identity. Thin shim over identity.WithIdentity to keep the rest of
// the server package agnostic of the storage location.
func withIdentity(ctx context.Context, ident *identity.AgentIdentity) context.Context {
	return identity.WithIdentity(ctx, ident)
}

// identityFromContext returns the verified identity attached to ctx, or
// nil when none is present.
func identityFromContext(ctx context.Context) *identity.AgentIdentity {
	v, _ := identity.FromContext(ctx)
	return v
}

// resolveAgentIdentity runs the configured identity provider against
// the request headers and returns the resolved agent ID plus the
// verified AgentIdentity (which may be nil). It falls back to legacy
// X-Agent-ID / X-Client-ID headers only when no provider is configured
// or when the operator has explicitly opted into header fallback via
// `policy.identity.allow_header_fallback: true`. For any cryptographic
// provider (static/oidc/mtls) the default is to NOT fall back —
// otherwise an attacker can bypass identity verification by simply
// omitting the bearer token. See [config.IdentityConfig.HeaderFallbackEnabled].
//
// When an identity has already been attached to ctx by an upstream
// HTTP middleware (e.g. the mTLS middleware that runs against the raw
// *http.Request before the MCP handler), that identity is preferred
// and re-verification is skipped.
func (s *Server) resolveAgentIdentity(ctx context.Context, header http.Header) (string, *identity.AgentIdentity) {
	if ident, ok := identity.FromContext(ctx); ok && ident != nil && ident.ID != "" {
		s.warnOnIdentityConflict(ident, header)
		return sanitizeAgentID(ident.ID), ident
	}
	if s.identityProvider != nil && header != nil {
		ident, err := s.identityProvider.Verify(ctx, header)
		switch {
		case err == nil && ident != nil && ident.ID != "":
			s.warnOnIdentityConflict(ident, header)
			return sanitizeAgentID(ident.ID), ident
		case errors.Is(err, identity.ErrNoCredentials):
			if !s.config.Policies.Identity.HeaderFallbackEnabled() {
				s.logger.Warn("identity provider returned no credentials and header fallback is disabled; rejecting",
					"provider", s.identityProvider.Name())
				return "anonymous", nil
			}
			// fall through to header-based behavior (legacy mode)
		case err != nil:
			s.logger.Warn("agent identity verification failed",
				"provider", s.identityProvider.Name(),
				"error", err.Error())
			if !s.config.Policies.Identity.HeaderFallbackEnabled() {
				return "anonymous", nil
			}
		}
	}
	if header != nil {
		if id := header.Get("X-Agent-ID"); id != "" {
			return sanitizeAgentID(id), nil
		}
		if id := header.Get("X-Client-ID"); id != "" {
			return sanitizeAgentID(id), nil
		}
	}
	return "anonymous", nil
}

// warnOnIdentityConflict emits a structured warning when a verified
// identity is accompanied by a legacy X-Agent-ID / X-Client-ID header
// asserting a different agent. This is a strong indicator of either a
// misconfigured caller or an attacker probing whether the legacy
// fallback path can override the verified identity. We never trust the
// header in this case, but we surface the conflict so operators can
// investigate.
func (s *Server) warnOnIdentityConflict(ident *identity.AgentIdentity, header http.Header) {
	if ident == nil || header == nil {
		return
	}
	for _, name := range []string{"X-Agent-ID", "X-Client-ID"} {
		claimed := header.Get(name)
		if claimed == "" {
			continue
		}
		if claimed == ident.ID {
			continue
		}
		s.logger.Warn("agent identity conflict: header asserts different ID than verified credentials",
			"verified_id", ident.ID,
			"verified_method", ident.Method,
			"verified_issuer", ident.Issuer,
			"header", name,
			"header_value", claimed,
		)
		if s.metrics != nil && s.metrics.IdentityConflicts != nil {
			s.metrics.IdentityConflicts.WithLabelValues(metrics.SanitizeAgentLabel(ident.ID), name).Inc()
		}
	}
}

// extractAgentID is the package-level shim used by tests and pre-existing
// call sites that don't have access to the Server. It mirrors the old
// behavior (header-only).
func extractAgentID(request mcp.CallToolRequest) string {
	if request.Header != nil {
		if id := request.Header.Get("X-Agent-ID"); id != "" {
			return sanitizeAgentID(id)
		}
		if id := request.Header.Get("X-Client-ID"); id != "" {
			return sanitizeAgentID(id)
		}
	}
	return "anonymous"
}

// extractAgentIDFromResource is the package-level shim for resource
// handlers in legacy code paths.
func extractAgentIDFromResource(request mcp.ReadResourceRequest) string {
	if request.Header != nil {
		if id := request.Header.Get("X-Agent-ID"); id != "" {
			return sanitizeAgentID(id)
		}
		if id := request.Header.Get("X-Client-ID"); id != "" {
			return sanitizeAgentID(id)
		}
	}
	return "anonymous"
}

// sanitizeAgentID validates and sanitizes an agent ID to contain only
// alphanumeric characters, hyphens, and underscores.
func sanitizeAgentID(id string) string {
	if len(id) > 128 {
		id = id[:128]
	}
	for _, c := range id {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.') {
			return "anonymous"
		}
	}
	return id
}

// validateAgentID checks that the claimed agent ID exists in policy config when default_deny is true.
func (s *Server) validateAgentID(agentID string) *mcp.CallToolResult {
	if !s.policyEngine.DefaultDeny() {
		return nil
	}
	if agentID == "anonymous" || !s.policyEngine.AgentExists(agentID) {
		return mcp.NewToolResultError((&AuthError{AgentID: agentID, Reason: "valid X-Agent-ID header must be provided"}).Error())
	}
	return nil
}

// validateResourceAgentID validates agent identity for resource handlers.
func (s *Server) validateResourceAgentID(agentID string) error {
	if !s.policyEngine.DefaultDeny() {
		return nil
	}
	if agentID == "anonymous" || !s.policyEngine.AgentExists(agentID) {
		return &AuthError{AgentID: agentID, Reason: "valid X-Agent-ID header must be provided"}
	}
	return nil
}

// --- Auth & Rate Limit helpers ---

// toolAuthConfig defines the authorization requirements for a tool handler,
// specifying which resource type and action to check against the policy engine.
type toolAuthConfig struct {
	tool         string
	resourceType auth.ResourceType
	action       auth.Action
	// resourceName extracts the resource name from the request.
	// If nil, defaults to "*".
	resourceName func(mcp.CallToolRequest) string
}

// toolHandler is a handler function that processes an MCP tool call after
// agent identity, rate limiting, and authorization have been validated.
type toolHandler func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error)

// withToolAuth wraps a tool handler with cross-cutting concerns: agent ID
// extraction, OpenTelemetry span creation, rate limiting, authorization
// checks, and audit logging. It eliminates repeated boilerplate across handlers.
func (s *Server) withToolAuth(cfg toolAuthConfig, handler toolHandler) func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		start := time.Now()
		agentID, ident := s.resolveAgentIdentity(ctx, request.Header)
		if ident != nil {
			ctx = withIdentity(ctx, ident)
			if s.activation != nil {
				s.activation.ObserveVerifiedAgent(agentID)
			}
		}
		args := request.GetArguments()

		// Start OTel span if provider is configured
		if s.otelProvider != nil {
			cluster := ""
			if args != nil {
				if c, ok := args["cluster"].(string); ok {
					cluster = c
				}
			}
			var otelSpan otelTrace.Span
			ctx, otelSpan = s.otelProvider.StartToolSpan(ctx, cfg.tool, agentID, cluster)
			defer otelSpan.End()
		}

		if deny := s.checkRateLimit(agentID, cfg.tool); deny != nil {
			return deny, nil
		}

		resourceName := "*"
		if cfg.resourceName != nil {
			resourceName = cfg.resourceName(request)
		}
		if deny := s.checkAuth(agentID, cfg.tool, cfg.resourceType, resourceName, cfg.action); deny != nil {
			return deny, nil
		}

		receipt, replay, err := s.beginIdempotentMutation(ctx, cfg.tool, agentID, args)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		if replay != nil {
			return replay, nil
		}

		// Common approval enforcement for write/destructive tools. Read-only
		// tools and disabled enforcement short-circuit inside enforceApproval.
		if deny := s.enforceApproval(ctx, cfg.tool, agentID, request, args); deny != nil {
			s.abortIdempotentMutation(ctx, receipt)
			return deny, nil
		}

		result, err := handler(ctx, request, agentID, args, start)
		if err != nil || (result != nil && result.IsError) {
			s.abortIdempotentMutation(ctx, receipt)
			return result, err
		}
		if err := s.completeIdempotentMutation(ctx, receipt, result); err != nil {
			return mcp.NewToolResultError("operation completed but its idempotency receipt could not be persisted; inspect the target state before retrying"), nil
		}
		return result, err
	}
}

// checkAuth verifies that the given agent is authorized to perform the specified
// action on the resource. Returns a denial result if unauthorized, nil if allowed.
func (s *Server) checkAuth(agentID, tool string, resource auth.ResourceType, resourceName string, action auth.Action) *mcp.CallToolResult {
	if deny := s.validateAgentID(agentID); deny != nil {
		return deny
	}
	if !s.policyEngine.Authorize(agentID, resource, resourceName, action) {
		s.metrics.AuthzDenials.WithLabelValues(metrics.SanitizeAgentLabel(agentID), tool, string(action)).Inc()
		s.logger.Warn("authorization denied", "agent", agentID, "tool", tool, "resource", resourceName, "action", action)
		return mcp.NewToolResultError((&AuthzError{AgentID: agentID, Tool: tool, ResourceName: resourceName, Action: string(action)}).Error())
	}
	return nil
}

// checkRateLimit checks whether the agent has exceeded its rate limit.
// Returns a denial result if rate-limited, nil if the request is allowed.
func (s *Server) checkRateLimit(agentID, tool string) *mcp.CallToolResult {
	if !s.rateLimiter.Allow(agentID) {
		s.metrics.RateLimitDenials.WithLabelValues(metrics.SanitizeAgentLabel(agentID), tool).Inc()
		return mcp.NewToolResultError((&RateLimitError{AgentID: agentID, Resource: tool}).Error())
	}
	return nil
}

// checkResourceRateLimit checks rate limits for resource handlers.
func (s *Server) checkResourceRateLimit(agentID, resource string) error {
	if !s.rateLimiter.Allow(agentID) {
		s.metrics.RateLimitDenials.WithLabelValues(metrics.SanitizeAgentLabel(agentID), resource).Inc()
		return &RateLimitError{AgentID: agentID, Resource: resource}
	}
	return nil
}
