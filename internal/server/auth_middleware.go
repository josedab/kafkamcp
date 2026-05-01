package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/metrics"
	"github.com/mark3labs/mcp-go/mcp"
	otelTrace "go.opentelemetry.io/otel/trace"
)

// extractAgentID gets the agent ID from the MCP request headers or defaults.
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

// extractAgentIDFromResource gets the agent ID from a resource request.
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
		agentID := extractAgentID(request)
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

		result, err := handler(ctx, request, agentID, args, start)
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
