package server

import (
	"context"
	"strings"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/audit"
	"github.com/kafkamcp/kafkamcp/internal/metrics"
)

// logAudit records a tool call in the audit log and increments Prometheus metrics.
//
// ctx is consulted for a verified [identity.AgentIdentity] attached by
// the auth middleware (see internal/server/auth_middleware.go). When
// present, the entry's IdentityMethod / Issuer / Subject fields are
// populated so audit consumers can distinguish a header-asserted
// agent ID (legacy / dev) from one verified by OIDC, static-token, or
// mTLS. ctx may be context.Background() — the identity fields then
// remain empty, matching the pre-identity-verification behaviour.
func (s *Server) logAudit(ctx context.Context, agentID, tool string, params map[string]any, cluster, status string, count int, err error, start time.Time) {
	latency := time.Since(start)
	entry := audit.Entry{
		AgentID:    agentID,
		Tool:       tool,
		Parameters: sanitizeAuditParams(tool, params),
		Cluster:    s.resolveClusterName(cluster),
		Result:     audit.Result{Status: status, MessageCount: count},
		LatencyMs:  latency.Milliseconds(),
	}
	if ident := identityFromContext(ctx); ident != nil {
		entry.IdentityMethod = ident.Method
		entry.Issuer = ident.Issuer
		entry.Subject = ident.Subject
	}
	if err != nil {
		entry.Result.Error = categorizeError(err)
	}
	s.auditLogger.Log(entry)
	safeAgent := metrics.SanitizeAgentLabel(agentID)
	s.metrics.ToolCallsTotal.WithLabelValues(tool, safeAgent, entry.Cluster, status).Inc()
	s.metrics.ToolLatencySeconds.WithLabelValues(tool, safeAgent, entry.Cluster).Observe(latency.Seconds())

	// Record for agent profiling
	if s.agentProfiler != nil {
		s.agentProfiler.RecordCall(tool, agentID, nil, latency.Milliseconds(), status)
	}
}

// categorizeError maps raw errors to safe category strings to prevent
// leaking internal details (broker IPs, TLS info, connection strings)
// in audit logs and dashboard responses.
func categorizeError(err error) string {
	msg := err.Error()
	lower := strings.ToLower(msg)

	switch {
	case strings.Contains(lower, "context deadline exceeded") || strings.Contains(lower, "timeout") || strings.Contains(lower, "timed out"):
		return "timeout"
	case strings.Contains(lower, "connection refused") || strings.Contains(lower, "connection reset") || strings.Contains(lower, "broken pipe") || strings.Contains(lower, "no such host") || strings.Contains(lower, "dial tcp"):
		return "connection_error"
	case strings.Contains(lower, "sasl") || strings.Contains(lower, "authentication") || strings.Contains(lower, "unauthorized") || strings.Contains(lower, "credential"):
		return "auth_error"
	case strings.Contains(lower, "tls") || strings.Contains(lower, "certificate") || strings.Contains(lower, "x509"):
		return "tls_error"
	case strings.Contains(lower, "permission") || strings.Contains(lower, "forbidden") || strings.Contains(lower, "not authorized") || strings.Contains(lower, "acl"):
		return "permission_denied"
	case strings.Contains(lower, "not found") || strings.Contains(lower, "unknown topic") || strings.Contains(lower, "does not exist"):
		return "not_found"
	case strings.Contains(lower, "already exists") || strings.Contains(lower, "duplicate"):
		return "already_exists"
	case strings.Contains(lower, "invalid") || strings.Contains(lower, "malformed"):
		return "invalid_request"
	default:
		return "internal_error"
	}
}

// sanitizeAuditParams redacts sensitive data from audit log parameters.
// It applies tool-specific redactions and deep-inspects all parameters for
// sensitive key patterns (password, secret, token, key, credential).
func sanitizeAuditParams(tool string, params map[string]any) map[string]any {
	if params == nil {
		return nil
	}

	sensitiveTools := map[string][]string{
		"kafka_produce":                    {"messages"},
		"kafka_consume":                    {"messages"},
		"kafka_alter_topic":                {"configs"},
		"kafka_create_topic":               {"configs"},
		"kafka_check_schema_compatibility": {"schema"},
		"kafka_describe_connector":         {"name"},
	}

	sanitized := make(map[string]any, len(params))
	for k, v := range params {
		sanitized[k] = v
	}

	// Tool-specific redactions
	if keys, ok := sensitiveTools[tool]; ok {
		for _, key := range keys {
			if _, exists := sanitized[key]; exists {
				sanitized[key] = "**REDACTED**"
			}
		}
	}

	// Deep-redact any parameter whose key matches sensitive patterns
	deepRedactSensitiveKeys(sanitized)

	return sanitized
}

// sensitiveKeyPatterns contains substrings that indicate a parameter key holds sensitive data.
var sensitiveKeyPatterns = []string{"password", "secret", "token", "key", "credential", "api_key", "apikey", "webhook_url", "sasl", "bearer", "jwt", "auth", "connection_string"}

// deepRedactSensitiveKeys recursively inspects map values and redacts any key
// matching sensitive patterns.
func deepRedactSensitiveKeys(m map[string]any) {
	for k, v := range m {
		lower := strings.ToLower(k)
		isSensitive := false
		for _, pat := range sensitiveKeyPatterns {
			if strings.Contains(lower, pat) {
				isSensitive = true
				break
			}
		}
		if isSensitive {
			m[k] = "**REDACTED**"
			continue
		}
		// Recurse into nested maps
		if nested, ok := v.(map[string]any); ok {
			deepRedactSensitiveKeys(nested)
		}
	}
}
