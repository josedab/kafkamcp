package server

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
)

// kafkaIdentifierPattern matches valid Kafka topic names and consumer group IDs.
var kafkaIdentifierPattern = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

// toolError logs the raw error server-side and returns a generic error message
// to the client, preventing leakage of internal details like broker IPs,
// TLS certificate info, connection strings, and file system paths.
func (s *Server) toolError(msg string, err error) *mcp.CallToolResult {
	s.logger.Error(msg, "error", err)
	return mcp.NewToolResultError(msg)
}

// toolErrorWithContext logs the raw error with tool name and request ID for
// faster debugging, and returns a generic error message to the client.
func (s *Server) toolErrorWithContext(msg string, err error, toolName string, requestID string) *mcp.CallToolResult {
	s.logger.Error(msg, "error", err, "tool", toolName, "request_id", requestID)
	return mcp.NewToolResultError(msg)
}

// redactURL masks a URL for safe inclusion in responses and audit logs,
// preserving only the scheme and host.
func redactURL(rawURL string) string {
	if rawURL == "" {
		return ""
	}
	// Show scheme + host, redact path/query
	if idx := strings.Index(rawURL, "://"); idx >= 0 {
		rest := rawURL[idx+3:]
		if slashIdx := strings.Index(rest, "/"); slashIdx >= 0 {
			return rawURL[:idx+3+slashIdx] + "/***"
		}
		return rawURL + "/***"
	}
	return "***"
}

// marshalResult serializes a value as indented JSON and wraps it in an MCP tool result.
func marshalResult(v any) (*mcp.CallToolResult, error) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		slog.Error("failed to marshal tool result", "error", err)
		return mcp.NewToolResultError("internal error: failed to format response"), nil
	}
	return mcp.NewToolResultText(string(data)), nil
}

// marshalResourceJSON serializes a value as JSON resource contents for MCP resource responses.
func marshalResourceJSON(uri string, v any) ([]mcp.ResourceContents, error) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshaling resource: %w", err)
	}
	return []mcp.ResourceContents{
		mcp.TextResourceContents{URI: uri, MIMEType: "application/json", Text: string(data)},
	}, nil
}

// validateStringLength checks that a string parameter doesn't exceed the maximum length.
func validateStringLength(name, value string) *mcp.CallToolResult {
	if len(value) > maxInputLength {
		return mcp.NewToolResultError(fmt.Sprintf("%s exceeds maximum length of %d characters", name, maxInputLength))
	}
	return nil
}

// Valid values for serializer/deserializer and acks parameters.
var (
	validDeserializers = map[string]bool{"auto": true, "json": true, "string": true, "bytes": true}
	validAcks          = map[string]bool{"all": true, "0": true, "1": true, "-1": true}
)

// validateKafkaIdentifier checks that a Kafka topic name or consumer group ID
// contains only valid characters and does not use reserved prefixes.
func validateKafkaIdentifier(name, value string) *mcp.CallToolResult {
	if value == "" {
		return nil
	}
	if !kafkaIdentifierPattern.MatchString(value) {
		return mcp.NewToolResultError(fmt.Sprintf("%s contains invalid characters; only alphanumeric, '.', '_', and '-' are allowed", name))
	}
	if strings.HasPrefix(value, "__") {
		return mcp.NewToolResultError(fmt.Sprintf("%s must not start with '__' (reserved prefix)", name))
	}
	return nil
}

// validateEnum checks that a string parameter is one of the allowed values.
func validateEnum(name, value string, allowed map[string]bool) *mcp.CallToolResult {
	if !allowed[value] {
		keys := make([]string, 0, len(allowed))
		for k := range allowed {
			keys = append(keys, k)
		}
		return mcp.NewToolResultError(fmt.Sprintf("%s must be one of: %s", name, strings.Join(keys, ", ")))
	}
	return nil
}
