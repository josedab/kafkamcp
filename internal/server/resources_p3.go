package server

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/masking"
	"github.com/kafkamcp/kafkamcp/internal/watch"
	"github.com/mark3labs/mcp-go/mcp"
)

// ===================== P3 RESOURCE HANDLERS =====================

// handleReplicationStatus returns cross-cluster replication status.
func (s *Server) handleReplicationStatus(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	start := time.Now()
	agentID := extractAgentIDFromResource(request)
	if err := s.validateResourceAgentID(agentID); err != nil {
		s.logAudit(agentID, "resource:kafka://replication/status", nil, "", "error", 0, err, start)
		return nil, err
	}
	if err := s.checkResourceRateLimit(agentID, "resource:kafka://replication/status"); err != nil {
		s.logAudit(agentID, "resource:kafka://replication/status", nil, "", "error", 0, err, start)
		return nil, err
	}

	if s.replicationMon == nil {
		return marshalResourceJSON("kafka://replication/status", map[string]any{
			"links": []any{}, "total_links": 0, "active_links": 0, "alerts": []any{},
		})
	}

	status := s.replicationMon.GetStatus(ctx)
	s.logAudit(agentID, "resource:kafka://replication/status", nil, "", "success", status.TotalLinks, nil, start)
	return marshalResourceJSON("kafka://replication/status", status)
}

// handlePluginRegistry returns registered plugins and extensions.
func (s *Server) handlePluginRegistry(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	start := time.Now()
	agentID := extractAgentIDFromResource(request)
	if err := s.validateResourceAgentID(agentID); err != nil {
		s.logAudit(agentID, "resource:kafka://plugins", nil, "", "error", 0, err, start)
		return nil, err
	}
	if err := s.checkResourceRateLimit(agentID, "resource:kafka://plugins"); err != nil {
		s.logAudit(agentID, "resource:kafka://plugins", nil, "", "error", 0, err, start)
		return nil, err
	}

	if s.pluginRegistry == nil {
		return marshalResourceJSON("kafka://plugins", map[string]any{"plugins": []any{}, "total_count": 0})
	}

	plugins := s.pluginRegistry.List()
	s.logAudit(agentID, "resource:kafka://plugins", nil, "", "success", len(plugins), nil, start)
	return marshalResourceJSON("kafka://plugins", map[string]any{
		"plugins":     plugins,
		"total_count": len(plugins),
	})
}

// handleWatchSubscriptionsResource returns active watch subscriptions.
func (s *Server) handleWatchSubscriptionsResource(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	start := time.Now()
	agentID := extractAgentIDFromResource(request)
	if err := s.validateResourceAgentID(agentID); err != nil {
		s.logAudit(agentID, "resource:kafka://watch/subscriptions", nil, "", "error", 0, err, start)
		return nil, err
	}
	if err := s.checkResourceRateLimit(agentID, "resource:kafka://watch/subscriptions"); err != nil {
		s.logAudit(agentID, "resource:kafka://watch/subscriptions", nil, "", "error", 0, err, start)
		return nil, err
	}

	if s.watchManager == nil {
		return marshalResourceJSON("kafka://watch/subscriptions", map[string]any{
			"subscriptions": []any{}, "total_count": 0,
		})
	}

	subs := s.watchManager.ListSubscriptions("")
	s.logAudit(agentID, "resource:kafka://watch/subscriptions", nil, "", "success", len(subs), nil, start)
	return marshalResourceJSON("kafka://watch/subscriptions", map[string]any{
		"subscriptions": subs,
		"total_count":   len(subs),
	})
}

// computeSchemaDiff computes a field-by-field diff between two schema definitions.
func computeSchemaDiff(schemaFrom, schemaTo any) map[string]any {
	diff := map[string]any{
		"added":    []string{},
		"removed":  []string{},
		"changed":  []map[string]any{},
		"unchanged": 0,
	}

	fromFields := extractSchemaFields(schemaFrom)
	toFields := extractSchemaFields(schemaTo)

	var added, removed []string
	var changed []map[string]any
	unchanged := 0

	// Find added and changed fields
	for name, toType := range toFields {
		if fromType, exists := fromFields[name]; exists {
			if !reflect.DeepEqual(fromType, toType) {
				changed = append(changed, map[string]any{
					"field": name,
					"from":  fromType,
					"to":    toType,
				})
			} else {
				unchanged++
			}
		} else {
			added = append(added, name)
		}
	}

	// Find removed fields
	for name := range fromFields {
		if _, exists := toFields[name]; !exists {
			removed = append(removed, name)
		}
	}

	diff["added"] = added
	diff["removed"] = removed
	diff["changed"] = changed
	diff["unchanged"] = unchanged

	return diff
}

// extractSchemaFields extracts field names and types from a schema definition.
func extractSchemaFields(schema any) map[string]any {
	fields := make(map[string]any)

	switch s := schema.(type) {
	case map[string]any:
		if fieldsArr, ok := s["fields"].([]any); ok {
			for _, f := range fieldsArr {
				if field, ok := f.(map[string]any); ok {
					name, _ := field["name"].(string) //nolint:errcheck // safe: zero value if not string
					if name != "" {
						fields[name] = field["type"]
					}
				}
			}
		}
		// Handle JSON Schema properties
		if props, ok := s["properties"].(map[string]any); ok {
			for name, prop := range props {
				fields[name] = prop
			}
		}
	case string:
		// Try to parse JSON string
		var parsed map[string]any
		if err := json.Unmarshal([]byte(s), &parsed); err == nil {
			return extractSchemaFields(parsed)
		}
	}

	return fields
}

// WatchManager returns the watch manager for external access.
func (s *Server) WatchManager() *watch.Manager {
	return s.watchManager
}

// MaskingEngine returns the masking engine for external access.
func (s *Server) MaskingEngine() *masking.Engine {
	return s.maskingEngine
}

// formatDuration formats a duration as human-readable string.
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return fmt.Sprintf("%.1fm", d.Minutes())
}

// Ensure unused imports are used
var _ = strings.Contains
