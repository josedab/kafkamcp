package server

import (
	"context"
	"strings"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/mark3labs/mcp-go/mcp"
)

// ===================== handlePluginRegistry =====================

func TestHandlePluginRegistry_Success(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handlePluginRegistry(context.Background(), mcp.ReadResourceRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) == 0 {
		t.Fatal("expected non-empty result")
	}
	text := result[0].(mcp.TextResourceContents).Text
	if !strings.Contains(text, "plugins") {
		t.Error("expected plugins in result")
	}
}

func TestHandlePluginRegistry_NilRegistry(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	s.pluginRegistry = nil
	result, err := s.handlePluginRegistry(context.Background(), mcp.ReadResourceRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) == 0 {
		t.Fatal("expected non-empty result")
	}
	text := result[0].(mcp.TextResourceContents).Text
	if !strings.Contains(text, "total_count") {
		t.Error("expected total_count in result")
	}
}

// ===================== handleWatchSubscriptionsResource =====================

func TestHandleWatchSubscriptionsResource_Success(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleWatchSubscriptionsResource(context.Background(), mcp.ReadResourceRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) == 0 {
		t.Fatal("expected non-empty result")
	}
	text := result[0].(mcp.TextResourceContents).Text
	if !strings.Contains(text, "subscriptions") {
		t.Error("expected subscriptions in result")
	}
}

func TestHandleWatchSubscriptionsResource_NilManager(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	s.watchManager = nil
	result, err := s.handleWatchSubscriptionsResource(context.Background(), mcp.ReadResourceRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) == 0 {
		t.Fatal("expected non-empty result")
	}
	text := result[0].(mcp.TextResourceContents).Text
	if !strings.Contains(text, "total_count") {
		t.Error("expected total_count in result")
	}
}

// ===================== handleReplicationStatus =====================

func TestHandleReplicationStatus_NilMonitor_Mock(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	s.replicationMon = nil
	result, err := s.handleReplicationStatus(context.Background(), mcp.ReadResourceRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) == 0 {
		t.Fatal("expected non-empty result")
	}
	text := result[0].(mcp.TextResourceContents).Text
	if !strings.Contains(text, "total_links") {
		t.Error("expected total_links in result")
	}
}

func TestHandleReplicationStatus_WithMonitor(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleReplicationStatus(context.Background(), mcp.ReadResourceRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) == 0 {
		t.Fatal("expected non-empty result")
	}
}

// ===================== computeSchemaDiff =====================

func TestComputeSchemaDiff_AddedRemovedModified(t *testing.T) {
	from := map[string]any{
		"fields": []any{
			map[string]any{"name": "id", "type": "int"},
			map[string]any{"name": "name", "type": "string"},
			map[string]any{"name": "old_field", "type": "string"},
		},
	}
	to := map[string]any{
		"fields": []any{
			map[string]any{"name": "id", "type": "long"},
			map[string]any{"name": "name", "type": "string"},
			map[string]any{"name": "new_field", "type": "int"},
		},
	}
	diff := computeSchemaDiff(from, to)

	added := diff["added"].([]string)
	removed := diff["removed"].([]string)
	changed := diff["changed"].([]map[string]any)
	unchanged := diff["unchanged"].(int)

	if len(added) != 1 || added[0] != "new_field" {
		t.Errorf("expected [new_field] added, got %v", added)
	}
	if len(removed) != 1 || removed[0] != "old_field" {
		t.Errorf("expected [old_field] removed, got %v", removed)
	}
	if len(changed) != 1 {
		t.Errorf("expected 1 changed, got %d", len(changed))
	}
	if unchanged != 1 {
		t.Errorf("expected 1 unchanged, got %d", unchanged)
	}
}

func TestComputeSchemaDiff_IdenticalSchemas(t *testing.T) {
	schema := map[string]any{
		"fields": []any{
			map[string]any{"name": "id", "type": "int"},
		},
	}
	diff := computeSchemaDiff(schema, schema)
	unchanged := diff["unchanged"].(int)
	if unchanged != 1 {
		t.Errorf("expected 1 unchanged, got %d", unchanged)
	}
}

// ===================== extractSchemaFields =====================

func TestExtractSchemaFields_AvroFormat(t *testing.T) {
	schema := map[string]any{
		"fields": []any{
			map[string]any{"name": "id", "type": "int"},
			map[string]any{"name": "name", "type": "string"},
		},
	}
	fields := extractSchemaFields(schema)
	if len(fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(fields))
	}
}

func TestExtractSchemaFields_JSONSchemaFormat(t *testing.T) {
	schema := map[string]any{
		"properties": map[string]any{
			"id":   map[string]any{"type": "integer"},
			"name": map[string]any{"type": "string"},
		},
	}
	fields := extractSchemaFields(schema)
	if len(fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(fields))
	}
}

func TestExtractSchemaFields_StringJSON(t *testing.T) {
	schema := `{"fields": [{"name": "id", "type": "int"}]}`
	fields := extractSchemaFields(schema)
	if len(fields) != 1 {
		t.Errorf("expected 1 field, got %d", len(fields))
	}
}

// ===================== formatDuration =====================

func TestFormatDuration_Milliseconds(t *testing.T) {
	result := formatDuration(500 * 1e6) // 500ms in nanoseconds
	if !strings.Contains(result, "ms") {
		t.Errorf("expected ms format, got %s", result)
	}
}

// ===================== Server getters =====================

func TestWatchManager(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	if s.WatchManager() == nil {
		t.Error("expected non-nil watch manager")
	}
}

func TestMaskingEngine(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	if s.MaskingEngine() == nil {
		t.Error("expected non-nil masking engine")
	}
}
