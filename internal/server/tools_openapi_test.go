package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

func TestGenerateOpenAPIPaths_HasExpectedPaths(t *testing.T) {
	t.Parallel()
	s := newTestServerWithMock(t, &kafka.MockKafkaManager{})
	paths := s.generateOpenAPIPaths()

	expectedPaths := []string{
		"/tools/kafka_list_topics",
		"/tools/kafka_consume",
		"/tools/kafka_produce",
		"/tools/kafka_search",
		"/tools/kafka_nl_query",
		"/tools/kafka_migrate_execute",
		"/tools/kafka_migrate_status",
		"/tools/kafka_migrate_rollback",
	}

	for _, p := range expectedPaths {
		if _, ok := paths[p]; !ok {
			t.Errorf("missing expected path: %s", p)
		}
	}
}

func TestGenerateOpenAPIPaths_PostWithSummaryAndDescription(t *testing.T) {
	t.Parallel()
	s := newTestServerWithMock(t, &kafka.MockKafkaManager{})
	paths := s.generateOpenAPIPaths()

	for path, def := range paths {
		pathDef := def.(map[string]any)
		post, ok := pathDef["post"].(map[string]any)
		if !ok {
			t.Errorf("path %s missing 'post' method", path)
			continue
		}
		if post["summary"] == nil || post["summary"] == "" {
			t.Errorf("path %s missing summary", path)
		}
		if post["description"] == nil || post["description"] == "" {
			t.Errorf("path %s missing description", path)
		}
	}
}

func TestGenerateOpenAPIPaths_OperationIdStripsPrefix(t *testing.T) {
	t.Parallel()
	s := newTestServerWithMock(t, &kafka.MockKafkaManager{})
	paths := s.generateOpenAPIPaths()

	for path, def := range paths {
		post := def.(map[string]any)["post"].(map[string]any)
		opID := post["operationId"].(string)
		if strings.HasPrefix(opID, "/tools/") {
			t.Errorf("operationId for %s should not have /tools/ prefix: %s", path, opID)
		}
		expectedID := path[7:] // strip /tools/
		if opID != expectedID {
			t.Errorf("path %s: expected operationId %q, got %q", path, expectedID, opID)
		}
	}
}

func TestGenerateOpenAPIPaths_SecurityScheme(t *testing.T) {
	t.Parallel()
	s := newTestServerWithMock(t, &kafka.MockKafkaManager{})
	paths := s.generateOpenAPIPaths()

	for path, def := range paths {
		post := def.(map[string]any)["post"].(map[string]any)
		security, ok := post["security"].([]map[string]any)
		if !ok || len(security) == 0 {
			t.Errorf("path %s missing security scheme", path)
			continue
		}
		if _, ok := security[0]["agentId"]; !ok {
			t.Errorf("path %s security should reference agentId", path)
		}
	}
}

func TestHandleOpenAPISpec_ReturnsValidJSON(t *testing.T) {
	t.Parallel()
	s := newTestServerWithMock(t, &kafka.MockKafkaManager{})
	result, err := s.handleOpenAPISpec(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)

	text := resultText(t, result)
	var spec map[string]any
	if err := json.Unmarshal([]byte(text), &spec); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}

	requiredKeys := []string{"openapi", "info", "servers", "paths", "components"}
	for _, key := range requiredKeys {
		if spec[key] == nil {
			t.Errorf("missing required key: %s", key)
		}
	}

	if spec["openapi"] != "3.0.3" {
		t.Errorf("expected openapi version 3.0.3, got %v", spec["openapi"])
	}
}

func TestHandleOpenAPISpec_InfoFields(t *testing.T) {
	t.Parallel()
	s := newTestServerWithMock(t, &kafka.MockKafkaManager{})
	result, _ := s.handleOpenAPISpec(context.Background(), makeToolRequest(nil))
	text := resultText(t, result)

	var spec map[string]any
	_ = json.Unmarshal([]byte(text), &spec)

	info := spec["info"].(map[string]any)
	if info["title"] == nil || info["title"] == "" {
		t.Error("missing info.title")
	}
	if info["version"] == nil || info["version"] == "" {
		t.Error("missing info.version")
	}
}

func TestHandleOpenAPISpec_ComponentsSecurityScheme(t *testing.T) {
	t.Parallel()
	s := newTestServerWithMock(t, &kafka.MockKafkaManager{})
	result, _ := s.handleOpenAPISpec(context.Background(), makeToolRequest(nil))
	text := resultText(t, result)

	var spec map[string]any
	_ = json.Unmarshal([]byte(text), &spec)

	components := spec["components"].(map[string]any)
	schemes := components["securitySchemes"].(map[string]any)
	agentID := schemes["agentId"].(map[string]any)
	if agentID["type"] != "apiKey" {
		t.Errorf("expected apiKey type, got %v", agentID["type"])
	}
}
