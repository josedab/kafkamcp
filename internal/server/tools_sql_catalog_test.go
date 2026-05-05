package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/autopilot"
	"github.com/kafkamcp/kafkamcp/internal/catalog"
	"github.com/kafkamcp/kafkamcp/internal/digitaltwin"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/kafkamcp/kafkamcp/internal/migration"
	"github.com/kafkamcp/kafkamcp/internal/orchestration"
	"github.com/kafkamcp/kafkamcp/internal/sandbox"
	"github.com/kafkamcp/kafkamcp/internal/streamsql"
	"github.com/mark3labs/mcp-go/mcp"
)

func timeNow() time.Time { return time.Now() }

// newTestServerWithEngines creates a test server with SQL, catalog, autopilot, and orchestration engines.
func newTestServerWithEngines(t *testing.T, mock *kafka.MockKafkaManager) *Server {
	t.Helper()
	s := newTestServer(t)
	s.kafkaManager = mock
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s.streamSQLEngine = streamsql.NewEngine(logger)
	s.catalogEngine = catalog.NewEngine(logger)
	s.orchestrationEngine = orchestration.NewEngine(logger)
	s.autopilotEngine = autopilot.NewEngine(migration.NewPlanner(), digitaltwin.NewEngine(logger), sandbox.NewEngine(logger), logger)
	return s
}

// ===================== handleSQLExecute =====================

func TestHandleSQLExecute_ValidSQL(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{
				{Partition: 0, Offset: 0, Key: "k1", Value: `{"name":"alice","age":30}`, Timestamp: "2024-01-01T00:00:00.000Z"},
				{Partition: 0, Offset: 1, Key: "k2", Value: `{"name":"bob","age":25}`, Timestamp: "2024-01-01T00:00:01.000Z"},
			}, nil
		},
	}
	s := newTestServerWithEngines(t, mock)

	result, err := s.handleSQLExecute(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"sql": "SELECT name FROM users",
	}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if text == "" {
		t.Error("expected non-empty result")
	}
}

func TestHandleSQLExecute_EmptySQL(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	result, err := s.handleSQLExecute(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"sql": "",
	}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "sql query is required") {
		t.Error("expected 'sql query is required' error")
	}
}

func TestHandleSQLExecute_MissingSQL(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	result, err := s.handleSQLExecute(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleSQLExecute_ParseError(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	result, err := s.handleSQLExecute(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"sql": "NOT VALID SQL $$$$",
	}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "SQL parse error") {
		t.Errorf("expected SQL parse error, got: %s", resultText(t, result))
	}
}

func TestHandleSQLExecute_ConsumeFailure(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return nil, fmt.Errorf("broker unavailable")
		},
	}
	s := newTestServerWithEngines(t, mock)

	result, err := s.handleSQLExecute(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"sql": "SELECT * FROM orders",
	}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "failed to consume") {
		t.Errorf("expected consume error, got: %s", resultText(t, result))
	}
}

func TestHandleSQLExecute_LimitClamping(t *testing.T) {
	t.Parallel()
	var capturedLimit int
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			capturedLimit = limit
			return []kafka.Message{}, nil
		},
	}
	s := newTestServerWithEngines(t, mock)

	s.handleSQLExecute(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"sql":   "SELECT * FROM orders",
		"limit": float64(99999),
	}, timeNow())

	if capturedLimit != 10000 {
		t.Errorf("expected limit clamped to 10000, got %d", capturedLimit)
	}
}

func TestHandleSQLExecute_MessageToMapConversion(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{
				{
					Partition: 0, Offset: 42, Key: "order-1",
					Value:   `{"status":"shipped","amount":99.5}`,
					Headers: map[string]string{"trace-id": "abc"},
				},
				{
					Partition: 1, Offset: 10, Key: "order-2",
					Value: "plain-text-value",
				},
				{
					Partition: 2, Offset: 5, Key: "order-3",
					Value: 12345,
				},
			}, nil
		},
	}
	s := newTestServerWithEngines(t, mock)

	result, err := s.handleSQLExecute(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"sql": "SELECT * FROM orders",
	}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== handleSQLExplain =====================

func TestHandleSQLExplain_Valid(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	result, err := s.handleSQLExplain(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"sql": "SELECT name FROM users",
	}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if text == "" {
		t.Error("expected non-empty explain result")
	}
}

func TestHandleSQLExplain_EmptySQL(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	result, err := s.handleSQLExplain(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"sql": "",
	}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleSQLExplain_ParseError(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	result, err := s.handleSQLExplain(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"sql": "INVALID @@@ SQL",
	}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleSQLManage =====================

func TestHandleSQLManage_ListQueries(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	result, err := s.handleSQLManage(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"action": "list",
	}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	var resp map[string]any
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("invalid JSON response: %v", err)
	}
	if _, ok := resp["queries"]; !ok {
		t.Error("expected 'queries' in response")
	}
}

func TestHandleSQLManage_GetQuery_MissingID(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	result, err := s.handleSQLManage(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"action": "get",
	}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "query_id is required") {
		t.Error("expected 'query_id is required' error")
	}
}

func TestHandleSQLManage_GetQuery_NonExistent(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	result, err := s.handleSQLManage(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"action":   "get",
		"query_id": "nonexistent-id",
	}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleSQLManage_DeleteQuery_MissingID(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	result, err := s.handleSQLManage(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"action": "delete",
	}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleSQLManage_DeleteQuery_NonExistent(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	result, err := s.handleSQLManage(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"action":   "delete",
		"query_id": "nonexistent-id",
	}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleSQLManage_InvalidAction(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	result, err := s.handleSQLManage(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"action": "invalid",
	}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "action must be") {
		t.Error("expected action must be error")
	}
}

// ===================== handleCatalogSearch =====================

func TestHandleCatalogSearch_BasicQuery(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	req := makeToolRequest(map[string]any{"query": "orders"})
	result, err := s.handleCatalogSearch(context.Background(), req, "test-agent", map[string]any{"query": "orders"}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	var resp map[string]any
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if _, ok := resp["results"]; !ok {
		t.Error("expected 'results' in response")
	}
	if _, ok := resp["total"]; !ok {
		t.Error("expected 'total' in response")
	}
}

func TestHandleCatalogSearch_EmptyResults(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	req := makeToolRequest(map[string]any{"query": "nonexistent-topic-xyz"})
	result, err := s.handleCatalogSearch(context.Background(), req, "test-agent", map[string]any{"query": "nonexistent-topic-xyz"}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== handleCatalogTag =====================

func TestHandleCatalogTag_IndexAndTag(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	// First index an entry
	result, err := s.handleCatalogTag(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"action":      "index",
		"name":        "orders-topic",
		"type":        "topic",
		"description": "Order events",
		"owner":       "team-a",
	}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)

	// Parse the response to get entry ID
	text := resultText(t, result)
	var entry map[string]any
	if err := json.Unmarshal([]byte(text), &entry); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	entryID, _ := entry["id"].(string)
	if entryID == "" {
		t.Fatal("expected non-empty entry ID")
	}

	// Now tag it
	result, err = s.handleCatalogTag(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"action":   "tag",
		"entry_id": entryID,
		"tags":     []any{"important", "production"},
	}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleCatalogTag_IndexMissingFields(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	result, err := s.handleCatalogTag(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"action": "index",
		"name":   "",
		"type":   "",
	}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "name and type are required") {
		t.Error("expected 'name and type are required' error")
	}
}

func TestHandleCatalogTag_SetLineage(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	// Index first
	result, _ := s.handleCatalogTag(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"action": "index",
		"name":   "lineage-test",
		"type":   "topic",
	}, timeNow())
	text := resultText(t, result)
	var entry map[string]any
	json.Unmarshal([]byte(text), &entry)
	entryID, _ := entry["id"].(string)

	// Set lineage
	result, err := s.handleCatalogTag(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"action":   "lineage",
		"entry_id": entryID,
		"lineage": map[string]any{
			"upstream":   []any{"source-topic"},
			"downstream": []any{"sink-topic"},
		},
	}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	if !strings.Contains(resultText(t, result), "lineage_set") {
		t.Error("expected 'lineage_set' status")
	}
}

func TestHandleCatalogTag_DeleteEntry(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	// Index first
	result, _ := s.handleCatalogTag(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"action": "index",
		"name":   "delete-me",
		"type":   "topic",
	}, timeNow())
	text := resultText(t, result)
	var entry map[string]any
	json.Unmarshal([]byte(text), &entry)
	entryID, _ := entry["id"].(string)

	// Delete
	result, err := s.handleCatalogTag(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"action":   "delete",
		"entry_id": entryID,
	}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	if !strings.Contains(resultText(t, result), "deleted") {
		t.Error("expected 'deleted' status")
	}
}

func TestHandleCatalogTag_UnknownAction(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	result, err := s.handleCatalogTag(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"action": "unknown",
	}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "action must be") {
		t.Error("expected action must be error")
	}
}

func TestHandleCatalogTag_MissingEntryID(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	for _, action := range []string{"tag", "lineage", "delete"} {
		t.Run(action, func(t *testing.T) {
			result, err := s.handleCatalogTag(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
				"action": action,
			}, timeNow())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertIsError(t, result)
			if !strings.Contains(resultText(t, result), "entry_id is required") {
				t.Errorf("expected 'entry_id is required' for action %s", action)
			}
		})
	}
}

// ===================== handleCatalogStats =====================

func TestHandleCatalogStats_Empty(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	result, err := s.handleCatalogStats(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	var stats map[string]any
	if err := json.Unmarshal([]byte(text), &stats); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if _, ok := stats["total_entries"]; !ok {
		t.Error("expected 'total_entries' in stats")
	}
}

func TestHandleCatalogStats_WithEntries(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})

	// Index some entries first
	s.handleCatalogTag(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"action": "index", "name": "topic1", "type": "topic",
	}, timeNow())
	s.handleCatalogTag(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{
		"action": "index", "name": "schema1", "type": "schema",
	}, timeNow())

	result, err := s.handleCatalogStats(context.Background(), mcp.CallToolRequest{}, "test-agent", map[string]any{}, timeNow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}
