package kafkamcpsdk

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type mockTransport struct {
	callToolFn     func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error)
	readResourceFn func(ctx context.Context, uri string) (json.RawMessage, error)
	closeFn        func() error
}

func (m *mockTransport) CallTool(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
	if m.callToolFn != nil {
		return m.callToolFn(ctx, name, args)
	}
	return nil, errors.New("not implemented")
}

func (m *mockTransport) ReadResource(ctx context.Context, uri string) (json.RawMessage, error) {
	if m.readResourceFn != nil {
		return m.readResourceFn(ctx, uri)
	}
	return nil, errors.New("not implemented")
}

func (m *mockTransport) Close() error {
	if m.closeFn != nil {
		return m.closeFn()
	}
	return nil
}

func TestNew(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{}
	c := New(tr)
	if c == nil {
		t.Fatal("expected non-nil client")
	}
	if c.timeout != 30*time.Second {
		t.Errorf("expected default timeout 30s, got %v", c.timeout)
	}
}

func TestNewWithOptions(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{}
	c := New(tr, WithTimeout(10*time.Second))
	if c.timeout != 10*time.Second {
		t.Errorf("expected timeout 10s, got %v", c.timeout)
	}
}

func TestListTopics(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			if name != "kafka_list_topics" {
				t.Errorf("expected tool name 'kafka_list_topics', got %q", name)
			}
			return json.Marshal(map[string]any{
				"topics": []TopicSummary{
					{Name: "orders", Partitions: 3, ReplicationFactor: 2, MessageCount: 1000},
					{Name: "payments", Partitions: 6, ReplicationFactor: 3, MessageCount: 500},
				},
			})
		},
	}
	c := New(tr)
	topics, err := c.ListTopics(context.Background(), "*")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(topics) != 2 {
		t.Errorf("expected 2 topics, got %d", len(topics))
	}
	if topics[0].Name != "orders" {
		t.Errorf("expected first topic 'orders', got %q", topics[0].Name)
	}
}

func TestListTopicsError(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			return nil, errors.New("connection refused")
		},
	}
	c := New(tr)
	_, err := c.ListTopics(context.Background(), "")
	if err == nil {
		t.Error("expected error")
	}
}

func TestListTopicsInvalidJSON(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			return json.RawMessage(`invalid json`), nil
		},
	}
	c := New(tr)
	_, err := c.ListTopics(context.Background(), "")
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestConsume(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			if name != "kafka_consume" {
				t.Errorf("expected tool name 'kafka_consume', got %q", name)
			}
			if args["topic"] != "orders" {
				t.Errorf("expected topic 'orders', got %v", args["topic"])
			}
			return json.Marshal(map[string]any{
				"messages": []Message{
					{Partition: 0, Offset: 100, Timestamp: "2025-01-01T00:00:00Z", Key: "key1", Value: map[string]any{"id": 1}},
				},
			})
		},
	}
	c := New(tr)
	msgs, err := c.Consume(context.Background(), "orders", 5, "latest")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 1 {
		t.Errorf("expected 1 message, got %d", len(msgs))
	}
}

func TestConsumeDefaults(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			if args["limit"].(float64) != 10 {
				t.Errorf("expected default limit 10, got %v", args["limit"])
			}
			if args["offset"] != "latest" {
				t.Errorf("expected default offset 'latest', got %v", args["offset"])
			}
			return json.Marshal(map[string]any{"messages": []Message{}})
		},
	}
	c := New(tr)
	_, _ = c.Consume(context.Background(), "test", 0, "")
}

func TestConsumeError(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			return nil, errors.New("timeout")
		},
	}
	c := New(tr)
	_, err := c.Consume(context.Background(), "test", 10, "latest")
	if err == nil {
		t.Error("expected error")
	}
}

func TestProduce(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			if name != "kafka_produce" {
				t.Errorf("expected tool 'kafka_produce', got %q", name)
			}
			if args["approval_token"] != "approval-token" {
				t.Errorf("missing approval token: %v", args)
			}
			return json.Marshal(map[string]any{
				"deliveries": []DeliveryResult{
					{Partition: 0, Offset: 42, Timestamp: "2025-01-01T00:00:00Z", Status: "success"},
				},
			})
		},
	}
	c := New(tr)
	results, err := c.Produce(context.Background(), "orders", []map[string]any{
		{"key": "k1", "value": map[string]any{"id": 1}},
	}, MutationOptions{ApprovalToken: "approval-token"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
	if results[0].Status != "success" {
		t.Errorf("expected status 'success', got %q", results[0].Status)
	}
}

func TestProduceError(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			return nil, errors.New("auth failed")
		},
	}
	c := New(tr)
	_, err := c.Produce(context.Background(), "test", nil, MutationOptions{ApprovalToken: "approval-token"})
	if err == nil {
		t.Error("expected error")
	}
}

func TestGuardedMutationRequiresApproval(t *testing.T) {
	t.Parallel()
	called := false
	client := New(&mockTransport{
		callToolFn: func(context.Context, string, map[string]any) (json.RawMessage, error) {
			called = true
			return nil, nil
		},
	})
	if _, err := client.Produce(context.Background(), "orders", nil, MutationOptions{}); err == nil {
		t.Fatal("expected missing approval error")
	}
	if called {
		t.Fatal("transport must not be called without approval")
	}
}

func TestSearch(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			return json.Marshal(map[string]any{
				"messages": []Message{
					{Partition: 0, Offset: 50, Key: "k1"},
				},
			})
		},
	}
	c := New(tr)
	msgs, err := c.Search(context.Background(), "orders", "error", 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 1 {
		t.Errorf("expected 1 match, got %d", len(msgs))
	}
}

func TestSearchError(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			return nil, errors.New("failed")
		},
	}
	c := New(tr)
	_, err := c.Search(context.Background(), "test", "", 10)
	if err == nil {
		t.Error("expected error")
	}
}

func TestClose(t *testing.T) {
	t.Parallel()
	closed := false
	tr := &mockTransport{
		closeFn: func() error {
			closed = true
			return nil
		},
	}
	c := New(tr)
	err := c.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !closed {
		t.Error("expected transport Close to be called")
	}
}

func TestCloseError(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		closeFn: func() error {
			return errors.New("close failed")
		},
	}
	c := New(tr)
	err := c.Close()
	if err == nil {
		t.Error("expected error")
	}
}

// --- Consumer Group Tests ---

func TestListConsumerGroups(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			if name != "kafka_list_consumer_groups" {
				t.Errorf("expected tool kafka_list_consumer_groups, got %q", name)
			}
			return json.Marshal(map[string]any{
				"groups": []ConsumerGroupSummary{
					{GroupID: "group-1", State: "Stable", MemberCount: 3, TotalLag: 100},
				},
			})
		},
	}
	c := New(tr)
	groups, err := c.ListConsumerGroups(context.Background(), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(groups) != 1 {
		t.Errorf("expected 1 group, got %d", len(groups))
	}
	if groups[0].GroupID != "group-1" {
		t.Errorf("expected group-1, got %s", groups[0].GroupID)
	}
}

func TestListConsumerGroupsWithCluster(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			if args["cluster"] != "prod" {
				t.Errorf("expected cluster prod, got %v", args["cluster"])
			}
			return json.Marshal(map[string]any{"groups": []ConsumerGroupSummary{}})
		},
	}
	c := New(tr)
	_, _ = c.ListConsumerGroups(context.Background(), "prod")
}

func TestListConsumerGroupsError(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			return nil, errors.New("timeout")
		},
	}
	c := New(tr)
	_, err := c.ListConsumerGroups(context.Background(), "")
	if err == nil {
		t.Error("expected error")
	}
}

func TestListConsumerGroupsInvalidJSON(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			return json.RawMessage(`{invalid`), nil
		},
	}
	c := New(tr)
	_, err := c.ListConsumerGroups(context.Background(), "")
	if err == nil {
		t.Error("expected error for malformed JSON")
	}
}

func TestDescribeConsumerGroup(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			if name != "kafka_describe_consumer_group" {
				t.Errorf("expected kafka_describe_consumer_group, got %q", name)
			}
			if args["group_id"] != "my-group" {
				t.Errorf("expected group_id my-group, got %v", args["group_id"])
			}
			return json.Marshal(map[string]any{"group_id": "my-group", "state": "Stable"})
		},
	}
	c := New(tr)
	result, err := c.DescribeConsumerGroup(context.Background(), "my-group")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestDescribeConsumerGroupError(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			return nil, errors.New("not found")
		},
	}
	c := New(tr)
	_, err := c.DescribeConsumerGroup(context.Background(), "missing")
	if err == nil {
		t.Error("expected error")
	}
}

// --- Topic Management Tests ---

func TestDescribeTopic(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			if name != "kafka_describe_topic" {
				t.Errorf("expected kafka_describe_topic, got %q", name)
			}
			return json.Marshal(map[string]any{"name": "orders", "partitions": 3})
		},
	}
	c := New(tr)
	result, err := c.DescribeTopic(context.Background(), "orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestDescribeTopicError(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			return nil, errors.New("failed")
		},
	}
	c := New(tr)
	_, err := c.DescribeTopic(context.Background(), "orders")
	if err == nil {
		t.Error("expected error")
	}
}

func TestCreateTopic(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			if name != "kafka_create_topic" {
				t.Errorf("expected kafka_create_topic, got %q", name)
			}
			if args["name"] != "new-topic" {
				t.Errorf("expected name new-topic, got %v", args["name"])
			}
			if args["partitions"].(float64) != 3 {
				t.Errorf("expected 3 partitions, got %v", args["partitions"])
			}
			return json.Marshal(map[string]any{"status": "created"})
		},
	}
	c := New(tr)
	err := c.CreateTopic(context.Background(), "new-topic", 3, 2, nil, MutationOptions{
		ApprovalToken: "approval-token", IdempotencyKey: "request-123",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCreateTopicWithConfigs(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			if args["configs"] == nil {
				t.Error("expected configs to be present")
			}
			return json.Marshal(map[string]any{})
		},
	}
	c := New(tr)
	err := c.CreateTopic(context.Background(), "t", 1, 1, map[string]string{"retention.ms": "86400000"}, MutationOptions{ApprovalToken: "approval-token"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCreateTopicError(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			return nil, errors.New("failed")
		},
	}
	c := New(tr)
	err := c.CreateTopic(context.Background(), "t", 1, 1, nil, MutationOptions{ApprovalToken: "approval-token"})
	if err == nil {
		t.Error("expected error")
	}
}

func TestDeleteTopic(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			if name != "kafka_delete_topic" {
				t.Errorf("expected kafka_delete_topic, got %q", name)
			}
			return json.Marshal(map[string]any{})
		},
	}
	c := New(tr)
	err := c.DeleteTopic(context.Background(), "old-topic", MutationOptions{ApprovalToken: "approval-token"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeleteTopicError(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			return nil, errors.New("permission denied")
		},
	}
	c := New(tr)
	err := c.DeleteTopic(context.Background(), "protected", MutationOptions{ApprovalToken: "approval-token"})
	if err == nil {
		t.Error("expected error")
	}
}

// --- Schema Registry Tests ---

func TestListSchemas(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			if name != "kafka_list_schemas" {
				t.Errorf("expected kafka_list_schemas, got %q", name)
			}
			return json.Marshal(map[string]any{
				"subjects": []map[string]any{
					{
						"subject_name":        "orders-value",
						"latest_version":      1,
						"schema_type":         "AVRO",
						"compatibility_level": "BACKWARD",
					},
					{
						"subject_name":   "payments-value",
						"latest_version": 2,
						"schema_type":    "PROTOBUF",
					},
				},
			})
		},
	}
	c := New(tr)
	subjects, err := c.ListSchemas(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(subjects) != 2 {
		t.Errorf("expected 2 subjects, got %d", len(subjects))
	}
	if subjects[0].Subject != "orders-value" || subjects[0].Version != 1 {
		t.Errorf("unexpected first subject: %+v", subjects[0])
	}
	if subjects[0].CompatibilityLevel != "BACKWARD" {
		t.Errorf("expected BACKWARD compatibility, got %q", subjects[0].CompatibilityLevel)
	}
}

func TestListSchemasError(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			return nil, errors.New("registry unavailable")
		},
	}
	c := New(tr)
	_, err := c.ListSchemas(context.Background())
	if err == nil {
		t.Error("expected error")
	}
}

func TestListSchemasInvalidJSON(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			return json.RawMessage(`not json`), nil
		},
	}
	c := New(tr)
	_, err := c.ListSchemas(context.Background())
	if err == nil {
		t.Error("expected error for malformed JSON")
	}
}

func TestGetSchema(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			if name != "kafka_get_schema" {
				t.Errorf("expected kafka_get_schema, got %q", name)
			}
			return json.Marshal(map[string]any{
				"subject":       "orders-value",
				"version":       1,
				"schema_id":     42,
				"schema_type":   "AVRO",
				"schema":        map[string]any{"type": "record", "name": "Order"},
				"field_summary": "Order { id: string }",
				"references":    []any{},
			})
		},
	}
	c := New(tr)
	info, err := c.GetSchema(context.Background(), "orders-value")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.Subject != "orders-value" {
		t.Errorf("expected orders-value, got %s", info.Subject)
	}
	schemaObject, ok := info.Schema.(map[string]any)
	if !ok || schemaObject["name"] != "Order" {
		t.Errorf("expected parsed schema object, got %#v", info.Schema)
	}
}

func TestGetSchemaError(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			return nil, errors.New("not found")
		},
	}
	c := New(tr)
	_, err := c.GetSchema(context.Background(), "missing")
	if err == nil {
		t.Error("expected error")
	}
}

func TestGetSchemaInvalidJSON(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			return json.RawMessage(`bad`), nil
		},
	}
	c := New(tr)
	_, err := c.GetSchema(context.Background(), "test")
	if err == nil {
		t.Error("expected error for malformed JSON")
	}
}

// --- Admin Operations Tests ---

func TestResetOffsets(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			if name != "kafka_reset_offsets" {
				t.Errorf("expected kafka_reset_offsets, got %q", name)
			}
			if args["group_id"] != "my-group" {
				t.Errorf("expected group_id my-group, got %v", args["group_id"])
			}
			return json.Marshal(map[string]any{})
		},
	}
	c := New(tr)
	err := c.ResetOffsets(context.Background(), "my-group", "orders", "earliest", ResetOffsetsOptions{
		MutationOptions: MutationOptions{ApprovalToken: "approval-token"},
		DryRun:          true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestResetOffsetsError(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			return nil, errors.New("failed")
		},
	}
	c := New(tr)
	err := c.ResetOffsets(context.Background(), "g", "t", "earliest", ResetOffsetsOptions{
		MutationOptions: MutationOptions{ApprovalToken: "approval-token"},
	})
	if err == nil {
		t.Error("expected error")
	}
}

func TestResetOffsetsTimestamp(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			if args["value"] != "2026-07-27T00:00:00Z" {
				t.Errorf("expected timestamp value, got %v", args["value"])
			}
			if args["dry_run"] != true {
				t.Errorf("expected dry_run true, got %v", args["dry_run"])
			}
			return json.Marshal(map[string]any{})
		},
	}
	c := New(tr)

	err := c.ResetOffsets(context.Background(), "my-group", "orders", "timestamp", ResetOffsetsOptions{
		MutationOptions: MutationOptions{ApprovalToken: "approval-token"},
		Value:           "2026-07-27T00:00:00Z",
		DryRun:          true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestResetOffsetsTimestampRequiresValue(t *testing.T) {
	t.Parallel()
	c := New(&mockTransport{})

	err := c.ResetOffsets(context.Background(), "my-group", "orders", "timestamp", ResetOffsetsOptions{
		MutationOptions: MutationOptions{ApprovalToken: "approval-token"},
	})
	if err == nil {
		t.Fatal("expected timestamp validation error")
	}
}

func TestDiagnoseLag(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			if name != "kafka_diagnose_lag" {
				t.Errorf("expected kafka_diagnose_lag, got %q", name)
			}
			return json.Marshal(map[string]any{"diagnosis": "healthy"})
		},
	}
	c := New(tr)
	result, err := c.DiagnoseLag(context.Background(), "my-group")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestDiagnoseLagError(t *testing.T) {
	t.Parallel()
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			return nil, errors.New("failed")
		},
	}
	c := New(tr)
	_, err := c.DiagnoseLag(context.Background(), "g")
	if err == nil {
		t.Error("expected error")
	}
}

func TestTopicSummaryJSON(t *testing.T) {
	t.Parallel()
	ts := TopicSummary{Name: "test", Partitions: 3, ReplicationFactor: 2, MessageCount: 100}
	data, err := json.Marshal(ts)
	if err != nil {
		t.Fatal(err)
	}
	var decoded TopicSummary
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Name != "test" {
		t.Errorf("expected name 'test', got %q", decoded.Name)
	}
}

func TestDeliveryResultJSON(t *testing.T) {
	t.Parallel()
	dr := DeliveryResult{Partition: 0, Offset: 42, Status: "success"}
	data, err := json.Marshal(dr)
	if err != nil {
		t.Fatal(err)
	}
	var decoded DeliveryResult
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Status != "success" {
		t.Errorf("expected status 'success', got %q", decoded.Status)
	}
}
