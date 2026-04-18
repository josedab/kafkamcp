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
	tr := &mockTransport{}
	c := New(tr)
	if c == nil {
		t.Fatal("expected non-nil client")
	}
	if c.agentID != "sdk-agent" {
		t.Errorf("expected default agent ID 'sdk-agent', got %q", c.agentID)
	}
	if c.timeout != 30*time.Second {
		t.Errorf("expected default timeout 30s, got %v", c.timeout)
	}
}

func TestNewWithOptions(t *testing.T) {
	tr := &mockTransport{}
	c := New(tr, WithAgentID("my-agent"), WithTimeout(10*time.Second))
	if c.agentID != "my-agent" {
		t.Errorf("expected agent ID 'my-agent', got %q", c.agentID)
	}
	if c.timeout != 10*time.Second {
		t.Errorf("expected timeout 10s, got %v", c.timeout)
	}
}

func TestListTopics(t *testing.T) {
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
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			if name != "kafka_consume" {
				t.Errorf("expected tool name 'kafka_consume', got %q", name)
			}
			if args["topic"] != "orders" {
				t.Errorf("expected topic 'orders', got %v", args["topic"])
			}
			return json.Marshal([]Message{
				{Partition: 0, Offset: 100, Timestamp: "2025-01-01T00:00:00Z", Key: "key1", Value: map[string]any{"id": 1}},
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
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			if args["limit"].(float64) != 10 {
				t.Errorf("expected default limit 10, got %v", args["limit"])
			}
			if args["offset"] != "latest" {
				t.Errorf("expected default offset 'latest', got %v", args["offset"])
			}
			return json.Marshal([]Message{})
		},
	}
	c := New(tr)
	c.Consume(context.Background(), "test", 0, "")
}

func TestConsumeError(t *testing.T) {
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
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			if name != "kafka_produce" {
				t.Errorf("expected tool 'kafka_produce', got %q", name)
			}
			return json.Marshal(map[string]any{
				"results": []DeliveryResult{
					{Partition: 0, Offset: 42, Timestamp: "2025-01-01T00:00:00Z", Status: "success"},
				},
			})
		},
	}
	c := New(tr)
	results, err := c.Produce(context.Background(), "orders", []map[string]any{
		{"key": "k1", "value": map[string]any{"id": 1}},
	})
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
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			return nil, errors.New("auth failed")
		},
	}
	c := New(tr)
	_, err := c.Produce(context.Background(), "test", nil)
	if err == nil {
		t.Error("expected error")
	}
}

func TestSearch(t *testing.T) {
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			return json.Marshal(map[string]any{
				"matches": []Message{
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

func TestWatch(t *testing.T) {
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			return json.Marshal(map[string]any{
				"subscription_id": "sub-123",
			})
		},
	}
	c := New(tr)
	subID, err := c.Watch(context.Background(), "orders.*", "", "https://hooks.example.com/kafka")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if subID != "sub-123" {
		t.Errorf("expected sub ID 'sub-123', got %q", subID)
	}
}

func TestWatchError(t *testing.T) {
	tr := &mockTransport{
		callToolFn: func(ctx context.Context, name string, args map[string]any) (json.RawMessage, error) {
			return nil, errors.New("failed")
		},
	}
	c := New(tr)
	_, err := c.Watch(context.Background(), "test", "", "")
	if err == nil {
		t.Error("expected error")
	}
}

func TestClose(t *testing.T) {
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

func TestTopicSummaryJSON(t *testing.T) {
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
