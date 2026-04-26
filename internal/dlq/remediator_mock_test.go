package dlq

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// --- analyzeForRemediation tests ---

func TestAnalyzeForRemediation_SerializationError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*kafka.AnalyzeDLQResult, error) {
			return &kafka.AnalyzeDLQResult{
				Topic:         topic,
				TotalMessages: 5,
				ErrorTypes:    map[string]int{"serialization": 5},
			}, nil
		},
	}

	r := NewRemediator(discardLogger())
	result, err := r.Remediate(context.Background(), mock, "cluster", "orders-dlq", "analyze", "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Action != "analyze" {
		t.Errorf("expected action 'analyze', got %q", result.Action)
	}
	if result.ProcessedCount != 5 {
		t.Errorf("expected ProcessedCount 5, got %d", result.ProcessedCount)
	}
	if len(result.Suggestions) != 1 {
		t.Fatalf("expected 1 suggestion, got %d", len(result.Suggestions))
	}
	if !strings.Contains(result.Suggestions[0], "schema evolution") {
		t.Errorf("expected schema evolution suggestion, got %q", result.Suggestions[0])
	}
	if mock.CallCount("AnalyzeDLQ") != 1 {
		t.Errorf("expected AnalyzeDLQ called once, got %d", mock.CallCount("AnalyzeDLQ"))
	}
}

func TestAnalyzeForRemediation_TimeoutError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*kafka.AnalyzeDLQResult, error) {
			return &kafka.AnalyzeDLQResult{
				Topic:         topic,
				TotalMessages: 3,
				ErrorTypes:    map[string]int{"timeout": 3},
			}, nil
		},
	}

	r := NewRemediator(discardLogger())
	result, err := r.Remediate(context.Background(), mock, "cluster", "orders-dlq", "analyze", "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result.Suggestions[0], "retry with backoff") {
		t.Errorf("expected retry with backoff suggestion, got %q", result.Suggestions[0])
	}
}

func TestAnalyzeForRemediation_ValidationError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*kafka.AnalyzeDLQResult, error) {
			return &kafka.AnalyzeDLQResult{
				Topic:         topic,
				TotalMessages: 2,
				ErrorTypes:    map[string]int{"validation": 2},
			}, nil
		},
	}

	r := NewRemediator(discardLogger())
	result, err := r.Remediate(context.Background(), mock, "cluster", "orders-dlq", "analyze", "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result.Suggestions[0], "correction topic") {
		t.Errorf("expected route to correction suggestion, got %q", result.Suggestions[0])
	}
}

func TestAnalyzeForRemediation_UnknownError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*kafka.AnalyzeDLQResult, error) {
			return &kafka.AnalyzeDLQResult{
				Topic:         topic,
				TotalMessages: 1,
				ErrorTypes:    map[string]int{"something_weird": 1},
			}, nil
		},
	}

	r := NewRemediator(discardLogger())
	result, err := r.Remediate(context.Background(), mock, "cluster", "orders-dlq", "analyze", "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result.Suggestions[0], "review samples") {
		t.Errorf("expected review samples suggestion, got %q", result.Suggestions[0])
	}
}

func TestAnalyzeForRemediation_AnalyzeDLQError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*kafka.AnalyzeDLQResult, error) {
			return nil, errors.New("connection refused")
		},
	}

	r := NewRemediator(discardLogger())
	_, err := r.Remediate(context.Background(), mock, "cluster", "orders-dlq", "analyze", "", 10)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "analyzing DLQ") {
		t.Errorf("expected wrapped error, got %q", err.Error())
	}
}

// --- retryMessages tests ---

func TestRetryMessages_Success(t *testing.T) {
	var producedTopic string
	var producedMsgs []kafka.ProduceMessage

	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{
				{Partition: 0, Offset: 10, Key: "key1", Value: "value1", Headers: map[string]string{"orig": "h1"}},
				{Partition: 0, Offset: 11, Key: "key2", Value: "value2"},
			}, nil
		},
		ProduceFn: func(ctx context.Context, cluster, topic string, messages []kafka.ProduceMessage, opts kafka.ProduceOptions) ([]kafka.DeliveryResult, error) {
			producedTopic = topic
			producedMsgs = append(producedMsgs, messages...)
			return []kafka.DeliveryResult{{Partition: 0, Offset: 100, Status: "success"}}, nil
		},
	}

	r := NewRemediator(discardLogger())
	result, err := r.Remediate(context.Background(), mock, "cluster", "orders-dlq", "retry", "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Action != "retry" {
		t.Errorf("expected action 'retry', got %q", result.Action)
	}
	if result.ProcessedCount != 2 {
		t.Errorf("expected ProcessedCount 2, got %d", result.ProcessedCount)
	}
	if result.SuccessCount != 2 {
		t.Errorf("expected SuccessCount 2, got %d", result.SuccessCount)
	}
	if result.FailureCount != 0 {
		t.Errorf("expected FailureCount 0, got %d", result.FailureCount)
	}
	// Verify messages produced to original topic (strip -dlq suffix)
	if producedTopic != "orders" {
		t.Errorf("expected produce to 'orders', got %q", producedTopic)
	}
	// Verify retry headers
	if len(producedMsgs) != 2 {
		t.Fatalf("expected 2 produced messages, got %d", len(producedMsgs))
	}
	for i, msg := range producedMsgs {
		if _, ok := msg.Headers["x-dlq-retry-at"]; !ok {
			t.Errorf("msg[%d]: missing x-dlq-retry-at header", i)
		}
		if msg.Headers["x-dlq-source"] != "orders-dlq" {
			t.Errorf("msg[%d]: expected x-dlq-source 'orders-dlq', got %q", i, msg.Headers["x-dlq-source"])
		}
	}
	// Verify original headers preserved on first message
	if producedMsgs[0].Headers["orig"] != "h1" {
		t.Errorf("expected original header preserved, got %v", producedMsgs[0].Headers)
	}
	// Verify details
	if len(result.Details) != 2 {
		t.Fatalf("expected 2 details, got %d", len(result.Details))
	}
	if result.Details[0].Status != "retried" {
		t.Errorf("expected status 'retried', got %q", result.Details[0].Status)
	}
}

func TestRetryMessages_ProduceFailure(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{
				{Partition: 0, Offset: 10, Key: "key1", Value: "value1"},
			}, nil
		},
		ProduceFn: func(ctx context.Context, cluster, topic string, messages []kafka.ProduceMessage, opts kafka.ProduceOptions) ([]kafka.DeliveryResult, error) {
			return nil, errors.New("broker unavailable")
		},
	}

	r := NewRemediator(discardLogger())
	result, err := r.Remediate(context.Background(), mock, "cluster", "orders-dlq", "retry", "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.SuccessCount != 0 {
		t.Errorf("expected SuccessCount 0, got %d", result.SuccessCount)
	}
	if result.FailureCount != 1 {
		t.Errorf("expected FailureCount 1, got %d", result.FailureCount)
	}
	if result.Details[0].Status != "failed" {
		t.Errorf("expected status 'failed', got %q", result.Details[0].Status)
	}
	if result.Details[0].Error != "produce_failed" {
		t.Errorf("expected categorized error 'produce_failed', got %q", result.Details[0].Error)
	}
}

func TestRetryMessages_ConsumeFailure(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return nil, errors.New("consume error")
		},
	}

	r := NewRemediator(discardLogger())
	_, err := r.Remediate(context.Background(), mock, "cluster", "orders-dlq", "retry", "", 10)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "consuming DLQ messages") {
		t.Errorf("expected wrapped error, got %q", err.Error())
	}
}

// --- routeMessages tests ---

func TestRouteMessages_Success(t *testing.T) {
	var producedTopic string
	var producedMsgs []kafka.ProduceMessage

	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{
				{Partition: 1, Offset: 20, Key: "k", Value: "v", Headers: map[string]string{"existing": "yes"}},
			}, nil
		},
		ProduceFn: func(ctx context.Context, cluster, topic string, messages []kafka.ProduceMessage, opts kafka.ProduceOptions) ([]kafka.DeliveryResult, error) {
			producedTopic = topic
			producedMsgs = append(producedMsgs, messages...)
			return []kafka.DeliveryResult{{Partition: 0, Offset: 50, Status: "success"}}, nil
		},
	}

	r := NewRemediator(discardLogger())
	result, err := r.Remediate(context.Background(), mock, "cluster", "orders-dlq", "route", "orders-corrected", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Action != "route" {
		t.Errorf("expected action 'route', got %q", result.Action)
	}
	if producedTopic != "orders-corrected" {
		t.Errorf("expected produce to 'orders-corrected', got %q", producedTopic)
	}
	if result.SuccessCount != 1 {
		t.Errorf("expected SuccessCount 1, got %d", result.SuccessCount)
	}
	// Verify routing headers
	if len(producedMsgs) != 1 {
		t.Fatalf("expected 1 produced message, got %d", len(producedMsgs))
	}
	msg := producedMsgs[0]
	if msg.Headers["x-dlq-routed-from"] != "orders-dlq" {
		t.Errorf("expected x-dlq-routed-from 'orders-dlq', got %q", msg.Headers["x-dlq-routed-from"])
	}
	if _, ok := msg.Headers["x-dlq-routed-at"]; !ok {
		t.Error("missing x-dlq-routed-at header")
	}
	if msg.Headers["existing"] != "yes" {
		t.Error("expected existing header preserved")
	}
	if result.Details[0].Status != "routed" {
		t.Errorf("expected status 'routed', got %q", result.Details[0].Status)
	}
}

func TestRouteMessages_ProduceFailure(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{
				{Partition: 0, Offset: 5, Value: "v"},
			}, nil
		},
		ProduceFn: func(ctx context.Context, cluster, topic string, messages []kafka.ProduceMessage, opts kafka.ProduceOptions) ([]kafka.DeliveryResult, error) {
			return nil, errors.New("target topic not found")
		},
	}

	r := NewRemediator(discardLogger())
	result, err := r.Remediate(context.Background(), mock, "cluster", "orders-dlq", "route", "orders-corrected", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.FailureCount != 1 {
		t.Errorf("expected FailureCount 1, got %d", result.FailureCount)
	}
	if result.Details[0].Status != "failed" {
		t.Errorf("expected status 'failed', got %q", result.Details[0].Status)
	}
}

func TestRouteMessages_MissingTargetTopic(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	r := NewRemediator(discardLogger())
	_, err := r.Remediate(context.Background(), mock, "cluster", "orders-dlq", "route", "", 10)
	if err == nil {
		t.Fatal("expected error for missing target_topic")
	}
	if !strings.Contains(err.Error(), "target_topic is required") {
		t.Errorf("unexpected error: %v", err)
	}
}

// --- suggestFixes tests ---

func TestSuggestFixes_SchemaError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*kafka.AnalyzeDLQResult, error) {
			return &kafka.AnalyzeDLQResult{
				Topic:         topic,
				TotalMessages: 4,
				ErrorTypes:    map[string]int{"schema": 4},
			}, nil
		},
	}

	r := NewRemediator(discardLogger())
	result, err := r.Remediate(context.Background(), mock, "cluster", "orders-dlq", "suggest", "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Action != "suggest" {
		t.Errorf("expected action 'suggest', got %q", result.Action)
	}
	found := false
	for _, s := range result.Suggestions {
		if strings.Contains(s, "schema compatibility") || strings.Contains(s, "Schema mismatch") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected schema compatibility suggestion, got %v", result.Suggestions)
	}
}

func TestSuggestFixes_SerializationError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*kafka.AnalyzeDLQResult, error) {
			return &kafka.AnalyzeDLQResult{
				Topic:         topic,
				TotalMessages: 2,
				ErrorTypes:    map[string]int{"serialization": 2},
			}, nil
		},
	}

	r := NewRemediator(discardLogger())
	result, err := r.Remediate(context.Background(), mock, "cluster", "orders-dlq", "suggest", "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	found := false
	for _, s := range result.Suggestions {
		if strings.Contains(s, "Schema mismatch") || strings.Contains(s, "schema") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected schema-related suggestion for serialization, got %v", result.Suggestions)
	}
}

func TestSuggestFixes_TimeoutError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*kafka.AnalyzeDLQResult, error) {
			return &kafka.AnalyzeDLQResult{
				Topic:         topic,
				TotalMessages: 3,
				ErrorTypes:    map[string]int{"timeout": 3},
			}, nil
		},
	}

	r := NewRemediator(discardLogger())
	result, err := r.Remediate(context.Background(), mock, "cluster", "orders-dlq", "suggest", "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	found := false
	for _, s := range result.Suggestions {
		if strings.Contains(s, "retry") || strings.Contains(s, "backoff") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected retry suggestion for timeout, got %v", result.Suggestions)
	}
}

func TestSuggestFixes_ValidationError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*kafka.AnalyzeDLQResult, error) {
			return &kafka.AnalyzeDLQResult{
				Topic:         topic,
				TotalMessages: 1,
				ErrorTypes:    map[string]int{"validation": 1},
			}, nil
		},
	}

	r := NewRemediator(discardLogger())
	result, err := r.Remediate(context.Background(), mock, "cluster", "orders-dlq", "suggest", "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	found := false
	for _, s := range result.Suggestions {
		if strings.Contains(s, "correction topic") || strings.Contains(s, "Route") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected route to correction suggestion for validation, got %v", result.Suggestions)
	}
}

func TestSuggestFixes_AuthPermissionError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*kafka.AnalyzeDLQResult, error) {
			return &kafka.AnalyzeDLQResult{
				Topic:         topic,
				TotalMessages: 2,
				ErrorTypes:    map[string]int{"auth": 1, "permission": 1},
			}, nil
		},
	}

	r := NewRemediator(discardLogger())
	result, err := r.Remediate(context.Background(), mock, "cluster", "orders-dlq", "suggest", "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	foundNoRetry := false
	for _, s := range result.Suggestions {
		if strings.Contains(s, "Do NOT retry") {
			foundNoRetry = true
			break
		}
	}
	if !foundNoRetry {
		t.Errorf("expected 'Do NOT retry' suggestion for auth/permission, got %v", result.Suggestions)
	}
}

func TestSuggestFixes_IncludesOriginalTopic(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*kafka.AnalyzeDLQResult, error) {
			return &kafka.AnalyzeDLQResult{
				Topic:         topic,
				TotalMessages: 1,
				ErrorTypes:    map[string]int{},
			}, nil
		},
	}

	r := NewRemediator(discardLogger())
	result, err := r.Remediate(context.Background(), mock, "cluster", "orders-dlq", "suggest", "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	found := false
	for _, s := range result.Suggestions {
		if strings.Contains(s, `"orders"`) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected original topic 'orders' in suggestions, got %v", result.Suggestions)
	}
}

// --- Remediate dispatch tests ---

func TestRemediate_UnknownAction(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	r := NewRemediator(discardLogger())
	_, err := r.Remediate(context.Background(), mock, "cluster", "orders-dlq", "invalid", "", 10)
	if err == nil {
		t.Fatal("expected error for unknown action")
	}
	if !strings.Contains(err.Error(), "unknown remediation action") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRemediate_DefaultLimit(t *testing.T) {
	var capturedLimit int
	mock := &kafka.MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*kafka.AnalyzeDLQResult, error) {
			capturedLimit = limit
			return &kafka.AnalyzeDLQResult{
				Topic:         topic,
				TotalMessages: 0,
				ErrorTypes:    map[string]int{},
			}, nil
		},
	}

	r := NewRemediator(discardLogger())
	_, err := r.Remediate(context.Background(), mock, "cluster", "orders-dlq", "analyze", "", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedLimit != 100 {
		t.Errorf("expected default limit 100, got %d", capturedLimit)
	}
}
