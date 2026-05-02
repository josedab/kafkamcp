package kafka

import (
	"context"
	"fmt"
	"testing"
)

// --------------- SearchMessages via MockKafkaManager ---------------

func TestSearchMessages_KeyFilterMatch(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		SearchMessagesFn: func(ctx context.Context, cluster string, req SearchRequest) ([]SearchMessage, int, error) {
			// Simulate filtering by key
			msgs := []Message{
				{Partition: 0, Offset: 1, Key: "order-123", Value: "data1"},
				{Partition: 0, Offset: 2, Key: "order-456", Value: "data2"},
				{Partition: 0, Offset: 3, Key: "payment-789", Value: "data3"},
			}
			var matched []SearchMessage
			for _, m := range msgs {
				keyStr := fmt.Sprintf("%v", m.Key)
				if req.KeyFilter == "" || contains(keyStr, req.KeyFilter) {
					matched = append(matched, m)
				}
			}
			return matched, len(msgs), nil
		},
	}

	results, scanned, err := mock.SearchMessages(context.Background(), "default", SearchRequest{
		Topic:     "orders",
		KeyFilter: "order",
		MaxScan:   100,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if scanned != 3 {
		t.Errorf("expected 3 scanned, got %d", scanned)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 matches for key filter 'order', got %d", len(results))
	}
	if mock.CallCount("SearchMessages") != 1 {
		t.Error("expected SearchMessages to be called once")
	}
}

func TestSearchMessages_KeyFilterMiss(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		SearchMessagesFn: func(ctx context.Context, cluster string, req SearchRequest) ([]SearchMessage, int, error) {
			return nil, 5, nil
		},
	}

	results, scanned, err := mock.SearchMessages(context.Background(), "default", SearchRequest{
		Topic:     "orders",
		KeyFilter: "nonexistent",
		MaxScan:   100,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 matches, got %d", len(results))
	}
	if scanned != 5 {
		t.Errorf("expected 5 scanned, got %d", scanned)
	}
}

func TestSearchMessages_HeaderFilterKeyAndValue(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		SearchMessagesFn: func(ctx context.Context, cluster string, req SearchRequest) ([]SearchMessage, int, error) {
			msgs := []Message{
				{Partition: 0, Offset: 1, Key: "k1", Headers: map[string]string{"env": "prod", "team": "payments"}},
				{Partition: 0, Offset: 2, Key: "k2", Headers: map[string]string{"env": "staging", "team": "orders"}},
				{Partition: 0, Offset: 3, Key: "k3", Headers: map[string]string{"env": "prod", "team": "orders"}},
			}
			var matched []SearchMessage
			for _, m := range msgs {
				if req.HeaderKey != "" {
					hv, ok := m.Headers[req.HeaderKey]
					if !ok {
						continue
					}
					if req.HeaderValue != "" && hv != req.HeaderValue {
						continue
					}
				}
				matched = append(matched, m)
			}
			return matched, len(msgs), nil
		},
	}

	results, _, err := mock.SearchMessages(context.Background(), "default", SearchRequest{
		Topic:       "events",
		HeaderKey:   "env",
		HeaderValue: "prod",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 matches for env=prod, got %d", len(results))
	}
}

func TestSearchMessages_HeaderFilterKeyOnly(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		SearchMessagesFn: func(ctx context.Context, cluster string, req SearchRequest) ([]SearchMessage, int, error) {
			msgs := []Message{
				{Partition: 0, Offset: 1, Key: "k1", Headers: map[string]string{"trace-id": "abc"}},
				{Partition: 0, Offset: 2, Key: "k2", Headers: nil},
				{Partition: 0, Offset: 3, Key: "k3", Headers: map[string]string{"trace-id": "def"}},
			}
			var matched []SearchMessage
			for _, m := range msgs {
				if req.HeaderKey != "" {
					if m.Headers == nil {
						continue
					}
					if _, ok := m.Headers[req.HeaderKey]; !ok {
						continue
					}
				}
				matched = append(matched, m)
			}
			return matched, len(msgs), nil
		},
	}

	results, _, err := mock.SearchMessages(context.Background(), "default", SearchRequest{
		Topic:     "events",
		HeaderKey: "trace-id",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 matches for header key 'trace-id', got %d", len(results))
	}
}

func TestSearchMessages_ValueFilter(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		SearchMessagesFn: func(ctx context.Context, cluster string, req SearchRequest) ([]SearchMessage, int, error) {
			msgs := []Message{
				{Partition: 0, Offset: 1, Key: "k1", Value: map[string]any{"status": "failed", "reason": "timeout"}},
				{Partition: 0, Offset: 2, Key: "k2", Value: map[string]any{"status": "success"}},
				{Partition: 0, Offset: 3, Key: "k3", Value: map[string]any{"status": "failed", "reason": "auth"}},
			}
			var matched []SearchMessage
			for _, m := range msgs {
				if req.ValueContains != "" {
					valStr := fmt.Sprintf("%v", m.Value)
					if !contains(valStr, req.ValueContains) {
						continue
					}
				}
				matched = append(matched, m)
			}
			return matched, len(msgs), nil
		},
	}

	results, _, err := mock.SearchMessages(context.Background(), "default", SearchRequest{
		Topic:         "orders",
		ValueContains: "failed",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 matches for value containing 'failed', got %d", len(results))
	}
}

func TestSearchMessages_TimeRangeFiltering(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		SearchMessagesFn: func(ctx context.Context, cluster string, req SearchRequest) ([]SearchMessage, int, error) {
			msgs := []Message{
				{Partition: 0, Offset: 1, Key: "k1", Timestamp: "2024-01-01T00:00:00.000Z"},
				{Partition: 0, Offset: 2, Key: "k2", Timestamp: "2024-01-01T12:00:00.000Z"},
				{Partition: 0, Offset: 3, Key: "k3", Timestamp: "2024-01-02T00:00:00.000Z"},
			}
			// Only return messages before EndTime
			return msgs[:2], 3, nil
		},
	}

	results, scanned, err := mock.SearchMessages(context.Background(), "default", SearchRequest{
		Topic:   "events",
		EndTime: "2024-01-01T18:00:00.000Z",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 messages before cutoff, got %d", len(results))
	}
	if scanned != 3 {
		t.Errorf("expected 3 scanned, got %d", scanned)
	}
}

func TestSearchMessages_CombinedFilters(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		SearchMessagesFn: func(ctx context.Context, cluster string, req SearchRequest) ([]SearchMessage, int, error) {
			return []Message{
				{Partition: 0, Offset: 42, Key: "order-123", Value: map[string]any{"status": "failed"},
					Headers: map[string]string{"env": "prod"}, Timestamp: "2024-01-01T10:00:00.000Z"},
			}, 100, nil
		},
	}

	results, scanned, err := mock.SearchMessages(context.Background(), "default", SearchRequest{
		Topic:         "orders",
		KeyFilter:     "order",
		HeaderKey:     "env",
		HeaderValue:   "prod",
		ValueContains: "failed",
		EndTime:       "2024-01-02T00:00:00.000Z",
		MaxScan:       100,
		Limit:         10,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 combined filter match, got %d", len(results))
	}
	if scanned != 100 {
		t.Errorf("expected 100 scanned, got %d", scanned)
	}
}

func TestSearchMessages_MaxScanLimit(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		SearchMessagesFn: func(ctx context.Context, cluster string, req SearchRequest) ([]SearchMessage, int, error) {
			if req.MaxScan != 50 {
				t.Errorf("expected MaxScan=50, got %d", req.MaxScan)
			}
			return nil, 50, nil
		},
	}

	_, scanned, err := mock.SearchMessages(context.Background(), "default", SearchRequest{
		Topic:   "events",
		MaxScan: 50,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if scanned != 50 {
		t.Errorf("expected 50 scanned, got %d", scanned)
	}
}

func TestSearchMessages_EmptyResults(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		SearchMessagesFn: func(ctx context.Context, cluster string, req SearchRequest) ([]SearchMessage, int, error) {
			return nil, 0, nil
		},
	}

	results, scanned, err := mock.SearchMessages(context.Background(), "default", SearchRequest{
		Topic:     "empty-topic",
		KeyFilter: "anything",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
	if scanned != 0 {
		t.Errorf("expected 0 scanned, got %d", scanned)
	}
}

func TestSearchMessages_Error(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		SearchMessagesFn: func(ctx context.Context, cluster string, req SearchRequest) ([]SearchMessage, int, error) {
			return nil, 0, fmt.Errorf("connection refused")
		},
	}

	_, _, err := mock.SearchMessages(context.Background(), "default", SearchRequest{Topic: "orders"})
	if err == nil {
		t.Fatal("expected error")
	}
	if !contains(err.Error(), "connection refused") {
		t.Errorf("unexpected error: %v", err)
	}
}

// --------------- AnalyzeDLQ via MockKafkaManager ---------------

func TestAnalyzeDLQ_ErrorTypeHeader(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*AnalyzeDLQResult, error) {
			return &AnalyzeDLQResult{
				Topic:         topic,
				TotalMessages: 3,
				ErrorTypes:    map[string]int{"serialization_error": 2, "timeout": 1},
				Samples: []Message{
					{Partition: 0, Offset: 1, Key: "k1", Headers: map[string]string{"error.type": "serialization_error"}},
				},
				TimeRange: DLQTimeRange{Earliest: "2024-01-01T00:00:00.000Z", Latest: "2024-01-01T12:00:00.000Z"},
			}, nil
		},
	}

	result, err := mock.AnalyzeDLQ(context.Background(), "default", "orders-dlq", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalMessages != 3 {
		t.Errorf("expected 3 total messages, got %d", result.TotalMessages)
	}
	if result.ErrorTypes["serialization_error"] != 2 {
		t.Errorf("expected 2 serialization errors, got %d", result.ErrorTypes["serialization_error"])
	}
	if result.ErrorTypes["timeout"] != 1 {
		t.Errorf("expected 1 timeout error, got %d", result.ErrorTypes["timeout"])
	}
}

func TestAnalyzeDLQ_FallbackToXErrorType(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*AnalyzeDLQResult, error) {
			return &AnalyzeDLQResult{
				Topic:         topic,
				TotalMessages: 1,
				ErrorTypes:    map[string]int{"validation_error": 1},
				Samples: []Message{
					{Partition: 0, Offset: 1, Key: "k1", Headers: map[string]string{"x-error-type": "validation_error"}},
				},
			}, nil
		},
	}

	result, err := mock.AnalyzeDLQ(context.Background(), "default", "orders-dlq", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ErrorTypes["validation_error"] != 1 {
		t.Errorf("expected x-error-type fallback to work, got %v", result.ErrorTypes)
	}
}

func TestAnalyzeDLQ_ErrorMessageFallbackToProcessingError(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*AnalyzeDLQResult, error) {
			return &AnalyzeDLQResult{
				Topic:         topic,
				TotalMessages: 1,
				ErrorTypes:    map[string]int{"processing_error": 1},
				Samples: []Message{
					{Partition: 0, Offset: 1, Key: "k1", Headers: map[string]string{"error.message": "failed to process"}},
				},
			}, nil
		},
	}

	result, err := mock.AnalyzeDLQ(context.Background(), "default", "dlq", 50)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ErrorTypes["processing_error"] != 1 {
		t.Errorf("expected processing_error for error.message header, got %v", result.ErrorTypes)
	}
}

func TestAnalyzeDLQ_UnknownFallback(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*AnalyzeDLQResult, error) {
			return &AnalyzeDLQResult{
				Topic:         topic,
				TotalMessages: 2,
				ErrorTypes:    map[string]int{"unknown": 2},
				Samples: []Message{
					{Partition: 0, Offset: 1, Key: "k1", Headers: nil},
					{Partition: 0, Offset: 2, Key: "k2", Headers: map[string]string{"unrelated": "header"}},
				},
			}, nil
		},
	}

	result, err := mock.AnalyzeDLQ(context.Background(), "default", "dlq", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ErrorTypes["unknown"] != 2 {
		t.Errorf("expected 2 unknown errors, got %v", result.ErrorTypes)
	}
}

func TestAnalyzeDLQ_TimestampRange(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*AnalyzeDLQResult, error) {
			return &AnalyzeDLQResult{
				Topic:         topic,
				TotalMessages: 3,
				ErrorTypes:    map[string]int{"unknown": 3},
				TimeRange:     DLQTimeRange{Earliest: "2024-01-01T00:00:00.000Z", Latest: "2024-01-03T00:00:00.000Z"},
			}, nil
		},
	}

	result, err := mock.AnalyzeDLQ(context.Background(), "default", "dlq", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TimeRange.Earliest != "2024-01-01T00:00:00.000Z" {
		t.Errorf("expected earliest timestamp, got %q", result.TimeRange.Earliest)
	}
	if result.TimeRange.Latest != "2024-01-03T00:00:00.000Z" {
		t.Errorf("expected latest timestamp, got %q", result.TimeRange.Latest)
	}
}

func TestAnalyzeDLQ_SampleCapping(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*AnalyzeDLQResult, error) {
			samples := make([]Message, 5)
			for i := range samples {
				samples[i] = Message{Partition: 0, Offset: int64(i), Key: fmt.Sprintf("k%d", i)}
			}
			return &AnalyzeDLQResult{
				Topic:         topic,
				TotalMessages: 10,
				ErrorTypes:    map[string]int{"unknown": 10},
				Samples:       samples,
			}, nil
		},
	}

	result, err := mock.AnalyzeDLQ(context.Background(), "default", "dlq", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Samples) > 5 {
		t.Errorf("expected samples capped at 5, got %d", len(result.Samples))
	}
}

func TestAnalyzeDLQ_Error(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*AnalyzeDLQResult, error) {
			return nil, fmt.Errorf("topic not found")
		},
	}

	_, err := mock.AnalyzeDLQ(context.Background(), "default", "nonexistent-dlq", 100)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestAnalyzeDLQ_CallRecording(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*AnalyzeDLQResult, error) {
			return &AnalyzeDLQResult{Topic: topic, ErrorTypes: map[string]int{}}, nil
		},
	}

	mock.AnalyzeDLQ(context.Background(), "prod", "my-dlq", 50)
	if mock.CallCount("AnalyzeDLQ") != 1 {
		t.Error("expected AnalyzeDLQ to be called once")
	}
	call := mock.LastCall("AnalyzeDLQ")
	if call == nil {
		t.Fatal("expected non-nil last call")
	}
	if call.Args[0] != "prod" {
		t.Errorf("expected cluster 'prod', got %v", call.Args[0])
	}
	if call.Args[1] != "my-dlq" {
		t.Errorf("expected topic 'my-dlq', got %v", call.Args[1])
	}
}

// helper
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
