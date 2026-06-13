package server

import (
	"context"
	"strings"
	"testing"

	"github.com/josedab/kafkamcp/internal/config"
	"github.com/josedab/kafkamcp/internal/kafka"
	"github.com/josedab/kafkamcp/internal/masking"
)

// withMasking enables the masking engine with a single field-path rule that
// redacts the "email" field only, so tests can prove configured fields are
// masked while other fields (even ones that also contain an email) are not.
func withMasking(cfg *config.Config) {
	cfg.Masking = config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "user-email", Type: "email", FieldPath: "email"},
		},
	}
}

func newMaskingTestServer(t *testing.T, mock *kafka.MockKafkaManager) *Server {
	t.Helper()
	s := newTestServer(t, withMasking)
	s.maskingEngine = masking.NewEngine(s.config.Masking, s.logger)
	s.kafkaManager = mock
	return s
}

func TestHandleConsume_AppliesConfiguredMasking(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{
				{Partition: 0, Offset: 1, Value: map[string]any{
					"email": "alice@example.com",
					"note":  "reach me at bob@example.com",
				}},
			}, nil
		},
	}
	s := newMaskingTestServer(t, mock)

	result, err := s.handleConsume(context.Background(), makeToolRequest(map[string]any{"topic": "orders"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)

	if strings.Contains(text, "alice@example.com") {
		t.Errorf("configured email field was not masked: %s", text)
	}
	if !strings.Contains(text, "REDACTED") {
		t.Errorf("expected redaction marker in output: %s", text)
	}
	// The "note" field is not in the configured field path, so its email must
	// be preserved (proves field-path targeting, not blanket masking).
	if !strings.Contains(text, "bob@example.com") {
		t.Errorf("non-configured field should be preserved: %s", text)
	}
}

func TestHandleConsume_MaskingDisabledPreservesOutput(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{
				{Partition: 0, Offset: 1, Value: map[string]any{"email": "alice@example.com"}},
			}, nil
		},
	}
	// Default test server has masking disabled.
	s := newTestServerWithMock(t, mock)

	result, err := s.handleConsume(context.Background(), makeToolRequest(map[string]any{"topic": "orders"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	if !strings.Contains(resultText(t, result), "alice@example.com") {
		t.Errorf("masking disabled should preserve raw value: %s", resultText(t, result))
	}
}

func TestHandleSearch_AppliesConfiguredMasking(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		SearchMessagesFn: func(ctx context.Context, cluster string, req kafka.SearchRequest) ([]kafka.SearchMessage, int, error) {
			return []kafka.SearchMessage{
				{Partition: 0, Offset: 3, Value: map[string]any{"email": "carol@example.com"}},
			}, 1, nil
		},
	}
	s := newMaskingTestServer(t, mock)

	result, err := s.handleSearch(context.Background(), makeToolRequest(map[string]any{"topic": "orders"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	if strings.Contains(resultText(t, result), "carol@example.com") {
		t.Errorf("search result was not masked: %s", resultText(t, result))
	}
}

func TestHandleAnalyzeDLQ_MasksNestedSamples(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*kafka.AnalyzeDLQResult, error) {
			return &kafka.AnalyzeDLQResult{
				Topic:         topic,
				TotalMessages: 1,
				ErrorTypes:    map[string]int{"deserialize": 1},
				Samples: []kafka.Message{
					{Partition: 0, Offset: 7, Value: map[string]any{"email": "dave@example.com"}},
				},
			}, nil
		},
	}
	s := newMaskingTestServer(t, mock)

	result, err := s.handleAnalyzeDLQ(context.Background(), makeToolRequest(map[string]any{"topic": "orders.DLQ"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	if strings.Contains(resultText(t, result), "dave@example.com") {
		t.Errorf("nested DLQ sample was not masked: %s", resultText(t, result))
	}
}
