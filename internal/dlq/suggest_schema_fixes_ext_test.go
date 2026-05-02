package dlq

import (
	"context"
	"errors"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

func TestSuggestSchemaFixes_ValidMessageNoSuggestions(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{
				{Value: map[string]any{"id": "1", "name": "alice", "status": "active"}},
				{Value: map[string]any{"id": "2", "name": "bob", "status": "inactive"}},
			}, nil
		},
	}

	r := NewRemediator(discardLogger())
	suggestions, err := r.SuggestSchemaFixes(context.Background(), mock, "cluster", "orders-dlq", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(suggestions) != 0 {
		t.Errorf("expected no suggestions for valid messages, got %d: %+v", len(suggestions), suggestions)
	}
}

func TestSuggestSchemaFixes_NestedObjectValue(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{
				{Value: map[string]any{
					"user": map[string]any{"name": "alice", "age": nil},
					"metadata": nil,
				}},
			}, nil
		},
	}

	r := NewRemediator(discardLogger())
	suggestions, err := r.SuggestSchemaFixes(context.Background(), mock, "cluster", "orders-dlq", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// "metadata" is nil, should produce a suggestion
	foundMetadata := false
	for _, s := range suggestions {
		if s.Field == "metadata" && s.FixAction == "add_default" {
			foundMetadata = true
		}
	}
	if !foundMetadata {
		t.Errorf("expected suggestion for null 'metadata' field, got %+v", suggestions)
	}
}

func TestSuggestSchemaFixes_MultipleNullFieldsAggregated(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{
				{Value: map[string]any{"email": nil}},
				{Value: map[string]any{"email": nil}},
				{Value: map[string]any{"email": nil}},
			}, nil
		},
	}

	r := NewRemediator(discardLogger())
	suggestions, err := r.SuggestSchemaFixes(context.Background(), mock, "cluster", "orders-dlq", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Multiple null occurrences should produce a single suggestion with count
	found := false
	for _, s := range suggestions {
		if s.Field == "email" {
			found = true
			// Should mention "3 messages"
			if s.Issue == "" {
				t.Error("expected issue description")
			}
		}
	}
	if !found {
		t.Errorf("expected aggregated null field suggestion, got %+v", suggestions)
	}
}

func TestSuggestSchemaFixes_NonSchemaErrorHeader(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{
				{
					Value:   map[string]any{"id": "1"},
					Headers: map[string]string{"x-error-type": "timeout_exceeded"},
				},
			}, nil
		},
	}

	r := NewRemediator(discardLogger())
	suggestions, err := r.SuggestSchemaFixes(context.Background(), mock, "cluster", "orders-dlq", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Non-schema error header should not produce a _schema suggestion
	for _, s := range suggestions {
		if s.Field == "_schema" {
			t.Error("did not expect _schema suggestion for non-schema error header")
		}
	}
}

func TestSuggestSchemaFixes_BothNullAndEmptyInSameField(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{
				{Value: map[string]any{"status": nil}},
				{Value: map[string]any{"status": ""}},
			}, nil
		},
	}

	r := NewRemediator(discardLogger())
	suggestions, err := r.SuggestSchemaFixes(context.Background(), mock, "cluster", "orders-dlq", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should produce suggestions for both null and empty
	hasNull, hasEmpty := false, false
	for _, s := range suggestions {
		if s.Field == "status" && s.FixAction == "add_default" {
			hasNull = true
		}
		if s.Field == "status" && !s.AutoFixable {
			hasEmpty = true
		}
	}
	if !hasNull {
		t.Error("expected null suggestion for 'status'")
	}
	if !hasEmpty {
		t.Error("expected empty string suggestion for 'status'")
	}
}

func TestSuggestSchemaFixes_InvalidJSONMessage(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{
				{Value: "not a map value"},
			}, nil
		},
	}

	r := NewRemediator(discardLogger())
	suggestions, err := r.SuggestSchemaFixes(context.Background(), mock, "cluster", "orders-dlq", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	found := false
	for _, s := range suggestions {
		if s.Field == "_value" {
			found = true
		}
	}
	if !found {
		t.Error("expected _value suggestion for non-JSON message")
	}
}

func TestSuggestSchemaFixes_SchemaMismatchHeader(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{
				{
					Value:   map[string]any{"id": "1"},
					Headers: map[string]string{"x-error-type": "schema_mismatch"},
				},
			}, nil
		},
	}

	r := NewRemediator(discardLogger())
	suggestions, err := r.SuggestSchemaFixes(context.Background(), mock, "cluster", "orders-dlq", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	found := false
	for _, s := range suggestions {
		if s.Field == "_schema" {
			found = true
		}
	}
	if !found {
		t.Error("expected _schema suggestion for schema_mismatch header")
	}
}

func TestSuggestSchemaFixes_LimitDefault(t *testing.T) {
	t.Parallel()
	var capturedLimit int
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			capturedLimit = limit
			return nil, nil
		},
	}

	r := NewRemediator(discardLogger())
	_, err := r.SuggestSchemaFixes(context.Background(), mock, "cluster", "topic", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedLimit != 20 {
		t.Errorf("expected default limit 20, got %d", capturedLimit)
	}
}

func TestSuggestSchemaFixes_NegativeLimit(t *testing.T) {
	t.Parallel()
	var capturedLimit int
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			capturedLimit = limit
			return nil, nil
		},
	}

	r := NewRemediator(discardLogger())
	_, err := r.SuggestSchemaFixes(context.Background(), mock, "cluster", "topic", -5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedLimit != 20 {
		t.Errorf("expected default limit 20 for negative input, got %d", capturedLimit)
	}
}

func TestSuggestSchemaFixes_ConsumeErrorExt(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return nil, errors.New("consume failed")
		},
	}

	r := NewRemediator(discardLogger())
	_, err := r.SuggestSchemaFixes(context.Background(), mock, "cluster", "topic", 10)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSuggestSchemaFixes_EmptyConsumeResult(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return nil, nil
		},
	}

	r := NewRemediator(discardLogger())
	suggestions, err := r.SuggestSchemaFixes(context.Background(), mock, "cluster", "topic", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(suggestions) != 0 {
		t.Errorf("expected 0 suggestions for empty result, got %d", len(suggestions))
	}
}

func TestSuggestSchemaFixes_MixOfValidInvalidNull(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{
				{Value: map[string]any{"id": "1", "name": "alice"}},
				{Value: "raw string not a map"},
				{Value: map[string]any{"id": nil, "name": ""}},
			}, nil
		},
	}

	r := NewRemediator(discardLogger())
	suggestions, err := r.SuggestSchemaFixes(context.Background(), mock, "cluster", "topic", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should have: _value (invalid JSON), id:null, name:empty
	if len(suggestions) < 2 {
		t.Errorf("expected at least 2 suggestions, got %d: %+v", len(suggestions), suggestions)
	}
}
