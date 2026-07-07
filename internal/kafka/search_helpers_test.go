package kafka

import (
	"testing"
	"time"
)

func TestSearchMessageMatches(t *testing.T) {
	t.Parallel()

	message := Message{
		Timestamp: "2024-01-02T03:04:05.000Z",
		Key:       "order-123",
		Value:     map[string]any{"status": "failed"},
		Headers:   map[string]string{"env": "prod"},
	}
	tests := []struct {
		name    string
		message Message
		req     SearchRequest
		endTime time.Time
		want    bool
	}{
		{
			name:    "all filters match",
			message: message,
			req: SearchRequest{
				KeyFilter:     "order",
				HeaderKey:     "env",
				HeaderValue:   "prod",
				ValueContains: "failed",
			},
			endTime: time.Date(2024, time.January, 3, 0, 0, 0, 0, time.UTC),
			want:    true,
		},
		{
			name:    "after end time",
			message: message,
			endTime: time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:    "key mismatch",
			message: message,
			req:     SearchRequest{KeyFilter: "payment"},
		},
		{
			name:    "missing headers",
			message: Message{Key: message.Key, Value: message.Value, Timestamp: message.Timestamp},
			req:     SearchRequest{HeaderKey: "env"},
		},
		{
			name:    "header value mismatch",
			message: message,
			req:     SearchRequest{HeaderKey: "env", HeaderValue: "staging"},
		},
		{
			name:    "value mismatch",
			message: message,
			req:     SearchRequest{ValueContains: "success"},
		},
		{
			name:    "malformed message timestamp does not exclude",
			message: Message{Timestamp: "not-a-time", Key: "key", Value: "value"},
			endTime: time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC),
			want:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := searchMessageMatches(tt.message, tt.req, tt.endTime); got != tt.want {
				t.Fatalf("searchMessageMatches() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestSummarizeDLQ(t *testing.T) {
	t.Parallel()

	messages := []Message{
		{Offset: 0, Timestamp: "2024-01-03T00:00:00.000Z", Headers: map[string]string{
			"error.type": "primary", "x-error-type": "secondary",
		}},
		{Offset: 1, Timestamp: "2024-01-01T00:00:00.000Z", Headers: map[string]string{
			"x-error-type": "secondary",
		}},
		{Offset: 2, Timestamp: "2024-01-02T00:00:00.000Z", Headers: map[string]string{
			"error_type": "tertiary",
		}},
		{Offset: 3, Timestamp: "2024-01-04T00:00:00.000Z", Headers: map[string]string{
			"error.message": "failed",
		}},
		{Offset: 4, Timestamp: "2024-01-05T00:00:00.000Z", Headers: map[string]string{
			"x-error-message": "failed",
		}},
		{Offset: 5, Timestamp: "2024-01-06T00:00:00.000Z"},
	}

	result := summarizeDLQ("orders.dlq", messages)
	if result.Topic != "orders.dlq" || result.TotalMessages != 6 {
		t.Fatalf("summarizeDLQ metadata = %#v", result)
	}
	wantTypes := map[string]int{
		"primary": 1, "secondary": 1, "tertiary": 1, "processing_error": 2, "unknown": 1,
	}
	for errorType, want := range wantTypes {
		if got := result.ErrorTypes[errorType]; got != want {
			t.Fatalf("summarizeDLQ error type %q = %d, want %d", errorType, got, want)
		}
	}
	if result.TimeRange.Earliest != "2024-01-01T00:00:00.000Z" ||
		result.TimeRange.Latest != "2024-01-06T00:00:00.000Z" {
		t.Fatalf("summarizeDLQ time range = %#v", result.TimeRange)
	}
	if len(result.Samples) != 5 || result.Samples[4].Offset != 4 {
		t.Fatalf("summarizeDLQ samples = %#v", result.Samples)
	}
}
