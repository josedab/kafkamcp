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
