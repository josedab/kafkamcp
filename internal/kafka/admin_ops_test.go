package kafka

import (
	"context"
	"encoding/base64"
	"testing"
	"time"
)

func TestValidateTopicConfigs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		configs map[string]string
		wantErr bool
	}{
		{"nil configs", nil, false},
		{"empty configs", map[string]string{}, false},
		{"safe configs", map[string]string{"retention.ms": "86400000", "cleanup.policy": "compact"}, false},
		{"dangerous min.insync.replicas", map[string]string{"min.insync.replicas": "1"}, true},
		{"dangerous unclean.leader", map[string]string{"unclean.leader.election.enable": "true"}, true},
		{"dangerous message.downconversion", map[string]string{"message.downconversion.enable": "false"}, true},
		{"case insensitive", map[string]string{"Min.Insync.Replicas": "2"}, true},
		{"mixed safe and dangerous", map[string]string{"retention.ms": "1000", "min.insync.replicas": "1"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTopicConfigs(tt.configs)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateTopicConfigs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateTopicConfigs_ErrorMessage(t *testing.T) {
	t.Parallel()
	err := validateTopicConfigs(map[string]string{"min.insync.replicas": "1"})
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() == "" {
		t.Error("expected non-empty error message")
	}
}

func TestRedactSensitiveConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cfg  map[string]string
		want map[string]string
	}{
		{
			"no sensitive keys",
			map[string]string{"connector.class": "FileSource", "topic": "test"},
			map[string]string{"connector.class": "FileSource", "topic": "test"},
		},
		{
			"password redacted",
			map[string]string{"db.password": "secret123", "topic": "test"},
			map[string]string{"db.password": "**REDACTED**", "topic": "test"},
		},
		{
			"multiple sensitive keys",
			map[string]string{"api.key": "k", "ssl.keystore.password": "p", "name": "conn1"},
			map[string]string{"api.key": "**REDACTED**", "ssl.keystore.password": "**REDACTED**", "name": "conn1"},
		},
		{
			"empty sensitive value not redacted",
			map[string]string{"db.password": "", "topic": "test"},
			map[string]string{"db.password": "", "topic": "test"},
		},
		{
			"case insensitive match",
			map[string]string{"DB.Password": "secret"},
			map[string]string{"DB.Password": "**REDACTED**"},
		},
		{
			"token in key",
			map[string]string{"auth.token": "abc123"},
			map[string]string{"auth.token": "**REDACTED**"},
		},
		{
			"nil config",
			nil,
			map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := redactSensitiveConfig(tt.cfg)
			if len(got) != len(tt.want) {
				t.Errorf("length mismatch: got %d, want %d", len(got), len(tt.want))
				return
			}
			for k, wantV := range tt.want {
				if gotV, ok := got[k]; !ok || gotV != wantV {
					t.Errorf("key %q: got %q, want %q", k, gotV, wantV)
				}
			}
		})
	}
}

// ===================== Helper function tests =====================

func TestDecodeValue_Formats(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		data     []byte
		format   string
		wantType string // "string", "map", "nil", "base64"
	}{
		{"empty data", nil, "auto", "nil"},
		{"empty bytes", []byte{}, "auto", "nil"},
		{"auto json", []byte(`{"key":"value"}`), "auto", "map"},
		{"auto plain", []byte("hello"), "auto", "string"},
		{"string format", []byte("plain"), "string", "string"},
		{"json format valid", []byte(`{"a":1}`), "json", "map"},
		{"json format invalid", []byte("not json"), "json", "string"},
		{"bytes format", []byte("binary"), "bytes", "base64"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := decodeValue(tt.data, tt.format)
			switch tt.wantType {
			case "nil":
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
			case "string":
				if _, ok := result.(string); !ok {
					t.Errorf("expected string, got %T", result)
				}
			case "map":
				if _, ok := result.(map[string]any); !ok {
					t.Errorf("expected map, got %T", result)
				}
			case "base64":
				s, ok := result.(string)
				if !ok {
					t.Fatalf("expected string for base64, got %T", result)
				}
				if _, err := base64.StdEncoding.DecodeString(s); err != nil {
					t.Errorf("expected valid base64, got: %s", s)
				}
			}
		})
	}
}

func TestParseTimestamp_Formats(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input   string
		wantErr bool
	}{
		{"2024-01-01T00:00:00Z", false},
		{"2024-01-01T00:00:00+00:00", false},
		{"2024-01-01T00:00:00.123456789Z", false},
		{"2024-01-01T12:30:45", false},
		{"2024-01-01", false},
		{"not-a-timestamp", true},
		{"", true},
		{"12345", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			_, err := parseTimestamp(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseTimestamp(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestParseDurationMs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		ms       int
		expected time.Duration
	}{
		{5000, 5 * time.Second},
		{0, 5 * time.Second},
		{-1, 5 * time.Second},
		{100, 100 * time.Millisecond},
		{30000, 30 * time.Second},
	}

	for _, tt := range tests {
		d := parseDurationMs(tt.ms)
		if d != tt.expected {
			t.Errorf("parseDurationMs(%d) = %v, want %v", tt.ms, d, tt.expected)
		}
	}
}

func TestSerializeValue_Extended(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{"nil", nil, ""},
		{"string", "hello", "hello"},
		{"map", map[string]string{"k": "v"}, `{"k":"v"}`},
		{"number", 42, "42"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := serializeValue(tt.input)
			if got != tt.expected {
				t.Errorf("serializeValue(%v) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

// ===================== SearchRequest / AnalyzeDLQ via mock =====================

func TestSearchMessages_MockManager_KeyFilter(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		SearchMessagesFn: func(ctx context.Context, cluster string, req SearchRequest) ([]SearchMessage, int, error) {
			return []Message{
				{Key: "order-123", Value: "data", Partition: 0, Offset: 1},
			}, 50, nil
		},
	}

	msgs, scanned, err := mock.SearchMessages(context.Background(), "default", SearchRequest{
		Topic:     "orders",
		KeyFilter: "order-123",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 1 {
		t.Errorf("expected 1 match, got %d", len(msgs))
	}
	if scanned != 50 {
		t.Errorf("expected 50 scanned, got %d", scanned)
	}
}

func TestSearchMessages_MockManager_EmptyResults(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		SearchMessagesFn: func(ctx context.Context, cluster string, req SearchRequest) ([]SearchMessage, int, error) {
			return nil, 0, nil
		},
	}

	msgs, scanned, err := mock.SearchMessages(context.Background(), "default", SearchRequest{Topic: "empty"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 0 || scanned != 0 {
		t.Errorf("expected empty results, got %d msgs, %d scanned", len(msgs), scanned)
	}
}

func TestAnalyzeDLQ_MockManager_CategorizeErrors(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*AnalyzeDLQResult, error) {
			return &AnalyzeDLQResult{
				Topic:         topic,
				TotalMessages: 25,
				ErrorTypes:    map[string]int{"serialization": 10, "timeout": 15},
				Samples:       []Message{{Key: "k", Value: "v"}},
				TimeRange:     DLQTimeRange{Earliest: "2024-01-01T00:00:00Z", Latest: "2024-01-02T00:00:00Z"},
			}, nil
		},
	}

	result, err := mock.AnalyzeDLQ(context.Background(), "default", "orders-dlq", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalMessages != 25 {
		t.Errorf("expected 25 messages, got %d", result.TotalMessages)
	}
	if result.ErrorTypes["serialization"] != 10 {
		t.Errorf("expected 10 serialization errors, got %d", result.ErrorTypes["serialization"])
	}
}

func TestAnalyzeDLQ_MockManager_EmptyDLQ(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*AnalyzeDLQResult, error) {
			return &AnalyzeDLQResult{
				Topic:         topic,
				TotalMessages: 0,
				ErrorTypes:    map[string]int{},
				Samples:       nil,
			}, nil
		},
	}

	result, err := mock.AnalyzeDLQ(context.Background(), "default", "empty-dlq", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalMessages != 0 {
		t.Errorf("expected 0 messages, got %d", result.TotalMessages)
	}
}
