package server

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/kafkamcp/kafkamcp/internal/nlquery"
)

func TestClampInt64(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		v, min, max int64
		want        int64
	}{
		{"within range", 50, 0, 100, 50},
		{"below min", -10, 0, 100, 0},
		{"above max", 200, 0, 100, 100},
		{"equal min", 0, 0, 100, 0},
		{"equal max", 100, 0, 100, 100},
		{"negative range", -5, -10, -1, -5},
		{"zero range", 5, 5, 5, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := clampInt64(tt.v, tt.min, tt.max)
			if got != tt.want {
				t.Errorf("clampInt64(%d, %d, %d) = %d, want %d", tt.v, tt.min, tt.max, got, tt.want)
			}
		})
	}
}

func TestValidateStringLength(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{"short string", "hello", false},
		{"empty string", "", false},
		{"at limit", strings.Repeat("a", maxInputLength), false},
		{"over limit", strings.Repeat("a", maxInputLength+1), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := validateStringLength("test_field", tt.value)
			if tt.wantErr && result == nil {
				t.Error("expected error result for over-limit string")
			}
			if !tt.wantErr && result != nil {
				t.Error("expected nil result for valid string")
			}
		})
	}
}

func TestSanitizeAuditParams(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		tool   string
		params map[string]any
		key    string
		want   string
	}{
		{
			"produce messages redacted",
			"kafka_produce",
			map[string]any{"topic": "test", "messages": `[{"key":"a"}]`},
			"messages",
			"**REDACTED**",
		},
		{
			"produce topic preserved",
			"kafka_produce",
			map[string]any{"topic": "test", "messages": `[{"key":"a"}]`},
			"topic",
			"test",
		},
		{
			"non-sensitive tool",
			"kafka_list_topics",
			map[string]any{"filter": "*"},
			"filter",
			"*",
		},
		{
			"nil params",
			"kafka_produce",
			nil,
			"",
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeAuditParams(tt.tool, tt.params)
			if tt.params == nil {
				if result != nil {
					t.Error("expected nil result for nil params")
				}
				return
			}
			if tt.key != "" {
				got, _ := result[tt.key].(string)
				if got != tt.want {
					t.Errorf("sanitizeAuditParams[%q] = %q, want %q", tt.key, got, tt.want)
				}
			}
		})
	}
}

func TestSanitizeAuditParamsSchemaRedacted(t *testing.T) {
	t.Parallel()
	params := map[string]any{"subject": "test", "schema": `{"type":"record"}`}
	result := sanitizeAuditParams("kafka_check_schema_compatibility", params)
	if result["schema"] != "**REDACTED**" {
		t.Errorf("expected schema redacted, got %v", result["schema"])
	}
	if result["subject"] != "test" {
		t.Errorf("expected subject preserved, got %v", result["subject"])
	}
}

func TestMarshalResultComplex(t *testing.T) {
	t.Parallel()
	data := map[string]any{
		"topics": []string{"a", "b", "c"},
		"count":  3,
	}
	result, err := marshalResult(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestMarshalResultNil(t *testing.T) {
	t.Parallel()
	result, err := marshalResult(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result for nil value (marshals as 'null')")
	}
}

func TestMarshalResourceJSONComplex(t *testing.T) {
	t.Parallel()
	data := map[string]any{"status": "healthy", "brokers": 3}
	contents, err := marshalResourceJSON("kafka://cluster/health", data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(contents) != 1 {
		t.Fatalf("expected 1 content, got %d", len(contents))
	}
}

func TestMatchTopicFilterAdditional(t *testing.T) {
	t.Parallel()
	tests := []struct {
		pattern string
		name    string
		want    bool
	}{
		{"", "anything", false},
		{"*", "", true},
		{"events.*", "events.click", true},
		{"events.*", "logs.click", false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.name, func(t *testing.T) {
			got := matchTopicFilter(tt.pattern, tt.name)
			if got != tt.want {
				t.Errorf("matchTopicFilter(%q, %q) = %v, want %v", tt.pattern, tt.name, got, tt.want)
			}
		})
	}
}

func TestExtractAgentIDVariants(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		headerKey string
		headerVal string
		want      string
	}{
		{"no header", "", "", "anonymous"},
		{"standard header", "X-Agent-Id", "agent-1", "agent-1"},
		{"empty header value", "X-Agent-Id", "", "anonymous"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := mcp.CallToolRequest{}
			if tt.headerKey != "" {
				req.Header = http.Header{tt.headerKey: []string{tt.headerVal}}
			}
			got := extractAgentID(req)
			if got != tt.want {
				t.Errorf("extractAgentID() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExtractAgentIDFromResourceVariants(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		headerKey string
		headerVal string
		want      string
	}{
		{"no header", "", "", "anonymous"},
		{"with header", "X-Agent-Id", "res-agent", "res-agent"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := mcp.ReadResourceRequest{}
			if tt.headerKey != "" {
				req.Header = http.Header{tt.headerKey: []string{tt.headerVal}}
			}
			got := extractAgentIDFromResource(req)
			if got != tt.want {
				t.Errorf("extractAgentIDFromResource() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestContainsStr(t *testing.T) {
	t.Parallel()
	tests := []struct {
		s, substr string
		want      bool
	}{
		{"hello world", "world", true},
		{"hello world", "xyz", false},
		{"hello", "", true},
		{"", "", true},
		{"", "a", false},
		{"abc", "abc", true},
	}
	for _, tt := range tests {
		got := containsStr(tt.s, tt.substr)
		if got != tt.want {
			t.Errorf("containsStr(%q, %q) = %v, want %v", tt.s, tt.substr, got, tt.want)
		}
	}
}

func TestSearchString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		s, substr string
		want      bool
	}{
		{"hello world", "world", true},
		{"hello world", "xyz", false},
		{"abc", "abc", true},
		{"abcdef", "bcd", true},
	}
	for _, tt := range tests {
		got := searchString(tt.s, tt.substr)
		if got != tt.want {
			t.Errorf("searchString(%q, %q) = %v, want %v", tt.s, tt.substr, got, tt.want)
		}
	}
}

func TestExtractFieldValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value any
		field string
		want  any
	}{
		{"empty field", "raw-value", "", "raw-value"},
		{"map field", map[string]any{"name": "test"}, "name", "test"},
		{"missing field", map[string]any{"a": 1}, "b", nil},
		{"non-map value", "string", "field", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractFieldValue(tt.value, tt.field)
			if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", tt.want) {
				t.Errorf("extractFieldValue = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEstimateThroughput(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		detail *kafka.TopicDetail
		want   float64
	}{
		{
			"no partitions",
			&kafka.TopicDetail{},
			1,
		},
		{
			"with data",
			&kafka.TopicDetail{
				Partitions: []kafka.PartitionDetail{
					{EarliestOffset: 0, LatestOffset: 3600},
				},
			},
			1,
		},
		{
			"multiple partitions",
			&kafka.TopicDetail{
				Partitions: []kafka.PartitionDetail{
					{EarliestOffset: 0, LatestOffset: 7200},
					{EarliestOffset: 0, LatestOffset: 7200},
				},
			},
			4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := estimateThroughput(tt.detail)
			if got != tt.want {
				t.Errorf("estimateThroughput() = %f, want %f", got, tt.want)
			}
		})
	}
}

func TestIntFromAny(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		v    any
		want int
	}{
		{"float64", float64(42), 42},
		{"int", int(10), 10},
		{"int64", int64(100), 100},
		{"string fallback", "not a number", 5},
		{"nil fallback", nil, 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := intFromAny(tt.v, 5)
			if got != tt.want {
				t.Errorf("intFromAny(%v) = %d, want %d", tt.v, got, tt.want)
			}
		})
	}
}

func TestMatchesFilters(t *testing.T) {
	t.Parallel()
	msg := kafka.Message{
		Value: map[string]any{
			"status": "error",
			"level":  "critical",
		},
	}

	tests := []struct {
		name    string
		filters []nlquery.Filter
		want    bool
	}{
		{"no filters", nil, true},
		{"eq match", []nlquery.Filter{{Field: "status", Operator: "eq", Value: "error"}}, true},
		{"eq no match", []nlquery.Filter{{Field: "status", Operator: "eq", Value: "success"}}, false},
		{"neq match", []nlquery.Filter{{Field: "status", Operator: "neq", Value: "success"}}, true},
		{"neq no match", []nlquery.Filter{{Field: "status", Operator: "neq", Value: "error"}}, false},
		{"contains match", []nlquery.Filter{{Field: "status", Operator: "contains", Value: "err"}}, true},
		{"contains no match", []nlquery.Filter{{Field: "status", Operator: "contains", Value: "xyz"}}, false},
		{"missing field", []nlquery.Filter{{Field: "nonexistent", Operator: "eq", Value: "val"}}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchesFilters(msg, tt.filters)
			if got != tt.want {
				t.Errorf("matchesFilters() = %v, want %v", got, tt.want)
			}
		})
	}
}
