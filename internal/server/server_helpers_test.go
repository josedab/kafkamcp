package server

import (
	"net/http"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
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
