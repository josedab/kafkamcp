package server

import (
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/kafkamcp/kafkamcp/internal/nlquery"
)

// ===================== computeSchemaDiff Tests =====================

func TestComputeSchemaDiff(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		schemaFrom    any
		schemaTo      any
		wantAdded     int
		wantRemoved   int
		wantChanged   int
		wantUnchanged int
	}{
		{
			name:          "identical avro schemas",
			schemaFrom:    map[string]any{"fields": []any{map[string]any{"name": "id", "type": "int"}, map[string]any{"name": "name", "type": "string"}}},
			schemaTo:      map[string]any{"fields": []any{map[string]any{"name": "id", "type": "int"}, map[string]any{"name": "name", "type": "string"}}},
			wantAdded:     0,
			wantRemoved:   0,
			wantChanged:   0,
			wantUnchanged: 2,
		},
		{
			name:          "field added",
			schemaFrom:    map[string]any{"fields": []any{map[string]any{"name": "id", "type": "int"}}},
			schemaTo:      map[string]any{"fields": []any{map[string]any{"name": "id", "type": "int"}, map[string]any{"name": "email", "type": "string"}}},
			wantAdded:     1,
			wantRemoved:   0,
			wantChanged:   0,
			wantUnchanged: 1,
		},
		{
			name:          "field removed",
			schemaFrom:    map[string]any{"fields": []any{map[string]any{"name": "id", "type": "int"}, map[string]any{"name": "email", "type": "string"}}},
			schemaTo:      map[string]any{"fields": []any{map[string]any{"name": "id", "type": "int"}}},
			wantAdded:     0,
			wantRemoved:   1,
			wantChanged:   0,
			wantUnchanged: 1,
		},
		{
			name:          "field type changed",
			schemaFrom:    map[string]any{"fields": []any{map[string]any{"name": "age", "type": "int"}}},
			schemaTo:      map[string]any{"fields": []any{map[string]any{"name": "age", "type": "long"}}},
			wantAdded:     0,
			wantRemoved:   0,
			wantChanged:   1,
			wantUnchanged: 0,
		},
		{
			name:          "mixed changes",
			schemaFrom:    map[string]any{"fields": []any{map[string]any{"name": "id", "type": "int"}, map[string]any{"name": "old_field", "type": "string"}, map[string]any{"name": "age", "type": "int"}}},
			schemaTo:      map[string]any{"fields": []any{map[string]any{"name": "id", "type": "int"}, map[string]any{"name": "new_field", "type": "string"}, map[string]any{"name": "age", "type": "long"}}},
			wantAdded:     1,
			wantRemoved:   1,
			wantChanged:   1,
			wantUnchanged: 1,
		},
		{
			name:          "both nil schemas",
			schemaFrom:    nil,
			schemaTo:      nil,
			wantAdded:     0,
			wantRemoved:   0,
			wantChanged:   0,
			wantUnchanged: 0,
		},
		{
			name:          "from nil to schema",
			schemaFrom:    nil,
			schemaTo:      map[string]any{"fields": []any{map[string]any{"name": "id", "type": "int"}}},
			wantAdded:     1,
			wantRemoved:   0,
			wantChanged:   0,
			wantUnchanged: 0,
		},
		{
			name:          "json schema properties diff",
			schemaFrom:    map[string]any{"properties": map[string]any{"name": map[string]any{"type": "string"}}},
			schemaTo:      map[string]any{"properties": map[string]any{"name": map[string]any{"type": "string"}, "email": map[string]any{"type": "string"}}},
			wantAdded:     1,
			wantRemoved:   0,
			wantChanged:   0,
			wantUnchanged: 1,
		},
		{
			name:          "empty fields arrays",
			schemaFrom:    map[string]any{"fields": []any{}},
			schemaTo:      map[string]any{"fields": []any{}},
			wantAdded:     0,
			wantRemoved:   0,
			wantChanged:   0,
			wantUnchanged: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diff := computeSchemaDiff(tt.schemaFrom, tt.schemaTo)

			added, _ := diff["added"].([]string)
			if added == nil {
				added = []string{}
			}
			removed, _ := diff["removed"].([]string)
			if removed == nil {
				removed = []string{}
			}
			changed, _ := diff["changed"].([]map[string]any)
			if changed == nil {
				changed = []map[string]any{}
			}
			unchanged, _ := diff["unchanged"].(int)

			if len(added) != tt.wantAdded {
				t.Errorf("added = %d, want %d (fields: %v)", len(added), tt.wantAdded, added)
			}
			if len(removed) != tt.wantRemoved {
				t.Errorf("removed = %d, want %d (fields: %v)", len(removed), tt.wantRemoved, removed)
			}
			if len(changed) != tt.wantChanged {
				t.Errorf("changed = %d, want %d", len(changed), tt.wantChanged)
			}
			if unchanged != tt.wantUnchanged {
				t.Errorf("unchanged = %d, want %d", unchanged, tt.wantUnchanged)
			}
		})
	}
}

// ===================== extractSchemaFields Tests =====================

func TestExtractSchemaFields(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		schema     any
		wantFields []string
	}{
		{
			name:       "avro schema with fields",
			schema:     map[string]any{"fields": []any{map[string]any{"name": "id", "type": "int"}, map[string]any{"name": "name", "type": "string"}}},
			wantFields: []string{"id", "name"},
		},
		{
			name:       "json schema with properties",
			schema:     map[string]any{"properties": map[string]any{"email": map[string]any{"type": "string"}, "age": map[string]any{"type": "integer"}}},
			wantFields: []string{"email", "age"},
		},
		{
			name:       "nil schema",
			schema:     nil,
			wantFields: []string{},
		},
		{
			name:       "empty map schema",
			schema:     map[string]any{},
			wantFields: []string{},
		},
		{
			name:       "json string avro schema",
			schema:     `{"fields":[{"name":"id","type":"int"},{"name":"val","type":"string"}]}`,
			wantFields: []string{"id", "val"},
		},
		{
			name:       "invalid json string",
			schema:     `not valid json`,
			wantFields: []string{},
		},
		{
			name:       "field with empty name skipped",
			schema:     map[string]any{"fields": []any{map[string]any{"name": "", "type": "int"}, map[string]any{"name": "valid", "type": "string"}}},
			wantFields: []string{"valid"},
		},
		{
			name:       "fields array with non-map entries",
			schema:     map[string]any{"fields": []any{"not-a-map", map[string]any{"name": "ok", "type": "int"}}},
			wantFields: []string{"ok"},
		},
		{
			name:       "integer schema value",
			schema:     42,
			wantFields: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields := extractSchemaFields(tt.schema)
			if len(fields) != len(tt.wantFields) {
				t.Errorf("got %d fields, want %d (got: %v)", len(fields), len(tt.wantFields), fields)
				return
			}
			for _, name := range tt.wantFields {
				if _, ok := fields[name]; !ok {
					t.Errorf("missing expected field %q in %v", name, fields)
				}
			}
		})
	}
}

// ===================== formatDuration Tests =====================

func TestFormatDuration(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		duration time.Duration
		want     string
	}{
		{"zero", 0, "0ms"},
		{"1 millisecond", time.Millisecond, "1ms"},
		{"500 milliseconds", 500 * time.Millisecond, "500ms"},
		{"999 milliseconds", 999 * time.Millisecond, "999ms"},
		{"1 second", time.Second, "1.0s"},
		{"1.5 seconds", 1500 * time.Millisecond, "1.5s"},
		{"59.9 seconds", 59900 * time.Millisecond, "59.9s"},
		{"1 minute", time.Minute, "1.0m"},
		{"2.5 minutes", 150 * time.Second, "2.5m"},
		{"10 minutes", 10 * time.Minute, "10.0m"},
		{"sub-millisecond", 500 * time.Microsecond, "0ms"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatDuration(tt.duration)
			if got != tt.want {
				t.Errorf("formatDuration(%v) = %q, want %q", tt.duration, got, tt.want)
			}
		})
	}
}

// ===================== extractStringMapArg Tests =====================

func TestExtractStringMapArg(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args any
		key  string
		want map[string]string
	}{
		{
			name: "map[string]any values",
			args: map[string]any{
				"config": map[string]any{
					"retention.ms":   "86400000",
					"compression":    "snappy",
					"numeric_as_any": 42,
				},
			},
			key: "config",
			want: map[string]string{
				"retention.ms":   "86400000",
				"compression":    "snappy",
				"numeric_as_any": "42",
			},
		},
		{
			name: "missing key returns nil",
			args: map[string]any{"other": "value"},
			key:  "config",
			want: nil,
		},
		{
			name: "nil arguments returns nil",
			args: nil,
			key:  "config",
			want: nil,
		},
		{
			name: "non-map arguments returns nil",
			args: "not a map",
			key:  "config",
			want: nil,
		},
		{
			name: "empty nested map returns nil",
			args: map[string]any{
				"config": map[string]any{},
			},
			key:  "config",
			want: nil,
		},
		{
			name: "value is not a map returns nil",
			args: map[string]any{
				"config": "string-value",
			},
			key:  "config",
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := mcp.CallToolRequest{}
			req.Params.Arguments = tt.args
			got := extractStringMapArg(req, tt.key)
			if tt.want == nil {
				if got != nil {
					t.Errorf("expected nil, got %v", got)
				}
				return
			}
			if len(got) != len(tt.want) {
				t.Errorf("got %d entries, want %d", len(got), len(tt.want))
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

// ===================== matchesFilters Additional Tests =====================

func TestMatchesFiltersComplex(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		msg     kafka.Message
		filters []nlquery.Filter
		want    bool
	}{
		{
			name: "multiple filters all match",
			msg: kafka.Message{
				Value: map[string]any{
					"status": "error",
					"level":  "critical",
					"code":   "500",
				},
			},
			filters: []nlquery.Filter{
				{Field: "status", Operator: "eq", Value: "error"},
				{Field: "level", Operator: "eq", Value: "critical"},
				{Field: "code", Operator: "eq", Value: "500"},
			},
			want: true,
		},
		{
			name: "multiple filters one fails",
			msg: kafka.Message{
				Value: map[string]any{
					"status": "error",
					"level":  "warning",
				},
			},
			filters: []nlquery.Filter{
				{Field: "status", Operator: "eq", Value: "error"},
				{Field: "level", Operator: "eq", Value: "critical"},
			},
			want: false,
		},
		{
			name: "mixed operators all match",
			msg: kafka.Message{
				Value: map[string]any{
					"status":  "error",
					"message": "connection timeout occurred",
					"code":    "500",
				},
			},
			filters: []nlquery.Filter{
				{Field: "status", Operator: "neq", Value: "success"},
				{Field: "message", Operator: "contains", Value: "timeout"},
				{Field: "code", Operator: "eq", Value: "500"},
			},
			want: true,
		},
		{
			name: "non-map value with empty field",
			msg: kafka.Message{
				Value: "raw string value",
			},
			filters: []nlquery.Filter{
				{Field: "", Operator: "eq", Value: "raw string value"},
			},
			want: true,
		},
		{
			name: "non-map value with field returns false",
			msg: kafka.Message{
				Value: "raw string value",
			},
			filters: []nlquery.Filter{
				{Field: "status", Operator: "eq", Value: "anything"},
			},
			want: false,
		},
		{
			name: "numeric value converted to string for eq",
			msg: kafka.Message{
				Value: map[string]any{
					"count": 42,
				},
			},
			filters: []nlquery.Filter{
				{Field: "count", Operator: "eq", Value: "42"},
			},
			want: true,
		},
		{
			name: "nil value in message",
			msg: kafka.Message{
				Value: nil,
			},
			filters: []nlquery.Filter{
				{Field: "any", Operator: "eq", Value: "val"},
			},
			want: false,
		},
		{
			name: "unknown operator passes (no match logic)",
			msg: kafka.Message{
				Value: map[string]any{"x": "y"},
			},
			filters: []nlquery.Filter{
				{Field: "x", Operator: "gt", Value: "0"},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchesFilters(tt.msg, tt.filters)
			if got != tt.want {
				t.Errorf("matchesFilters() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ===================== estimateThroughput Additional Tests =====================

func TestEstimateThroughputEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		detail *kafka.TopicDetail
		want   float64
	}{
		{
			name: "single partition with data",
			detail: &kafka.TopicDetail{
				Partitions: []kafka.PartitionDetail{
					{EarliestOffset: 0, LatestOffset: 3600},
				},
			},
			want: 1,
		},
		{
			name: "negative offset difference returns default",
			detail: &kafka.TopicDetail{
				Partitions: []kafka.PartitionDetail{
					{EarliestOffset: 100, LatestOffset: 50},
				},
			},
			want: 1,
		},
		{
			name: "zero offset range returns default",
			detail: &kafka.TopicDetail{
				Partitions: []kafka.PartitionDetail{
					{EarliestOffset: 100, LatestOffset: 100},
				},
			},
			want: 1,
		},
		{
			name: "large offset range",
			detail: &kafka.TopicDetail{
				Partitions: []kafka.PartitionDetail{
					{EarliestOffset: 0, LatestOffset: 36000},
					{EarliestOffset: 0, LatestOffset: 36000},
					{EarliestOffset: 0, LatestOffset: 36000},
				},
			},
			want: 30,
		},
		{
			name:   "nil partitions slice",
			detail: &kafka.TopicDetail{},
			want:   1,
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

// ===================== Constants Validation Tests =====================

func TestConstantsAreReasonable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value int
		min   int
		max   int
	}{
		{"MaxPartitions", MaxPartitions, 1, 100000},
		{"MaxReplicationFactor", MaxReplicationFactor, 1, 100},
		{"MaxSchemaSize", MaxSchemaSize, 1024, 10 << 20},
		{"MaxDLQMessages", MaxDLQMessages, 100, 1000000},
		{"MaxSearchScan", MaxSearchScan, 1000, 10000000},
		{"DefaultSearchScan", DefaultSearchScan, 100, MaxSearchScan},
		{"MaxProduceMessages", MaxProduceMessages, 1, 10000},
		{"MaxConsumeLimit", MaxConsumeLimit, 1, 100000},
		{"MaxConsumeTimeout", MaxConsumeTimeout, 1000, 600000},
		{"MaxJoinLimit", MaxJoinLimit, 1, 1000000},
		{"maxInputLength", maxInputLength, 64, 1 << 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.value < tt.min || tt.value > tt.max {
				t.Errorf("%s = %d, expected between %d and %d", tt.name, tt.value, tt.min, tt.max)
			}
		})
	}
}

func TestDefaultSearchScanLessThanMax(t *testing.T) {
	t.Parallel()
	if DefaultSearchScan > MaxSearchScan {
		t.Errorf("DefaultSearchScan (%d) should be <= MaxSearchScan (%d)", DefaultSearchScan, MaxSearchScan)
	}
}

func TestMaxConsumeLimitLessThanMaxSearchScan(t *testing.T) {
	t.Parallel()
	if MaxConsumeLimit > MaxSearchScan {
		t.Errorf("MaxConsumeLimit (%d) should be <= MaxSearchScan (%d)", MaxConsumeLimit, MaxSearchScan)
	}
}
