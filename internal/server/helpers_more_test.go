package server

import (
	"testing"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/sandbox"
)

func TestMapIntentToTools(t *testing.T) {
	t.Parallel()
	tests := []struct {
		intent  string
		wantOne string // any sentinel tool we expect in the result
	}{
		{"lag_diagnosis", "kafka_lag_diagnosis"},
		{"anomaly_detection", "kafka_anomaly_detect"},
		{"schema_inspection", "kafka_list_schemas"},
		{"topic_inspection", "kafka_list_topics"},
		{"group_inspection", "kafka_consumer_groups"},
		{"produce_message", "kafka_produce"},
		{"consume_message", "kafka_consume"},
		{"lineage_tracking", "kafka_lineage_trace"},
		{"capacity_planning", "kafka_forecast_topic"},
		{"incident_triage", "kafka_anomaly_detect"},
		{"cost_analysis", "kafka_cost_analyze"},
		{"unknown_intent", "kafka_cluster_info"}, // default fallback
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.intent, func(t *testing.T) {
			t.Parallel()
			tools := mapIntentToTools(tc.intent)
			if len(tools) == 0 {
				t.Fatal("expected non-empty tool list")
			}
			found := false
			for _, tool := range tools {
				if tool == tc.wantOne {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected %q in %v", tc.wantOne, tools)
			}
		})
	}
}

func TestClampSandboxQuotas(t *testing.T) {
	t.Parallel()
	t.Run("below cap is untouched", func(t *testing.T) {
		t.Parallel()
		q := sandbox.ResourceQuotas{
			MaxTopics:       10,
			MaxPartitions:   5,
			MaxMessages:     1000,
			MaxStorageBytes: 1 << 20,
			MaxDurationSec:  60,
		}
		orig := q
		clampSandboxQuotas(&q)
		if q != orig {
			t.Errorf("values below cap should not change: got %+v want %+v", q, orig)
		}
	})
	t.Run("above cap is clamped", func(t *testing.T) {
		t.Parallel()
		q := sandbox.ResourceQuotas{
			MaxTopics:       1000,
			MaxPartitions:   1000,
			MaxMessages:     1 << 30,
			MaxStorageBytes: 1 << 40,
			MaxDurationSec:  1 << 30,
		}
		clampSandboxQuotas(&q)
		if q.MaxTopics != 50 {
			t.Errorf("MaxTopics = %d want 50", q.MaxTopics)
		}
		if q.MaxPartitions != 12 {
			t.Errorf("MaxPartitions = %d want 12", q.MaxPartitions)
		}
		if q.MaxMessages != 100000 {
			t.Errorf("MaxMessages = %d want 100000", q.MaxMessages)
		}
		if q.MaxStorageBytes != 1<<30 {
			t.Errorf("MaxStorageBytes = %d want %d", q.MaxStorageBytes, int64(1<<30))
		}
		if q.MaxDurationSec != 86400 {
			t.Errorf("MaxDurationSec = %d want 86400", q.MaxDurationSec)
		}
	})
}

func TestBuildEventFilter(t *testing.T) {
	t.Parallel()
	t.Run("all fields populated", func(t *testing.T) {
		t.Parallel()
		req := mcp.CallToolRequest{}
		req.Params.Arguments = map[string]any{
			"event_type":     "tool_invoked",
			"agent_filter":   "agent-1",
			"tool_filter":    "kafka_consume",
			"cluster_filter": "primary",
			"source_pattern": "*.events",
		}
		f := buildEventFilter(req)
		if len(f.EventTypes) != 1 || string(f.EventTypes[0]) != "tool_invoked" {
			t.Errorf("EventTypes = %v", f.EventTypes)
		}
		if len(f.AgentIDs) != 1 || f.AgentIDs[0] != "agent-1" {
			t.Errorf("AgentIDs = %v", f.AgentIDs)
		}
		if len(f.Tools) != 1 || f.Tools[0] != "kafka_consume" {
			t.Errorf("Tools = %v", f.Tools)
		}
		if len(f.Clusters) != 1 || f.Clusters[0] != "primary" {
			t.Errorf("Clusters = %v", f.Clusters)
		}
		if f.SourcePattern != "*.events" {
			t.Errorf("SourcePattern = %q", f.SourcePattern)
		}
	})
	t.Run("empty fields yield empty filter", func(t *testing.T) {
		t.Parallel()
		req := mcp.CallToolRequest{}
		req.Params.Arguments = map[string]any{}
		f := buildEventFilter(req)
		if len(f.EventTypes) != 0 || len(f.AgentIDs) != 0 || len(f.Tools) != 0 ||
			len(f.Clusters) != 0 || f.SourcePattern != "" {
			t.Errorf("expected zero-value filter, got %+v", f)
		}
	})
}

func TestDetectSchemaType(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{"map is json", map[string]any{"a": 1}, "json"},
		{"slice is json", []any{1, 2}, "json"},
		{"string", "hello", "string"},
		{"float", float64(3.14), "string"},
		{"int", 42, "string"},
		{"int64", int64(99), "string"},
		{"bool is binary", true, "binary"},
		{"bytes is binary", []byte{1, 2}, "binary"},
		{"nil is binary", nil, "binary"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := detectSchemaType(tc.value); got != tc.want {
				t.Errorf("got %q want %q", got, tc.want)
			}
		})
	}
}
