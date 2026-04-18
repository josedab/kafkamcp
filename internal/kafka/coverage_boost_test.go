package kafka

import (
	"encoding/json"
	"testing"
)

// --------------- Type serialization tests ---------------

func TestCreateTopicRequest_JSON(t *testing.T) {
	req := CreateTopicRequest{
		Name:              "orders",
		Partitions:        6,
		ReplicationFactor: 3,
		Configs:           map[string]string{"retention.ms": "86400000"},
	}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out CreateTopicRequest
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Name != req.Name || out.Partitions != req.Partitions || out.ReplicationFactor != req.ReplicationFactor {
		t.Errorf("round-trip mismatch: got %+v, want %+v", out, req)
	}
	if out.Configs["retention.ms"] != "86400000" {
		t.Errorf("expected retention.ms=86400000, got %s", out.Configs["retention.ms"])
	}
}

func TestCreateTopicRequest_OmitEmptyConfigs(t *testing.T) {
	req := CreateTopicRequest{Name: "t", Partitions: 1, ReplicationFactor: 1}
	data, _ := json.Marshal(req)
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	if _, ok := raw["configs"]; ok {
		t.Error("configs should be omitted when nil")
	}
}

func TestSearchRequest_JSON(t *testing.T) {
	req := SearchRequest{
		Topic:         "events",
		KeyFilter:     "user-123",
		ValueContains: "error",
		HeaderKey:     "source",
		HeaderValue:   "api",
		MaxScan:       5000,
		Limit:         100,
	}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out SearchRequest
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Topic != req.Topic || out.KeyFilter != req.KeyFilter {
		t.Errorf("round-trip mismatch: got %+v", out)
	}
	if out.HeaderKey != "source" {
		t.Errorf("expected HeaderKey=source, got %s", out.HeaderKey)
	}
}

func TestAggregateRequest_JSON(t *testing.T) {
	req := AggregateRequest{
		Topic:    "metrics",
		Field:    "cpu_usage",
		WindowMs: 300000,
		Function: "avg",
	}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out AggregateRequest
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out != req {
		t.Errorf("round-trip mismatch: got %+v, want %+v", out, req)
	}
}

func TestAggregateResult_JSON(t *testing.T) {
	res := AggregateResult{
		Topic:        "metrics",
		Field:        "cpu_usage",
		Function:     "avg",
		WindowMs:     300000,
		Value:        72.5,
		MessageCount: 150,
		WindowStart:  "2024-01-01T00:00:00Z",
		WindowEnd:    "2024-01-01T00:05:00Z",
	}
	data, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out AggregateResult
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Value != 72.5 || out.MessageCount != 150 {
		t.Errorf("round-trip mismatch: got %+v", out)
	}
}

func TestJoinRequest_JSON(t *testing.T) {
	req := JoinRequest{
		LeftTopic:  "orders",
		RightTopic: "payments",
		KeyField:   "order_id",
		WindowMs:   60000,
		Limit:      100,
	}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out JoinRequest
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out != req {
		t.Errorf("round-trip mismatch: got %+v, want %+v", out, req)
	}
}

func TestJoinResult_JSON(t *testing.T) {
	res := JoinResult{
		LeftCount:  10,
		RightCount: 8,
		MatchCount: 5,
		Joined: []JoinedMessage{
			{
				Key:        "order-1",
				Left:       map[string]any{"amount": 100},
				Right:      map[string]any{"status": "paid"},
				LeftTopic:  "orders",
				RightTopic: "payments",
				Timestamp:  "2024-01-01T00:00:00Z",
			},
		},
		WindowMs: 60000,
	}
	data, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out JoinResult
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.MatchCount != 5 || len(out.Joined) != 1 {
		t.Errorf("round-trip mismatch: got %+v", out)
	}
}

func TestRecommendation_JSON(t *testing.T) {
	rec := Recommendation{
		Type:       "partition_hotspot",
		Severity:   "warning",
		Topic:      "events",
		Message:    "Partition skew detected",
		Suggestion: "Use a better partitioning key",
		Confidence: 0.85,
	}
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out Recommendation
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Type != "partition_hotspot" || out.Confidence != 0.85 {
		t.Errorf("round-trip mismatch: got %+v", out)
	}
}

func TestRecommendationResult_JSON(t *testing.T) {
	res := RecommendationResult{
		Recommendations: []Recommendation{
			{Type: "idle_topic", Severity: "info", Topic: "stale", Message: "No messages", Suggestion: "Remove topic", Confidence: 0.8},
		},
		TopicsAnalyzed: 10,
		Cluster:        "production",
	}
	data, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out RecommendationResult
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.TopicsAnalyzed != 10 || len(out.Recommendations) != 1 {
		t.Errorf("round-trip mismatch: got %+v", out)
	}
}

func TestTraceResult_JSON(t *testing.T) {
	res := TraceResult{
		CorrelationID:  "abc-123",
		TopicsSearched: 5,
		TotalMessages:  3,
		Hops: []TraceHop{
			{Topic: "orders", Partition: 0, Offset: 100, Timestamp: "2024-01-01T00:00:00Z"},
			{Topic: "payments", Partition: 1, Offset: 200, Timestamp: "2024-01-01T00:01:00Z", Key: "order-1", Headers: map[string]string{"trace-id": "abc-123"}},
		},
		FlowGraph: []FlowEdge{
			{FromTopic: "orders", ToTopic: "payments", LatencyMs: 60000},
		},
	}
	data, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out TraceResult
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.CorrelationID != "abc-123" || len(out.Hops) != 2 || len(out.FlowGraph) != 1 {
		t.Errorf("round-trip mismatch: got %+v", out)
	}
}

func TestTraceHop_OmitEmpty(t *testing.T) {
	hop := TraceHop{Topic: "t", Partition: 0, Offset: 0, Timestamp: "now"}
	data, _ := json.Marshal(hop)
	var raw map[string]json.RawMessage
	json.Unmarshal(data, &raw)
	if _, ok := raw["key"]; ok {
		t.Error("key should be omitted when nil")
	}
	if _, ok := raw["headers"]; ok {
		t.Error("headers should be omitted when nil")
	}
}

func TestFlowEdge_JSON(t *testing.T) {
	edge := FlowEdge{FromTopic: "a", ToTopic: "b", LatencyMs: 500}
	data, _ := json.Marshal(edge)
	var out FlowEdge
	json.Unmarshal(data, &out)
	if out != edge {
		t.Errorf("round-trip mismatch: got %+v, want %+v", out, edge)
	}
}

func TestConfigCompareResult_JSON(t *testing.T) {
	res := ConfigCompareResult{
		SourceCluster: "prod",
		TargetCluster: "staging",
		DiffCount:     2,
		MatchCount:    8,
		Diffs: []ConfigDiff{
			{Topic: "orders", Diffs: []ConfigValueDiff{{Key: "retention.ms", SourceValue: "86400000", TargetValue: "3600000"}}},
			{Topic: "events", OnlyIn: "source"},
		},
	}
	data, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out ConfigCompareResult
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.DiffCount != 2 || out.MatchCount != 8 || len(out.Diffs) != 2 {
		t.Errorf("round-trip mismatch: got %+v", out)
	}
}

func TestConfigDiff_OnlyIn(t *testing.T) {
	diff := ConfigDiff{Topic: "orphan", OnlyIn: "target"}
	data, _ := json.Marshal(diff)
	var out ConfigDiff
	json.Unmarshal(data, &out)
	if out.OnlyIn != "target" {
		t.Errorf("expected OnlyIn=target, got %s", out.OnlyIn)
	}
}

func TestConfigValueDiff_JSON(t *testing.T) {
	d := ConfigValueDiff{Key: "retention.ms", SourceValue: "100", TargetValue: "200"}
	data, _ := json.Marshal(d)
	var out ConfigValueDiff
	json.Unmarshal(data, &out)
	if out != d {
		t.Errorf("round-trip mismatch: got %+v, want %+v", out, d)
	}
}

func TestConfigAuditResult_JSON(t *testing.T) {
	res := ConfigAuditResult{
		Cluster:      "prod",
		Compliant:    8,
		NonCompliant: 2,
		Deviations: []ConfigDeviation{
			{Topic: "orders", Key: "retention.ms", Expected: "86400000", Actual: "3600000",
				Remediation: "kafka-configs --alter --entity-type topics --entity-name orders --add-config retention.ms=86400000"},
		},
	}
	data, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out ConfigAuditResult
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Compliant != 8 || out.NonCompliant != 2 || len(out.Deviations) != 1 {
		t.Errorf("round-trip mismatch: got %+v", out)
	}
}

func TestTimeTravelSnapshot_JSON(t *testing.T) {
	snap := TimeTravelSnapshot{
		Topic:     "orders",
		Timestamp: "2024-01-01T00:00:00Z",
		KeyCount:  5,
		State:     map[string]any{"key1": "val1"},
		ScanInfo: TimeTravelScanInfo{
			MessagesScanned: 100,
			Duration:        "50ms",
		},
	}
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out TimeTravelSnapshot
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Topic != "orders" || out.KeyCount != 5 {
		t.Errorf("round-trip mismatch: got %+v", out)
	}
}

func TestTimeTravelDiffResult_JSON(t *testing.T) {
	diff := TimeTravelDiffResult{
		Topic:        "orders",
		From:         "2024-01-01T00:00:00Z",
		To:           "2024-01-02T00:00:00Z",
		TotalChanges: 3,
		Added:        map[string]any{"new-key": "new-val"},
		Modified:     []TimeTravelModified{{Key: "key1", OldValue: "old", NewValue: "new"}},
		Deleted:      []string{"deleted-key"},
	}
	data, err := json.Marshal(diff)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out TimeTravelDiffResult
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.TotalChanges != 3 || len(out.Modified) != 1 || len(out.Deleted) != 1 {
		t.Errorf("round-trip mismatch: got %+v", out)
	}
}

func TestNaturalLanguageQueryResult_JSON(t *testing.T) {
	res := NaturalLanguageQueryResult{
		InterpretedAs: "Consume 10 messages from 'orders'",
		Topic:         "orders",
		Filter:        "error",
		Messages:      []Message{{Partition: 0, Offset: 1, Value: "test"}},
		Count:         1,
	}
	data, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out NaturalLanguageQueryResult
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Topic != "orders" || out.Count != 1 {
		t.Errorf("round-trip mismatch: got %+v", out)
	}
}

// --------------- AnalyzeDLQResult type ---------------

func TestAnalyzeDLQResult_JSON(t *testing.T) {
	res := AnalyzeDLQResult{
		Topic:         "orders-dlq",
		TotalMessages: 50,
		ErrorTypes:    map[string]int{"timeout": 30, "validation": 20},
		Samples: []Message{
			{Partition: 0, Offset: 10, Value: "sample"},
		},
	}
	data, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out AnalyzeDLQResult
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.TotalMessages != 50 || out.ErrorTypes["timeout"] != 30 {
		t.Errorf("round-trip mismatch: got %+v", out)
	}
}

// --------------- StreamConfig and StreamResult types ---------------

func TestStreamConfig_JSONRoundTrip(t *testing.T) {
	cfg := StreamConfig{
		Topic:        "events",
		GroupID:      "my-group",
		BatchSize:    50,
		MaxMsgPerSec: 1000,
		MaxMessages:  5000,
		TimeoutMs:    30000,
		Deserializer: "json",
		AutoCommit:   true,
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out StreamConfig
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Topic != "events" || out.BatchSize != 50 || !out.AutoCommit {
		t.Errorf("round-trip mismatch: got %+v", out)
	}
}

// --------------- correlationHeaders ---------------

func TestCorrelationHeadersList(t *testing.T) {
	if len(correlationHeaders) == 0 {
		t.Fatal("correlationHeaders should not be empty")
	}
	seen := make(map[string]bool)
	for _, h := range correlationHeaders {
		if h == "" {
			t.Error("empty correlation header")
		}
		if seen[h] {
			t.Errorf("duplicate correlation header: %s", h)
		}
		seen[h] = true
	}
}

// --------------- dangerousConfigKeys ---------------

func TestDangerousConfigKeysNotEmpty(t *testing.T) {
	if len(dangerousConfigKeys) == 0 {
		t.Fatal("dangerousConfigKeys should not be empty")
	}
	for _, k := range dangerousConfigKeys {
		if k == "" {
			t.Error("empty dangerous config key")
		}
	}
}

// --------------- ProduceMessage / ProduceOptions ---------------

func TestProduceMessage_JSONRoundTrip(t *testing.T) {
	msg := ProduceMessage{
		Key:     "user-123",
		Value:   map[string]any{"action": "click"},
		Headers: map[string]string{"source": "api"},
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out ProduceMessage
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Key != "user-123" {
		t.Errorf("expected Key=user-123, got %s", out.Key)
	}
}

func TestProduceOptions_JSONRoundTrip(t *testing.T) {
	opts := ProduceOptions{
		Acks:       "all",
		Serializer: "json",
		SchemaID:   42,
	}
	data, err := json.Marshal(opts)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out ProduceOptions
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Serializer != "json" || out.Acks != "all" || out.SchemaID != 42 {
		t.Errorf("round-trip mismatch: got %+v", out)
	}
}
