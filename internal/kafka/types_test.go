package kafka

import (
	"encoding/json"
	"testing"
)

// --------------- TopicSummary ---------------

func TestTopicSummary_JSON(t *testing.T) {
	ts := TopicSummary{
		Name:              "orders",
		Partitions:        6,
		ReplicationFactor: 3,
		RetentionMs:       604800000,
		IsInternal:        false,
		MessageCount:      42000,
		SchemaSubject:     "orders-value",
	}
	data, err := json.Marshal(ts)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out TopicSummary
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out != ts {
		t.Errorf("round-trip mismatch:\n got %+v\nwant %+v", out, ts)
	}
}

func TestTopicSummary_OmitEmpty(t *testing.T) {
	ts := TopicSummary{Name: "t", Partitions: 1}
	data, err := json.Marshal(ts)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	if _, ok := raw["retention_ms"]; ok {
		t.Error("retention_ms should be omitted when zero")
	}
	if _, ok := raw["schema_subject"]; ok {
		t.Error("schema_subject should be omitted when empty")
	}
}

// --------------- TopicDetail ---------------

func TestTopicDetail_JSON(t *testing.T) {
	td := TopicDetail{
		Name: "events",
		Partitions: []PartitionDetail{
			{ID: 0, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1, 2}, EarliestOffset: 0, LatestOffset: 100},
		},
		ReplicationFactor: 2,
		Configs:           map[string]string{"retention.ms": "86400000"},
		IsInternal:        false,
		MessageCount:      100,
	}
	data, err := json.Marshal(td)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out TopicDetail
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Name != "events" || len(out.Partitions) != 1 {
		t.Errorf("unexpected: %+v", out)
	}
	if out.Partitions[0].LatestOffset != 100 {
		t.Errorf("latest offset = %d, want 100", out.Partitions[0].LatestOffset)
	}
	if out.Configs["retention.ms"] != "86400000" {
		t.Errorf("config mismatch: %v", out.Configs)
	}
}

// --------------- PartitionDetail ---------------

func TestPartitionDetail_JSON(t *testing.T) {
	pd := PartitionDetail{
		ID: 2, Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2},
		EarliestOffset: 50, LatestOffset: 200,
	}
	data, err := json.Marshal(pd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out PartitionDetail
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.ID != 2 || out.Leader != 1 || len(out.Replicas) != 3 || len(out.ISR) != 2 {
		t.Errorf("unexpected: %+v", out)
	}
}

// --------------- ConsumerGroupSummary ---------------

func TestConsumerGroupSummary_JSON(t *testing.T) {
	cg := ConsumerGroupSummary{
		GroupID:     "my-group",
		State:       "Stable",
		MemberCount: 3,
		Protocol:    "range",
		TotalLag:    1500,
	}
	data, err := json.Marshal(cg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out ConsumerGroupSummary
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out != cg {
		t.Errorf("round-trip mismatch:\n got %+v\nwant %+v", out, cg)
	}
}

// --------------- ConsumerGroupDetail ---------------

func TestConsumerGroupDetail_JSON(t *testing.T) {
	detail := ConsumerGroupDetail{
		GroupID:      "grp",
		State:        "Stable",
		ProtocolType: "consumer",
		Protocol:     "range",
		Coordinator:  BrokerInfo{ID: 1, Host: "broker-1", Port: 9092},
		Members: []GroupMemberDetail{
			{MemberID: "m1", ClientID: "c1", Host: "/10.0.0.1",
				Assignment: []TopicPartitions{{Topic: "t1", Partitions: []int32{0, 1}}}},
		},
		Offsets: []GroupOffsetDetail{
			{Topic: "t1", Partition: 0, CurrentOffset: 50, LogEndOffset: 100, Lag: 50},
		},
		TotalLag: 50,
	}
	data, err := json.Marshal(detail)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out ConsumerGroupDetail
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.GroupID != "grp" || len(out.Members) != 1 || len(out.Offsets) != 1 {
		t.Errorf("unexpected: %+v", out)
	}
	if out.Members[0].Assignment[0].Topic != "t1" {
		t.Errorf("assignment topic = %q, want %q", out.Members[0].Assignment[0].Topic, "t1")
	}
}

// --------------- BrokerInfo ---------------

func TestBrokerInfo_JSON(t *testing.T) {
	bi := BrokerInfo{ID: 0, Host: "kafka-0", Port: 9092, Rack: "us-east-1a"}
	data, err := json.Marshal(bi)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out BrokerInfo
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out != bi {
		t.Errorf("round-trip mismatch:\n got %+v\nwant %+v", out, bi)
	}
}

func TestBrokerInfo_OmitRack(t *testing.T) {
	bi := BrokerInfo{ID: 1, Host: "kafka-1", Port: 9092}
	data, err := json.Marshal(bi)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	if _, ok := raw["rack"]; ok {
		t.Error("rack should be omitted when empty")
	}
}

// --------------- GroupMemberDetail ---------------

func TestGroupMemberDetail_JSON(t *testing.T) {
	md := GroupMemberDetail{
		MemberID: "m1", ClientID: "c1", Host: "/10.0.0.1",
		Assignment: []TopicPartitions{{Topic: "t", Partitions: []int32{0}}},
	}
	data, err := json.Marshal(md)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out GroupMemberDetail
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.MemberID != "m1" || len(out.Assignment) != 1 {
		t.Errorf("unexpected: %+v", out)
	}
}

// --------------- TopicPartitions ---------------

func TestTopicPartitions_JSON(t *testing.T) {
	tp := TopicPartitions{Topic: "events", Partitions: []int32{0, 1, 2}}
	data, err := json.Marshal(tp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out TopicPartitions
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Topic != "events" || len(out.Partitions) != 3 {
		t.Errorf("unexpected: %+v", out)
	}
}

// --------------- GroupOffsetDetail ---------------

func TestGroupOffsetDetail_JSON(t *testing.T) {
	god := GroupOffsetDetail{
		Topic: "t1", Partition: 2, CurrentOffset: 100, LogEndOffset: 200, Lag: 100,
	}
	data, err := json.Marshal(god)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out GroupOffsetDetail
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out != god {
		t.Errorf("round-trip mismatch:\n got %+v\nwant %+v", out, god)
	}
}

// --------------- ClusterMetadata ---------------

func TestClusterMetadata_JSON(t *testing.T) {
	cm := ClusterMetadata{
		ClusterID:      "abc-123",
		Controller:     BrokerInfo{ID: 0, Host: "ctrl", Port: 9092},
		Brokers:        []BrokerInfo{{ID: 0, Host: "ctrl", Port: 9092}},
		BrokerCount:    1,
		TopicCount:     10,
		PartitionCount: 30,
		KraftMode:      true,
	}
	data, err := json.Marshal(cm)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out ClusterMetadata
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.ClusterID != "abc-123" || !out.KraftMode || out.BrokerCount != 1 {
		t.Errorf("unexpected: %+v", out)
	}
}

// --------------- Message ---------------

func TestMessage_JSON(t *testing.T) {
	msg := Message{
		Partition: 0,
		Offset:    42,
		Timestamp: "2024-06-15T10:30:00.000Z",
		Key:       "order-123",
		Value:     map[string]any{"status": "shipped"},
		Headers:   map[string]string{"trace-id": "abc"},
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out Message
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Partition != 0 || out.Offset != 42 {
		t.Errorf("unexpected: %+v", out)
	}
	if out.Headers["trace-id"] != "abc" {
		t.Errorf("header mismatch: %v", out.Headers)
	}
}

func TestMessage_OmitHeaders(t *testing.T) {
	msg := Message{Partition: 0, Offset: 0, Timestamp: "t", Key: nil, Value: nil}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	if _, ok := raw["headers"]; ok {
		t.Error("headers should be omitted when nil")
	}
}

func TestMessage_NullKeyValue(t *testing.T) {
	raw := `{"partition":0,"offset":0,"timestamp":"t","key":null,"value":null}`
	var msg Message
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if msg.Key != nil {
		t.Errorf("key = %v, want nil", msg.Key)
	}
	if msg.Value != nil {
		t.Errorf("value = %v, want nil", msg.Value)
	}
}

// --------------- DeliveryResult ---------------

func TestDeliveryResult_JSON(t *testing.T) {
	dr := DeliveryResult{
		Partition: 1,
		Offset:    999,
		Timestamp: "2024-01-01T00:00:00.000Z",
		Status:    "delivered",
	}
	data, err := json.Marshal(dr)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out DeliveryResult
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out != dr {
		t.Errorf("round-trip mismatch:\n got %+v\nwant %+v", out, dr)
	}
}

func TestDeliveryResult_ErrorStatus(t *testing.T) {
	dr := DeliveryResult{Status: "error: delivery failed"}
	data, err := json.Marshal(dr)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out DeliveryResult
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Status != "error: delivery failed" {
		t.Errorf("status = %q, want %q", out.Status, "error: delivery failed")
	}
}

// --------------- StreamConfig ---------------

func TestStreamConfig_JSON(t *testing.T) {
	sc := StreamConfig{
		Topic:        "logs",
		GroupID:      "g1",
		BatchSize:    50,
		MaxMsgPerSec: 500,
		MaxMessages:  2000,
		TimeoutMs:    15000,
		Deserializer: "json",
		AutoCommit:   true,
	}
	data, err := json.Marshal(sc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out StreamConfig
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out != sc {
		t.Errorf("round-trip mismatch:\n got %+v\nwant %+v", out, sc)
	}
}

// --------------- StreamResult ---------------

func TestStreamResult_JSON(t *testing.T) {
	sr := StreamResult{
		SessionID: "stream-abc",
		Messages:  []Message{{Partition: 0, Offset: 1, Timestamp: "t", Key: "k", Value: "v"}},
		Count:     1,
		Status:    "running",
		Topic:     "t1",
		GroupID:   "g1",
		TotalSent: 100,
	}
	data, err := json.Marshal(sr)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out StreamResult
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.SessionID != "stream-abc" || out.Count != 1 || out.TotalSent != 100 {
		t.Errorf("unexpected: %+v", out)
	}
}
