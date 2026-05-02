package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/config"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kadm"
)

// testManager builds a Manager with two clusters and no real broker connections.
func testManager() *Manager {
	return &Manager{
		clients: make(map[string]*kgo.Client),
		admins:  make(map[string]*kadm.Client),
		configs: map[string]*config.ClusterConfig{
			"prod":    {Name: "prod", BootstrapServers: "localhost:9092", Default: true},
			"staging": {Name: "staging", BootstrapServers: "localhost:9093"},
		},
		logger:    slog.Default(),
		defClust:  "prod",
		streamMgr: newStreamSessionManager(),
	}
}

// --------------- DefaultClusterName ---------------

func TestDefaultClusterName(t *testing.T) {
	t.Parallel()
	m := testManager()
	if got := m.DefaultClusterName(); got != "prod" {
		t.Errorf("DefaultClusterName() = %q, want %q", got, "prod")
	}
}

func TestDefaultClusterName_Empty(t *testing.T) {
	t.Parallel()
	m := &Manager{
		clients:  make(map[string]*kgo.Client),
		admins:   make(map[string]*kadm.Client),
		configs:  make(map[string]*config.ClusterConfig),
		logger:   slog.Default(),
		defClust: "",
	}
	if got := m.DefaultClusterName(); got != "" {
		t.Errorf("DefaultClusterName() = %q, want empty", got)
	}
}

// --------------- ClusterNames ---------------

func TestClusterNames(t *testing.T) {
	t.Parallel()
	m := testManager()
	names := m.ClusterNames()
	sort.Strings(names)
	want := []string{"prod", "staging"}
	if len(names) != len(want) {
		t.Fatalf("ClusterNames() len = %d, want %d", len(names), len(want))
	}
	for i, n := range names {
		if n != want[i] {
			t.Errorf("ClusterNames()[%d] = %q, want %q", i, n, want[i])
		}
	}
}

func TestClusterNames_Empty(t *testing.T) {
	t.Parallel()
	m := &Manager{
		clients: make(map[string]*kgo.Client),
		admins:  make(map[string]*kadm.Client),
		configs: make(map[string]*config.ClusterConfig),
		logger:  slog.Default(),
	}
	if got := m.ClusterNames(); len(got) != 0 {
		t.Errorf("ClusterNames() = %v, want empty", got)
	}
}

// --------------- GetClusterConfig ---------------

func TestGetClusterConfig_Exists(t *testing.T) {
	t.Parallel()
	m := testManager()
	cfg, err := m.GetClusterConfig("staging")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Name != "staging" {
		t.Errorf("config name = %q, want %q", cfg.Name, "staging")
	}
	if cfg.BootstrapServers != "localhost:9093" {
		t.Errorf("bootstrap = %q, want %q", cfg.BootstrapServers, "localhost:9093")
	}
}

func TestGetClusterConfig_Default(t *testing.T) {
	t.Parallel()
	m := testManager()
	cfg, err := m.GetClusterConfig("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Name != "prod" {
		t.Errorf("config name = %q, want %q", cfg.Name, "prod")
	}
}

func TestGetClusterConfig_NotFound(t *testing.T) {
	t.Parallel()
	m := testManager()
	_, err := m.GetClusterConfig("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}

// --------------- resolveCluster ---------------

func TestResolveCluster_WithName(t *testing.T) {
	t.Parallel()
	m := testManager()
	if got := m.resolveCluster("staging"); got != "staging" {
		t.Errorf("resolveCluster(%q) = %q, want %q", "staging", got, "staging")
	}
}

func TestResolveCluster_Empty(t *testing.T) {
	t.Parallel()
	m := testManager()
	if got := m.resolveCluster(""); got != "prod" {
		t.Errorf("resolveCluster(%q) = %q, want %q", "", got, "prod")
	}
}

// --------------- Close ---------------

func TestClose_EmptyManager(t *testing.T) {
	t.Parallel()
	m := &Manager{
		clients: make(map[string]*kgo.Client),
		admins:  make(map[string]*kadm.Client),
		configs: make(map[string]*config.ClusterConfig),
		logger:  slog.Default(),
	}
	// Should not panic
	m.Close()
}

// --------------- GetClient / GetAdmin errors ---------------

func TestGetClient_NotFound(t *testing.T) {
	t.Parallel()
	m := testManager()
	_, err := m.GetClient("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}

func TestGetAdmin_NotFound(t *testing.T) {
	t.Parallel()
	m := testManager()
	_, err := m.GetAdmin("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}

func TestGetClient_Default(t *testing.T) {
	t.Parallel()
	// No clients registered, but default cluster name is set → still "not found"
	m := testManager()
	_, err := m.GetClient("")
	if err == nil {
		t.Fatal("expected error when no real client is stored")
	}
}

func TestGetAdmin_Default(t *testing.T) {
	t.Parallel()
	m := testManager()
	_, err := m.GetAdmin("")
	if err == nil {
		t.Fatal("expected error when no real admin client is stored")
	}
}

// --------------- ProduceMessage JSON ---------------

func TestProduceMessage_JSON(t *testing.T) {
	t.Parallel()
	p := int32(3)
	msg := ProduceMessage{
		Key:       "k1",
		Value:     map[string]string{"hello": "world"},
		Headers:   map[string]string{"h1": "v1"},
		Partition: &p,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var out ProduceMessage
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Key != "k1" {
		t.Errorf("key = %q, want %q", out.Key, "k1")
	}
	if out.Partition == nil || *out.Partition != 3 {
		t.Errorf("partition = %v, want 3", out.Partition)
	}
	if out.Headers["h1"] != "v1" {
		t.Errorf("headers[h1] = %q, want %q", out.Headers["h1"], "v1")
	}
}

func TestProduceMessage_OmitEmpty(t *testing.T) {
	t.Parallel()
	msg := ProduceMessage{Key: "k", Value: "v"}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	if _, ok := raw["partition"]; ok {
		t.Error("expected partition to be omitted when nil")
	}
	if _, ok := raw["headers"]; ok {
		t.Error("expected headers to be omitted when nil")
	}
}

// --------------- ProduceOptions JSON ---------------

func TestProduceOptions_JSON(t *testing.T) {
	t.Parallel()
	opts := ProduceOptions{Acks: "all", Serializer: "json", SchemaID: -1}
	data, err := json.Marshal(opts)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out ProduceOptions
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Acks != "all" {
		t.Errorf("acks = %q, want %q", out.Acks, "all")
	}
	if out.SchemaID != -1 {
		t.Errorf("schema_id = %d, want -1", out.SchemaID)
	}
}

// --------------- SubscribeRequest defaults ---------------

func TestSubscribeRequest_Defaults(t *testing.T) {
	t.Parallel()
	raw := `{"topic":"events"}`
	var req SubscribeRequest
	if err := json.Unmarshal([]byte(raw), &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if req.Topic != "events" {
		t.Errorf("topic = %q, want %q", req.Topic, "events")
	}
	if req.MaxMessages != 0 {
		t.Errorf("max_messages zero-value = %d, want 0", req.MaxMessages)
	}
	if req.AutoCommit {
		t.Error("auto_commit zero-value should be false")
	}
}

func TestSubscribeRequest_FullRoundTrip(t *testing.T) {
	t.Parallel()
	req := SubscribeRequest{
		Topic:        "orders",
		GroupID:      "grp-1",
		AutoCommit:   true,
		MaxMessages:  500,
		TimeoutMs:    5000,
		Deserializer: "json",
	}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out SubscribeRequest
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out != req {
		t.Errorf("round-trip mismatch: got %+v, want %+v", out, req)
	}
}

// --------------- SubscribeResult ---------------

func TestSubscribeResult_JSON(t *testing.T) {
	t.Parallel()
	res := SubscribeResult{
		Messages:   []Message{{Partition: 0, Offset: 10, Timestamp: "2024-01-01T00:00:00.000Z", Key: "k", Value: "v"}},
		Count:      1,
		Topic:      "t",
		GroupID:    "g",
		AutoCommit: true,
	}
	data, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out SubscribeResult
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Count != 1 || out.GroupID != "g" || !out.AutoCommit {
		t.Errorf("unexpected result: %+v", out)
	}
}

// --------------- Core I/O error paths ---------------

func TestConsume_ClusterNotFound(t *testing.T) {
	t.Parallel()
	m := testManager()
	_, err := m.Consume(t.Context(), "nonexistent", "topic", -1, "earliest", 10, 5000, "auto")
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}

func TestProduce_ClusterNotFound(t *testing.T) {
	t.Parallel()
	m := testManager()
	_, err := m.Produce(t.Context(), "nonexistent", "topic", []ProduceMessage{{Value: "v"}}, ProduceOptions{})
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}

func TestProduce_ClusterNotFound_Acks0(t *testing.T) {
	t.Parallel()
	m := testManager()
	_, err := m.Produce(t.Context(), "nonexistent", "topic", []ProduceMessage{{Value: "v"}}, ProduceOptions{Acks: "0"})
	if err == nil {
		t.Fatal("expected error for nonexistent cluster with acks=0")
	}
}

func TestListTopics_ClusterNotFound(t *testing.T) {
	t.Parallel()
	m := testManager()
	_, err := m.ListTopics(t.Context(), "nonexistent", false)
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}

func TestDescribeTopic_ClusterNotFound(t *testing.T) {
	t.Parallel()
	m := testManager()
	_, err := m.DescribeTopic(t.Context(), "nonexistent", "my-topic")
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}

func TestListConsumerGroups_ClusterNotFound(t *testing.T) {
	t.Parallel()
	m := testManager()
	_, err := m.ListConsumerGroups(t.Context(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}

func TestDescribeConsumerGroup_ClusterNotFound(t *testing.T) {
	t.Parallel()
	m := testManager()
	_, err := m.DescribeConsumerGroup(t.Context(), "nonexistent", "my-group")
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}

func TestGetClusterMetadata_ClusterNotFound(t *testing.T) {
	t.Parallel()
	m := testManager()
	_, err := m.GetClusterMetadata(t.Context(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}

func TestPing_NoAdminClient(t *testing.T) {
	t.Parallel()
	m := testManager()
	err := m.Ping(t.Context())
	if err == nil {
		t.Fatal("expected error when no admin client")
	}
}

func TestSubscribe_ClusterNotFound(t *testing.T) {
	t.Parallel()
	m := testManager()
	_, err := m.Subscribe(t.Context(), "nonexistent", SubscribeRequest{Topic: "test"})
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}

// --------------- resolveCluster ---------------

func TestResolveCluster_DefaultFallback(t *testing.T) {
	t.Parallel()
	m := testManager()
	if got := m.resolveCluster(""); got != "prod" {
		t.Errorf("expected 'prod', got %q", got)
	}
}

// --------------- MockKafkaManager ---------------

func TestMockKafkaManager_DefaultBehavior(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{}
	if got := mock.DefaultClusterName(); got != "default" {
		t.Errorf("DefaultClusterName() = %q, want %q", got, "default")
	}
	names := mock.ClusterNames()
	if len(names) != 1 || names[0] != "default" {
		t.Errorf("ClusterNames() = %v, want [default]", names)
	}
	cfg, err := mock.GetClusterConfig("test")
	if err != nil || cfg.Name != "default" {
		t.Errorf("GetClusterConfig() unexpected: %v, %v", cfg, err)
	}
	if err := mock.Ping(t.Context()); err != nil {
		t.Errorf("Ping() unexpected error: %v", err)
	}
	mock.Close()
	if mock.CallCount("Close") != 1 {
		t.Error("Close not recorded")
	}
}

func TestMockKafkaManager_CustomFunctions(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]TopicSummary, error) {
			return []TopicSummary{
				{Name: "orders", Partitions: 3, ReplicationFactor: 2},
				{Name: "events", Partitions: 6, ReplicationFactor: 3},
			}, nil
		},
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]Message, error) {
			return []Message{
				{Partition: 0, Offset: 42, Key: "k1", Value: "v1"},
			}, nil
		},
	}

	topics, err := mock.ListTopics(t.Context(), "prod", false)
	if err != nil || len(topics) != 2 {
		t.Fatalf("ListTopics() = %v, %v; want 2 topics", topics, err)
	}
	if topics[0].Name != "orders" {
		t.Errorf("first topic = %q, want 'orders'", topics[0].Name)
	}

	msgs, err := mock.Consume(t.Context(), "prod", "orders", 0, "earliest", 10, 5000, "auto")
	if err != nil || len(msgs) != 1 {
		t.Fatalf("Consume() = %v, %v; want 1 message", msgs, err)
	}

	if mock.CallCount("ListTopics") != 1 {
		t.Error("expected 1 ListTopics call")
	}
	if mock.CallCount("Consume") != 1 {
		t.Error("expected 1 Consume call")
	}
}

func TestMockKafkaManager_CallRecording(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{}
	mock.ListTopics(t.Context(), "prod", true)
	mock.ListTopics(t.Context(), "staging", false)
	mock.Consume(t.Context(), "prod", "test", 0, "latest", 10, 5000, "auto")

	if mock.CallCount("ListTopics") != 2 {
		t.Errorf("expected 2 ListTopics calls, got %d", mock.CallCount("ListTopics"))
	}
	last := mock.LastCall("ListTopics")
	if last == nil || last.Args[0] != "staging" {
		t.Error("LastCall should return the staging call")
	}
}

func TestMockKafkaManager_ConcurrentSafety(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{}
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			mock.ListTopics(t.Context(), "prod", false)
			mock.Consume(t.Context(), "prod", "test", 0, "latest", 10, 5000, "auto")
			done <- true
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
	if mock.CallCount("ListTopics") != 10 {
		t.Errorf("expected 10 ListTopics calls, got %d", mock.CallCount("ListTopics"))
	}
}
