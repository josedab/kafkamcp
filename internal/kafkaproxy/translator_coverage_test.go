package kafkaproxy

import (
	"encoding/binary"
	"log/slog"
	"sync"
	"testing"
)

func newTestTranslator(policies ...Policy) *Translator {
	cfg := TranslatorConfig{
		EnableAudit: true,
		Policies:    policies,
	}
	return NewTranslator(cfg, slog.Default())
}

// --- translateProduce tests ---

func TestTranslateProduce_SingleTopic(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator()
	body := append([]byte{0, 6}, []byte("orders")...)
	hdr := &RequestHeader{APIKey: APIKeyProduce, ClientID: "p1"}
	result, err := tr.Translate(hdr, body)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if result.Tool != "kafka_produce" {
		t.Errorf("Tool = %q, want kafka_produce", result.Tool)
	}
	if result.Args["topic"] != "orders" {
		t.Errorf("topic = %v, want orders", result.Args["topic"])
	}
}

func TestTranslateProduce_EmptyBody(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator()
	hdr := &RequestHeader{APIKey: APIKeyProduce, ClientID: "p1"}
	result, err := tr.Translate(hdr, nil)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if result.Tool != "kafka_produce" {
		t.Errorf("Tool = %q, want kafka_produce", result.Tool)
	}
	if _, ok := result.Args["topic"]; ok {
		t.Error("expected no topic for empty body")
	}
}

func TestTranslateProduce_MalformedBinary(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator()
	hdr := &RequestHeader{APIKey: APIKeyProduce, ClientID: "p1"}
	result, err := tr.Translate(hdr, []byte{0xFF, 0xFF})
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if result.Tool != "kafka_produce" {
		t.Errorf("Tool = %q, want kafka_produce", result.Tool)
	}
}

// --- translateFetch tests ---

func TestTranslateFetch_ValidTopic(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator()
	// Fetch body: replica_id(4) + max_wait(4) + min_bytes(4) + topic_name
	body := make([]byte, 12)
	body = append(body, 0, 5)
	body = append(body, []byte("topic")...)
	hdr := &RequestHeader{APIKey: APIKeyFetch, ClientID: "c1"}
	result, err := tr.Translate(hdr, body)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if result.Tool != "kafka_consume" {
		t.Errorf("Tool = %q, want kafka_consume", result.Tool)
	}
	if result.Args["topic"] != "topic" {
		t.Errorf("topic = %v, want topic", result.Args["topic"])
	}
}

func TestTranslateFetch_TruncatedBody(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator()
	hdr := &RequestHeader{APIKey: APIKeyFetch, ClientID: "c1"}
	result, err := tr.Translate(hdr, []byte{0, 0, 0, 1})
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if result.Tool != "kafka_consume" {
		t.Errorf("Tool = %q, want kafka_consume", result.Tool)
	}
	if _, ok := result.Args["topic"]; ok {
		t.Error("expected no topic for truncated body")
	}
}

// --- translateMetadata tests ---

func TestTranslateMetadata_TopicList(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator()
	// topic_count=1 + topic name
	body := make([]byte, 4)
	binary.BigEndian.PutUint32(body, 1)
	body = append(body, 0, 8)
	body = append(body, []byte("my-topic")...)
	hdr := &RequestHeader{APIKey: APIKeyMetadata, ClientID: "c1"}
	result, err := tr.Translate(hdr, body)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if result.Args["filter"] != "my-topic" {
		t.Errorf("filter = %v, want my-topic", result.Args["filter"])
	}
}

func TestTranslateMetadata_AllTopics(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator()
	body := make([]byte, 4)
	binary.BigEndian.PutUint32(body, 0) // 0 = all topics
	hdr := &RequestHeader{APIKey: APIKeyMetadata, ClientID: "c1"}
	result, err := tr.Translate(hdr, body)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if _, ok := result.Args["filter"]; ok {
		t.Error("expected no filter for all-topics metadata request")
	}
}

func TestTranslateMetadata_NegativeTopicCount(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator()
	body := make([]byte, 4)
	binary.BigEndian.PutUint32(body, 0xFFFFFFFF) // -1
	hdr := &RequestHeader{APIKey: APIKeyMetadata, ClientID: "c1"}
	result, err := tr.Translate(hdr, body)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if _, ok := result.Args["filter"]; ok {
		t.Error("expected no filter for -1 topic count")
	}
}

// --- translateFindCoordinator tests ---

func TestTranslateFindCoordinator_GroupID(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator()
	body := append([]byte{0, 8}, []byte("my-group")...)
	hdr := &RequestHeader{APIKey: APIKeyFindCoordinator, ClientID: "c1"}
	result, err := tr.Translate(hdr, body)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if result.Args["group_id"] != "my-group" {
		t.Errorf("group_id = %v, want my-group", result.Args["group_id"])
	}
}

// --- translateJoinGroup tests ---

func TestTranslateJoinGroup_GroupID(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator()
	body := append([]byte{0, 10}, []byte("join-group")...)
	hdr := &RequestHeader{APIKey: APIKeyJoinGroup, ClientID: "c1"}
	result, err := tr.Translate(hdr, body)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if result.Args["group_id"] != "join-group" {
		t.Errorf("group_id = %v, want join-group", result.Args["group_id"])
	}
}

// --- translateListOffsets tests ---

func TestTranslateListOffsets_Valid(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator()
	// replica_id(4) + isolation_level(1) + topic_count(4) + topic_name
	body := make([]byte, 9)
	body = append(body, 0, 6)
	body = append(body, []byte("events")...)
	hdr := &RequestHeader{APIKey: APIKeyListOffsets, ClientID: "c1"}
	result, err := tr.Translate(hdr, body)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if result.Args["topic"] != "events" {
		t.Errorf("topic = %v, want events", result.Args["topic"])
	}
}

func TestTranslateListOffsets_Malformed(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator()
	hdr := &RequestHeader{APIKey: APIKeyListOffsets, ClientID: "c1"}
	result, err := tr.Translate(hdr, []byte{0, 0})
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if _, ok := result.Args["topic"]; ok {
		t.Error("expected no topic for malformed input")
	}
}

// --- translateOffsetFetch tests ---

func TestTranslateOffsetFetch_Valid(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator()
	body := append([]byte{0, 7}, []byte("grp-abc")...)
	hdr := &RequestHeader{APIKey: APIKeyOffsetFetch, ClientID: "c1"}
	result, err := tr.Translate(hdr, body)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if result.Args["group_id"] != "grp-abc" {
		t.Errorf("group_id = %v, want grp-abc", result.Args["group_id"])
	}
}

func TestTranslateOffsetFetch_Malformed(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator()
	hdr := &RequestHeader{APIKey: APIKeyOffsetFetch, ClientID: "c1"}
	result, err := tr.Translate(hdr, []byte{0})
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if _, ok := result.Args["group_id"]; ok {
		t.Error("expected no group_id for malformed input")
	}
}

// --- evaluatePolicy tests ---

func TestEvaluatePolicy_DenyRule(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator(Policy{
		Name: "deny-produce", APIKeys: []int16{APIKeyProduce}, Action: PolicyDeny,
	})
	hdr := &RequestHeader{APIKey: APIKeyProduce, ClientID: "test"}
	_, err := tr.Translate(hdr, nil)
	if err == nil {
		t.Fatal("expected deny error")
	}
	stats := tr.Stats()
	if stats.DeniedByPolicy != 1 {
		t.Errorf("DeniedByPolicy = %d, want 1", stats.DeniedByPolicy)
	}
}

func TestEvaluatePolicy_AllowRule(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator(Policy{
		Name: "allow-produce", APIKeys: []int16{APIKeyProduce}, Action: PolicyAllow,
	})
	hdr := &RequestHeader{APIKey: APIKeyProduce, ClientID: "test"}
	result, err := tr.Translate(hdr, nil)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if !result.Translated {
		t.Error("expected translated result")
	}
}

func TestEvaluatePolicy_ClientIDMatch(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator(Policy{
		Name:      "deny-specific-client",
		APIKeys:   []int16{APIKeyProduce},
		ClientIDs: []string{"bad-client"},
		Action:    PolicyDeny,
	})

	// Non-matching client should be allowed
	hdr := &RequestHeader{APIKey: APIKeyProduce, ClientID: "good-client"}
	_, err := tr.Translate(hdr, nil)
	if err != nil {
		t.Fatalf("expected allow for non-matching client: %v", err)
	}

	// Matching client should be denied
	hdr2 := &RequestHeader{APIKey: APIKeyProduce, ClientID: "bad-client"}
	_, err = tr.Translate(hdr2, nil)
	if err == nil {
		t.Fatal("expected deny for matching client")
	}
}

func TestEvaluatePolicy_NoMatchingPolicy(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator(Policy{
		Name: "deny-fetch", APIKeys: []int16{APIKeyFetch}, Action: PolicyDeny,
	})
	hdr := &RequestHeader{APIKey: APIKeyProduce, ClientID: "test"}
	_, err := tr.Translate(hdr, nil)
	if err != nil {
		t.Fatalf("expected allow when no policy matches: %v", err)
	}
}

func TestEvaluatePolicy_NoPolicies(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator()
	hdr := &RequestHeader{APIKey: APIKeyProduce, ClientID: "test"}
	_, err := tr.Translate(hdr, nil)
	if err != nil {
		t.Fatalf("expected allow with no policies: %v", err)
	}
}

// --- Translate dispatcher tests ---

func TestTranslate_AllAPIKeys(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator()
	tests := []struct {
		name     string
		apiKey   int16
		wantTool string
		wantXlat bool
	}{
		{"produce", APIKeyProduce, "kafka_produce", true},
		{"fetch", APIKeyFetch, "kafka_consume", true},
		{"metadata", APIKeyMetadata, "kafka_list_topics", true},
		{"find_coordinator", APIKeyFindCoordinator, "kafka_describe_consumer_group", true},
		{"list_offsets", APIKeyListOffsets, "kafka_list_offsets", true},
		{"offset_commit", APIKeyOffsetCommit, "kafka_commit_offsets", true},
		{"offset_fetch", APIKeyOffsetFetch, "kafka_fetch_offsets", true},
		{"join_group", APIKeyJoinGroup, "kafka_consumer_group_join", true},
		{"sync_group", APIKeySyncGroup, "kafka_consumer_group_sync", true},
		{"api_versions", APIKeyAPIVersions, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hdr := &RequestHeader{APIKey: tt.apiKey, ClientID: "test"}
			result, err := tr.Translate(hdr, nil)
			if err != nil {
				t.Fatalf("Translate: %v", err)
			}
			if result.Translated != tt.wantXlat {
				t.Errorf("Translated = %v, want %v", result.Translated, tt.wantXlat)
			}
			if result.Tool != tt.wantTool {
				t.Errorf("Tool = %q, want %q", result.Tool, tt.wantTool)
			}
		})
	}
}

func TestTranslate_UnknownAPIKey(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator()
	hdr := &RequestHeader{APIKey: 99, ClientID: "test"}
	result, err := tr.Translate(hdr, nil)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if result.Translated {
		t.Error("expected passthrough for unknown API key")
	}
}

func TestTranslate_DeniedRequest(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator(Policy{
		Name: "deny-all-produce", APIKeys: []int16{APIKeyProduce}, Action: PolicyDeny,
	})
	hdr := &RequestHeader{APIKey: APIKeyProduce, ClientID: "test"}
	_, err := tr.Translate(hdr, nil)
	if err == nil {
		t.Fatal("expected error for denied request")
	}
}

// --- Stats thread safety ---

func TestStats_ThreadSafety(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator()
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			apiKey := int16(idx % 4) // Produce, Fetch, ListOffsets, Metadata
			hdr := &RequestHeader{APIKey: apiKey, ClientID: "client"}
			_, _ = tr.Translate(hdr, nil)
		}(i)
	}

	// Concurrent stats reads
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tr.Stats()
		}()
	}

	wg.Wait()

	stats := tr.Stats()
	if stats.TotalRequests != 50 {
		t.Errorf("TotalRequests = %d, want 50", stats.TotalRequests)
	}
}

func TestStats_ByAPIKeyAndClient(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator()

	hdr := &RequestHeader{APIKey: APIKeyProduce, ClientID: "client-A"}
	_, _ = tr.Translate(hdr, nil)
	_, _ = tr.Translate(hdr, nil)

	hdr2 := &RequestHeader{APIKey: APIKeyFetch, ClientID: "client-B"}
	_, _ = tr.Translate(hdr2, nil)

	stats := tr.Stats()
	if stats.ByAPIKey[APIKeyProduce] != 2 {
		t.Errorf("ByAPIKey[Produce] = %d, want 2", stats.ByAPIKey[APIKeyProduce])
	}
	if stats.ByAPIKey[APIKeyFetch] != 1 {
		t.Errorf("ByAPIKey[Fetch] = %d, want 1", stats.ByAPIKey[APIKeyFetch])
	}
	if stats.ByClient["client-A"] != 2 {
		t.Errorf("ByClient[client-A] = %d, want 2", stats.ByClient["client-A"])
	}
	if stats.ByClient["client-B"] != 1 {
		t.Errorf("ByClient[client-B] = %d, want 1", stats.ByClient["client-B"])
	}
}

func TestTranslate_EmptyClientID(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator()
	hdr := &RequestHeader{APIKey: APIKeyProduce, ClientID: ""}
	result, err := tr.Translate(hdr, nil)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if !result.Translated {
		t.Error("expected translated result")
	}
	stats := tr.Stats()
	if _, ok := stats.ByClient[""]; ok {
		t.Error("empty client ID should not be tracked")
	}
}

func TestTranslate_Latency(t *testing.T) {
	t.Parallel()
	tr := newTestTranslator()
	hdr := &RequestHeader{APIKey: APIKeyProduce, ClientID: "test"}
	result, _ := tr.Translate(hdr, nil)
	if result.Latency < 0 {
		t.Errorf("Latency = %v, should be non-negative", result.Latency)
	}
}
