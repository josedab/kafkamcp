package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

// ===================== handleAutopilotAssess =====================

func TestHandleAutopilotAssess_ValidClusters(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{
				{Name: "orders", Partitions: 3, ReplicationFactor: 3},
				{Name: "events", Partitions: 6, ReplicationFactor: 3},
			}, nil
		},
		ListConsumerGroupsFn: func(ctx context.Context, cluster string) ([]kafka.ConsumerGroupSummary, error) {
			return []kafka.ConsumerGroupSummary{
				{GroupID: "processor", State: "Stable", MemberCount: 3},
			}, nil
		},
	}
	s := newTestServerWithEngines(t, mock)
	req := makeToolRequest(map[string]any{
		"source_cluster": "source",
		"target_cluster": "target",
	})

	result, err := s.handleAutopilotAssess(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "session") {
		t.Error("expected 'session' in response")
	}
}

func TestHandleAutopilotAssess_GroupListingFailure(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{{Name: "t1", Partitions: 1}}, nil
		},
		ListConsumerGroupsFn: func(ctx context.Context, cluster string) ([]kafka.ConsumerGroupSummary, error) {
			return nil, fmt.Errorf("connection timeout")
		},
	}
	s := newTestServerWithEngines(t, mock)
	req := makeToolRequest(map[string]any{
		"source_cluster": "source",
		"target_cluster": "target",
	})

	// Should still succeed (groups are optional)
	result, err := s.handleAutopilotAssess(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== handleAutopilotPlan =====================

func TestHandleAutopilotPlan_MissingSessionID(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{})

	result, err := s.handleAutopilotPlan(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "session_id is required") {
		t.Error("expected 'session_id is required' error")
	}
}

func TestHandleAutopilotPlan_NonExistentSession(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{"session_id": "nonexistent"})

	result, err := s.handleAutopilotPlan(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "session not found") {
		t.Errorf("expected 'session not found', got: %s", resultText(t, result))
	}
}

func TestHandleAutopilotPlan_ValidSession(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{{Name: "t1", Partitions: 3, ReplicationFactor: 2}}, nil
		},
		ListConsumerGroupsFn: func(ctx context.Context, cluster string) ([]kafka.ConsumerGroupSummary, error) {
			return nil, nil
		},
	}
	s := newTestServerWithEngines(t, mock)

	// First assess to create a session
	assessReq := makeToolRequest(map[string]any{
		"source_cluster": "src",
		"target_cluster": "tgt",
	})
	assessResult, _ := s.handleAutopilotAssess(context.Background(), assessReq)
	text := resultText(t, assessResult)
	var assessResp map[string]any
	json.Unmarshal([]byte(text), &assessResp)
	sessionMap, _ := assessResp["session"].(map[string]any)
	sessionID, _ := sessionMap["id"].(string)

	// Now plan
	planReq := makeToolRequest(map[string]any{"session_id": sessionID})
	result, err := s.handleAutopilotPlan(context.Background(), planReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== handleAutopilotDryRun =====================

func TestHandleAutopilotDryRun_MissingSession(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{})

	result, err := s.handleAutopilotDryRun(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleAutopilotDryRun_NonExistentSession(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{"session_id": "nonexistent"})

	result, err := s.handleAutopilotDryRun(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleAutopilotExecute =====================

func TestHandleAutopilotExecute_MissingSessionID(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{})

	result, err := s.handleAutopilotExecute(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleAutopilotExecute_NonExistentSession(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{"session_id": "nonexistent"})

	result, err := s.handleAutopilotExecute(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleAutopilotRollback =====================

func TestHandleAutopilotRollback_MissingSessionID(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{})

	result, err := s.handleAutopilotRollback(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleAutopilotRollback_NonExistentSession(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{"session_id": "nonexistent"})

	result, err := s.handleAutopilotRollback(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleAutopilotStatus =====================

func TestHandleAutopilotStatus_ListAllSessions(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return nil, nil
		},
		ListConsumerGroupsFn: func(ctx context.Context, cluster string) ([]kafka.ConsumerGroupSummary, error) {
			return nil, nil
		},
	})
	req := makeToolRequest(map[string]any{})

	result, err := s.handleAutopilotStatus(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	var resp map[string]any
	json.Unmarshal([]byte(text), &resp)
	if _, ok := resp["sessions"]; !ok {
		t.Error("expected 'sessions' in response")
	}
}

func TestHandleAutopilotStatus_SingleSession(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{{Name: "t1", Partitions: 1}}, nil
		},
		ListConsumerGroupsFn: func(ctx context.Context, cluster string) ([]kafka.ConsumerGroupSummary, error) {
			return nil, nil
		},
	}
	s := newTestServerWithEngines(t, mock)

	// Create a session first
	assessReq := makeToolRequest(map[string]any{
		"source_cluster": "src",
		"target_cluster": "tgt",
	})
	assessResult, _ := s.handleAutopilotAssess(context.Background(), assessReq)
	text := resultText(t, assessResult)
	var assessResp map[string]any
	json.Unmarshal([]byte(text), &assessResp)
	sessionMap, _ := assessResp["session"].(map[string]any)
	sessionID, _ := sessionMap["id"].(string)

	// Get that session status
	statusReq := makeToolRequest(map[string]any{"session_id": sessionID})
	result, err := s.handleAutopilotStatus(context.Background(), statusReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleAutopilotStatus_NonExistentSession(t *testing.T) {
	t.Parallel()
	s := newTestServerWithEngines(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{"session_id": "bad-id"})

	result, err := s.handleAutopilotStatus(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== collectTopicInfoForMigration =====================

func TestCollectTopicInfoForMigration_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{
				{Name: "orders", Partitions: 3, ReplicationFactor: 3},
				{Name: "events", Partitions: 6, ReplicationFactor: 2},
			}, nil
		},
	}
	s := newTestServerWithEngines(t, mock)

	topics := s.collectTopicInfoForMigration(context.Background(), "default")
	if len(topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(topics))
	}
	if topics[0].Name != "orders" || topics[0].Partitions != 3 {
		t.Errorf("unexpected first topic: %+v", topics[0])
	}
}

func TestCollectTopicInfoForMigration_ListError(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	s := newTestServerWithEngines(t, mock)

	topics := s.collectTopicInfoForMigration(context.Background(), "default")
	if topics != nil {
		t.Errorf("expected nil on error, got %v", topics)
	}
}

func TestCollectTopicInfoForMigration_EmptyCluster(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{}, nil
		},
	}
	s := newTestServerWithEngines(t, mock)

	topics := s.collectTopicInfoForMigration(context.Background(), "default")
	if len(topics) != 0 {
		t.Errorf("expected 0 topics, got %d", len(topics))
	}
}
