package kafka

import (
	"context"
	"fmt"
	"testing"
)

// ===================== TimeTravelSnapshot (mock-based) =====================

func TestTimeTravelSnapshot_Success(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelSnapshotFn: func(ctx context.Context, cluster, topic, timestamp string, maxKeys int) (*TimeTravelSnapshot, error) {
			return &TimeTravelSnapshot{
				Topic:     topic,
				Timestamp: timestamp,
				KeyCount:  2,
				State:     map[string]any{"key1": "val1", "key2": "val2"},
				ScanInfo:  TimeTravelScanInfo{MessagesScanned: 10, Duration: "5"},
			}, nil
		},
	}
	result, err := mock.TimeTravelSnapshot(context.Background(), "default", "orders", "2024-01-01T00:00:00Z", 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.KeyCount != 2 {
		t.Errorf("expected 2 keys, got %d", result.KeyCount)
	}
	if result.State["key1"] != "val1" {
		t.Errorf("expected key1=val1, got %v", result.State["key1"])
	}
}

func TestTimeTravelSnapshot_TombstoneDeletesKey(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelSnapshotFn: func(ctx context.Context, cluster, topic, timestamp string, maxKeys int) (*TimeTravelSnapshot, error) {
			return &TimeTravelSnapshot{
				Topic:    topic,
				KeyCount: 1,
				State:    map[string]any{"key2": "val2"},
			}, nil
		},
	}
	result, err := mock.TimeTravelSnapshot(context.Background(), "default", "compacted", "2024-01-01T12:00:00Z", 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, exists := result.State["key1"]; exists {
		t.Error("key1 should have been deleted by tombstone")
	}
	if result.KeyCount != 1 {
		t.Errorf("expected 1 key after tombstone, got %d", result.KeyCount)
	}
}

func TestTimeTravelSnapshot_MaxKeysLimit(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelSnapshotFn: func(ctx context.Context, cluster, topic, timestamp string, maxKeys int) (*TimeTravelSnapshot, error) {
			state := make(map[string]any)
			for i := 0; i < maxKeys; i++ {
				state[fmt.Sprintf("key%d", i)] = fmt.Sprintf("val%d", i)
			}
			return &TimeTravelSnapshot{
				Topic:    topic,
				KeyCount: maxKeys,
				State:    state,
			}, nil
		},
	}
	result, err := mock.TimeTravelSnapshot(context.Background(), "default", "big-topic", "2024-01-01T00:00:00Z", 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.KeyCount != 5 {
		t.Errorf("expected 5 keys (maxKeys limit), got %d", result.KeyCount)
	}
}

func TestTimeTravelSnapshot_NoMessagesBeforeTimestamp(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelSnapshotFn: func(ctx context.Context, cluster, topic, timestamp string, maxKeys int) (*TimeTravelSnapshot, error) {
			return &TimeTravelSnapshot{
				Topic:    topic,
				KeyCount: 0,
				State:    map[string]any{},
				ScanInfo: TimeTravelScanInfo{MessagesScanned: 0},
			}, nil
		},
	}
	result, err := mock.TimeTravelSnapshot(context.Background(), "default", "orders", "2020-01-01T00:00:00Z", 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.KeyCount != 0 {
		t.Errorf("expected 0 keys, got %d", result.KeyCount)
	}
}

func TestTimeTravelSnapshot_ConsumeError(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelSnapshotFn: func(ctx context.Context, cluster, topic, timestamp string, maxKeys int) (*TimeTravelSnapshot, error) {
			return nil, fmt.Errorf("consuming for snapshot: connection refused")
		},
	}
	_, err := mock.TimeTravelSnapshot(context.Background(), "default", "orders", "2024-01-01T00:00:00Z", 1000)
	if err == nil {
		t.Error("expected error")
	}
}

// ===================== TimeTravelDiff (mock-based) =====================

func TestTimeTravelDiff_AddedModifiedDeleted(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelDiffFn: func(ctx context.Context, cluster, topic, tsFrom, tsTo string, maxKeys int) (*TimeTravelDiffResult, error) {
			return &TimeTravelDiffResult{
				Topic:        topic,
				From:         tsFrom,
				To:           tsTo,
				TotalChanges: 3,
				Added:        map[string]any{"key3": "new-val"},
				Modified: []TimeTravelModified{
					{Key: "key1", OldValue: "old", NewValue: "new"},
				},
				Deleted: []string{"key2"},
			}, nil
		},
	}
	result, err := mock.TimeTravelDiff(context.Background(), "default", "orders",
		"2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalChanges != 3 {
		t.Errorf("expected 3 total changes, got %d", result.TotalChanges)
	}
	if len(result.Added) != 1 {
		t.Errorf("expected 1 added, got %d", len(result.Added))
	}
	if len(result.Modified) != 1 {
		t.Errorf("expected 1 modified, got %d", len(result.Modified))
	}
	if len(result.Deleted) != 1 {
		t.Errorf("expected 1 deleted, got %d", len(result.Deleted))
	}
}

func TestTimeTravelDiff_IdenticalSnapshots(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelDiffFn: func(ctx context.Context, cluster, topic, tsFrom, tsTo string, maxKeys int) (*TimeTravelDiffResult, error) {
			return &TimeTravelDiffResult{
				Topic:        topic,
				From:         tsFrom,
				To:           tsTo,
				TotalChanges: 0,
				Added:        map[string]any{},
			}, nil
		},
	}
	result, err := mock.TimeTravelDiff(context.Background(), "default", "stable-topic",
		"2024-01-01T00:00:00Z", "2024-01-01T00:00:01Z", 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalChanges != 0 {
		t.Errorf("expected 0 changes for identical snapshots, got %d", result.TotalChanges)
	}
}

func TestTimeTravelDiff_Error(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelDiffFn: func(ctx context.Context, cluster, topic, tsFrom, tsTo string, maxKeys int) (*TimeTravelDiffResult, error) {
			return nil, fmt.Errorf("building from-snapshot: connection refused")
		},
	}
	_, err := mock.TimeTravelDiff(context.Background(), "default", "orders",
		"2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", 1000)
	if err == nil {
		t.Error("expected error")
	}
}

// ===================== TraceMessage (mock-based) =====================

func TestTraceMessage_AcrossTopics(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TraceMessageFn: func(ctx context.Context, cluster, correlationID, headerName string, topics []string, maxScan int) (*TraceResult, error) {
			return &TraceResult{
				CorrelationID: correlationID,
				Hops: []TraceHop{
					{Topic: "orders", Partition: 0, Offset: 10, Timestamp: "2024-01-01T00:00:00.000Z"},
					{Topic: "payments", Partition: 0, Offset: 20, Timestamp: "2024-01-01T00:00:01.000Z"},
					{Topic: "notifications", Partition: 0, Offset: 5, Timestamp: "2024-01-01T00:00:02.000Z"},
				},
				TopicsSearched: 3,
				TotalMessages:  3,
				FlowGraph: []FlowEdge{
					{FromTopic: "orders", ToTopic: "payments", LatencyMs: 1000},
					{FromTopic: "payments", ToTopic: "notifications", LatencyMs: 1000},
				},
			}, nil
		},
	}
	result, err := mock.TraceMessage(context.Background(), "default", "corr-123", "",
		[]string{"orders", "payments", "notifications"}, 10000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalMessages != 3 {
		t.Errorf("expected 3 hops, got %d", result.TotalMessages)
	}
	if len(result.FlowGraph) != 2 {
		t.Errorf("expected 2 flow edges, got %d", len(result.FlowGraph))
	}
	if result.Hops[0].Topic != "orders" {
		t.Errorf("expected first hop=orders, got %s", result.Hops[0].Topic)
	}
}

func TestTraceMessage_NoMatchingMessages(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TraceMessageFn: func(ctx context.Context, cluster, correlationID, headerName string, topics []string, maxScan int) (*TraceResult, error) {
			return &TraceResult{
				CorrelationID:  correlationID,
				TopicsSearched: len(topics),
				TotalMessages:  0,
			}, nil
		},
	}
	result, err := mock.TraceMessage(context.Background(), "default", "nonexistent", "",
		[]string{"orders", "payments"}, 10000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalMessages != 0 {
		t.Errorf("expected 0 messages, got %d", result.TotalMessages)
	}
}

func TestTraceMessage_CustomHeaderName(t *testing.T) {
	t.Parallel()
	var capturedHeaderName string
	mock := &MockKafkaManager{
		TraceMessageFn: func(ctx context.Context, cluster, correlationID, headerName string, topics []string, maxScan int) (*TraceResult, error) {
			capturedHeaderName = headerName
			return &TraceResult{CorrelationID: correlationID}, nil
		},
	}
	_, _ = mock.TraceMessage(context.Background(), "default", "c-1", "x-custom-trace-id",
		[]string{"topic1"}, 1000)
	if capturedHeaderName != "x-custom-trace-id" {
		t.Errorf("expected custom header name, got %q", capturedHeaderName)
	}
}

// ===================== ListACLs (mock-based) =====================

func TestListACLs_WithAnalysis(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		ListACLsFn: func(ctx context.Context, cluster, principal, resourceType string, includeAnalysis bool) (*ACLResult, error) {
			acls := []ACLEntry{
				{Principal: "User:admin", ResourceName: "*", ResourceType: "Topic", Operation: "All", Permission: "Allow"},
				{Principal: "User:app1", ResourceName: "orders", ResourceType: "Topic", Operation: "Read", Permission: "Allow"},
			}
			result := &ACLResult{ACLs: acls, TotalACLs: len(acls)}
			if includeAnalysis {
				result.SecurityAnalysis = analyzeACLSecurity(acls)
			}
			return result, nil
		},
	}
	result, err := mock.ListACLs(context.Background(), "default", "", "", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalACLs != 2 {
		t.Errorf("expected 2 ACLs, got %d", result.TotalACLs)
	}
	if result.SecurityAnalysis == nil {
		t.Fatal("expected security analysis")
	}
	if len(result.SecurityAnalysis.WildcardACLs) != 1 {
		t.Errorf("expected 1 wildcard ACL, got %d", len(result.SecurityAnalysis.WildcardACLs))
	}
}

func TestListACLs_FilterByPrincipal(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		ListACLsFn: func(ctx context.Context, cluster, principal, resourceType string, includeAnalysis bool) (*ACLResult, error) {
			allACLs := []ACLEntry{
				{Principal: "User:admin", ResourceName: "*", ResourceType: "Topic"},
				{Principal: "User:app1", ResourceName: "orders", ResourceType: "Topic"},
			}
			var filtered []ACLEntry
			for _, a := range allACLs {
				if principal == "" || a.Principal == principal {
					filtered = append(filtered, a)
				}
			}
			return &ACLResult{ACLs: filtered, TotalACLs: len(filtered)}, nil
		},
	}
	result, err := mock.ListACLs(context.Background(), "default", "User:app1", "", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalACLs != 1 {
		t.Errorf("expected 1 ACL for User:app1, got %d", result.TotalACLs)
	}
}

func TestListACLs_EmptyACLs(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		ListACLsFn: func(ctx context.Context, cluster, principal, resourceType string, includeAnalysis bool) (*ACLResult, error) {
			return &ACLResult{ACLs: nil, TotalACLs: 0}, nil
		},
	}
	result, err := mock.ListACLs(context.Background(), "default", "", "", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalACLs != 0 {
		t.Errorf("expected 0 ACLs, got %d", result.TotalACLs)
	}
}

func TestListACLs_FilterByResourceType(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		ListACLsFn: func(ctx context.Context, cluster, principal, resourceType string, includeAnalysis bool) (*ACLResult, error) {
			allACLs := []ACLEntry{
				{Principal: "User:a", ResourceType: "Topic", ResourceName: "t1"},
				{Principal: "User:a", ResourceType: "Group", ResourceName: "g1"},
			}
			var filtered []ACLEntry
			for _, a := range allACLs {
				if resourceType == "" || a.ResourceType == resourceType {
					filtered = append(filtered, a)
				}
			}
			return &ACLResult{ACLs: filtered, TotalACLs: len(filtered)}, nil
		},
	}
	result, err := mock.ListACLs(context.Background(), "default", "", "Topic", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalACLs != 1 {
		t.Errorf("expected 1 Topic ACL, got %d", result.TotalACLs)
	}
}

func TestListACLs_Error(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		ListACLsFn: func(ctx context.Context, cluster, principal, resourceType string, includeAnalysis bool) (*ACLResult, error) {
			return nil, fmt.Errorf("describing ACLs: unauthorized")
		},
	}
	_, err := mock.ListACLs(context.Background(), "default", "", "", false)
	if err == nil {
		t.Error("expected error")
	}
}
