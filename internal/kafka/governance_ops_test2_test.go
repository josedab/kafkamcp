package kafka

import (
	"context"
	"fmt"
	"testing"
)

// Tests for governance_ops.go: CompareConfigs, AuditConfigs, TraceMessage edge cases.

func TestCompareConfigs_BothClustersEmptyGov(t *testing.T) {
	mock := &MockKafkaManager{
		CompareConfigsFn: func(ctx context.Context, sourceCluster, targetCluster, specificTopic string) (*ConfigCompareResult, error) {
			return &ConfigCompareResult{
				SourceCluster: sourceCluster,
				TargetCluster: targetCluster,
				MatchCount:    0,
				DiffCount:     0,
			}, nil
		},
	}

	result, err := mock.CompareConfigs(context.Background(), "cluster-a", "cluster-b", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DiffCount != 0 {
		t.Errorf("expected 0 diffs for empty clusters, got %d", result.DiffCount)
	}
	if result.MatchCount != 0 {
		t.Errorf("expected 0 matches for empty clusters, got %d", result.MatchCount)
	}
}

func TestCompareConfigs_TopicOnlyInSourceGov(t *testing.T) {
	mock := &MockKafkaManager{
		CompareConfigsFn: func(ctx context.Context, sourceCluster, targetCluster, specificTopic string) (*ConfigCompareResult, error) {
			return &ConfigCompareResult{
				SourceCluster: sourceCluster,
				TargetCluster: targetCluster,
				Diffs:         []ConfigDiff{{Topic: "events", OnlyIn: "source"}},
				DiffCount:     0,
			}, nil
		},
	}

	result, err := mock.CompareConfigs(context.Background(), "prod", "staging", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Diffs) != 1 {
		t.Fatalf("expected 1 diff, got %d", len(result.Diffs))
	}
	if result.Diffs[0].OnlyIn != "source" {
		t.Errorf("expected topic only in source, got %q", result.Diffs[0].OnlyIn)
	}
}

func TestCompareConfigs_TopicOnlyInTargetGov(t *testing.T) {
	mock := &MockKafkaManager{
		CompareConfigsFn: func(ctx context.Context, sourceCluster, targetCluster, specificTopic string) (*ConfigCompareResult, error) {
			return &ConfigCompareResult{
				SourceCluster: sourceCluster,
				TargetCluster: targetCluster,
				Diffs:         []ConfigDiff{{Topic: "staging-only", OnlyIn: "target"}},
			}, nil
		},
	}

	result, err := mock.CompareConfigs(context.Background(), "prod", "staging", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Diffs) != 1 || result.Diffs[0].OnlyIn != "target" {
		t.Errorf("expected topic only in target")
	}
}

func TestCompareConfigs_ConfigValueDifferencesGov(t *testing.T) {
	mock := &MockKafkaManager{
		CompareConfigsFn: func(ctx context.Context, sourceCluster, targetCluster, specificTopic string) (*ConfigCompareResult, error) {
			return &ConfigCompareResult{
				SourceCluster: sourceCluster,
				TargetCluster: targetCluster,
				Diffs: []ConfigDiff{
					{
						Topic: "orders",
						Diffs: []ConfigValueDiff{
							{Key: "retention.ms", SourceValue: "604800000", TargetValue: "86400000"},
							{Key: "compression.type", SourceValue: "lz4", TargetValue: "none"},
						},
					},
				},
				DiffCount: 1,
			}, nil
		},
	}

	result, err := mock.CompareConfigs(context.Background(), "prod", "staging", "orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DiffCount != 1 {
		t.Errorf("expected 1 differing topic, got %d", result.DiffCount)
	}
	if len(result.Diffs[0].Diffs) != 2 {
		t.Errorf("expected 2 config value diffs, got %d", len(result.Diffs[0].Diffs))
	}
}

func TestCompareConfigs_SourceListErrorGov(t *testing.T) {
	mock := &MockKafkaManager{
		CompareConfigsFn: func(ctx context.Context, sourceCluster, targetCluster, specificTopic string) (*ConfigCompareResult, error) {
			return nil, fmt.Errorf("listing source topics: connection refused")
		},
	}

	_, err := mock.CompareConfigs(context.Background(), "bad-cluster", "target", "")
	if err == nil {
		t.Error("expected error for source list failure")
	}
}

func TestAuditConfigs_AllCompliantGov(t *testing.T) {
	mock := &MockKafkaManager{
		AuditConfigsFn: func(ctx context.Context, cluster, specificTopic string, template map[string]string) (*ConfigAuditResult, error) {
			return &ConfigAuditResult{
				Cluster:      cluster,
				Compliant:    3,
				NonCompliant: 0,
			}, nil
		},
	}

	result, err := mock.AuditConfigs(context.Background(), "prod", "",
		map[string]string{"retention.ms": "604800000", "compression.type": "lz4"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.NonCompliant != 0 {
		t.Errorf("expected 0 non-compliant, got %d", result.NonCompliant)
	}
	if result.Compliant != 3 {
		t.Errorf("expected 3 compliant, got %d", result.Compliant)
	}
}

func TestAuditConfigs_DeviationsWithRemediation(t *testing.T) {
	mock := &MockKafkaManager{
		AuditConfigsFn: func(ctx context.Context, cluster, specificTopic string, template map[string]string) (*ConfigAuditResult, error) {
			return &ConfigAuditResult{
				Cluster: cluster,
				Deviations: []ConfigDeviation{
					{
						Topic:       "orders",
						Key:         "retention.ms",
						Expected:    "604800000",
						Actual:      "-1",
						Remediation: "kafka-configs --alter --entity-type topics --entity-name orders --add-config retention.ms=604800000",
					},
				},
				NonCompliant: 1,
				Compliant:    2,
			}, nil
		},
	}

	result, err := mock.AuditConfigs(context.Background(), "prod", "",
		map[string]string{"retention.ms": "604800000"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.NonCompliant != 1 {
		t.Errorf("expected 1 non-compliant, got %d", result.NonCompliant)
	}
	if len(result.Deviations) != 1 {
		t.Fatalf("expected 1 deviation, got %d", len(result.Deviations))
	}
	d := result.Deviations[0]
	if d.Topic != "orders" {
		t.Errorf("expected topic=orders, got %s", d.Topic)
	}
	if d.Remediation == "" {
		t.Error("expected non-empty remediation command")
	}
}

func TestAuditConfigs_ListTopicsError(t *testing.T) {
	mock := &MockKafkaManager{
		AuditConfigsFn: func(ctx context.Context, cluster, specificTopic string, template map[string]string) (*ConfigAuditResult, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}

	_, err := mock.AuditConfigs(context.Background(), "bad", "", map[string]string{"retention.ms": "100"})
	if err == nil {
		t.Error("expected error")
	}
}

func TestTraceMessage_SortedByTimestamp(t *testing.T) {
	mock := &MockKafkaManager{
		TraceMessageFn: func(ctx context.Context, cluster, correlationID, headerName string, topics []string, maxScan int) (*TraceResult, error) {
			return &TraceResult{
				CorrelationID: correlationID,
				Hops: []TraceHop{
					{Topic: "orders", Timestamp: "2024-01-01T00:00:00.000Z"},
					{Topic: "payments", Timestamp: "2024-01-01T00:00:01.000Z"},
					{Topic: "notifications", Timestamp: "2024-01-01T00:00:02.000Z"},
				},
				TopicsSearched: 3,
				TotalMessages:  3,
			}, nil
		},
	}

	result, err := mock.TraceMessage(context.Background(), "default", "trace-1", "",
		[]string{"orders", "payments", "notifications"}, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i := 1; i < len(result.Hops); i++ {
		if result.Hops[i].Timestamp < result.Hops[i-1].Timestamp {
			t.Errorf("hops not sorted by timestamp: %s < %s",
				result.Hops[i].Timestamp, result.Hops[i-1].Timestamp)
		}
	}
}

func TestTraceMessage_ErrorOnOneTopic(t *testing.T) {
	mock := &MockKafkaManager{
		TraceMessageFn: func(ctx context.Context, cluster, correlationID, headerName string, topics []string, maxScan int) (*TraceResult, error) {
			// Simulates: one topic errors out but others succeed
			return &TraceResult{
				CorrelationID:  correlationID,
				TopicsSearched: 2, // only 2 of 3 succeeded
				TotalMessages:  1,
				Hops: []TraceHop{
					{Topic: "orders", Partition: 0, Offset: 5},
				},
			}, nil
		},
	}

	result, err := mock.TraceMessage(context.Background(), "default", "c-1", "",
		[]string{"orders", "bad-topic", "payments"}, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TopicsSearched != 2 {
		t.Errorf("expected 2 topics searched (one errored), got %d", result.TopicsSearched)
	}
}

func TestListACLs_SecurityAnalysisOverPermissioned(t *testing.T) {
	// Build a principal with >10 ACL entries to trigger over-permissioned detection
	acls := make([]ACLEntry, 11)
	for i := range acls {
		acls[i] = ACLEntry{
			Principal:    "User:overactive",
			ResourceName: fmt.Sprintf("topic-%d", i),
			ResourceType: "Topic",
			Operation:    "Read",
			Permission:   "Allow",
		}
	}

	analysis := analyzeACLSecurity(acls)
	if len(analysis.OverPermissioned) == 0 {
		t.Error("expected over-permissioned finding for principal with >10 ACLs")
	}
	if analysis.Findings == 0 {
		t.Error("expected findings > 0")
	}
}

func TestListACLs_SecurityAnalysisNoFindings(t *testing.T) {
	acls := []ACLEntry{
		{Principal: "User:app1", ResourceName: "orders", ResourceType: "Topic", Operation: "Read"},
	}

	analysis := analyzeACLSecurity(acls)
	if analysis.Findings != 0 {
		t.Errorf("expected 0 findings for well-scoped ACLs, got %d", analysis.Findings)
	}
}
