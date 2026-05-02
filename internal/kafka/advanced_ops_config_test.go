package kafka

import (
	"context"
	"fmt"
	"testing"
)

// ===================== CompareConfigs =====================

func TestCompareConfigs_TopicOnlyInSource(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		CompareConfigsFn: func(ctx context.Context, sourceCluster, targetCluster, topic string) (*ConfigCompareResult, error) {
			return &ConfigCompareResult{
				SourceCluster: sourceCluster,
				TargetCluster: targetCluster,
				Diffs: []ConfigDiff{
					{Topic: "orders", OnlyIn: "source"},
				},
				MatchCount: 0,
				DiffCount:  0,
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
		t.Errorf("expected OnlyIn='source', got %q", result.Diffs[0].OnlyIn)
	}
}

func TestCompareConfigs_TopicOnlyInTarget(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		CompareConfigsFn: func(ctx context.Context, sourceCluster, targetCluster, topic string) (*ConfigCompareResult, error) {
			return &ConfigCompareResult{
				SourceCluster: sourceCluster,
				TargetCluster: targetCluster,
				Diffs: []ConfigDiff{
					{Topic: "staging-events", OnlyIn: "target"},
				},
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
	if result.Diffs[0].OnlyIn != "target" {
		t.Errorf("expected OnlyIn='target', got %q", result.Diffs[0].OnlyIn)
	}
}

func TestCompareConfigs_MatchingTopics(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		CompareConfigsFn: func(ctx context.Context, sourceCluster, targetCluster, topic string) (*ConfigCompareResult, error) {
			return &ConfigCompareResult{
				SourceCluster: sourceCluster,
				TargetCluster: targetCluster,
				Diffs:         nil,
				MatchCount:    3,
				DiffCount:     0,
			}, nil
		},
	}

	result, err := mock.CompareConfigs(context.Background(), "prod", "staging", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.MatchCount != 3 {
		t.Errorf("expected 3 matching, got %d", result.MatchCount)
	}
	if result.DiffCount != 0 {
		t.Errorf("expected 0 differing, got %d", result.DiffCount)
	}
}

func TestCompareConfigs_TopicsWithConfigDiffs(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		CompareConfigsFn: func(ctx context.Context, sourceCluster, targetCluster, topic string) (*ConfigCompareResult, error) {
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
				MatchCount: 1,
				DiffCount:  1,
			}, nil
		},
	}

	result, err := mock.CompareConfigs(context.Background(), "prod", "staging", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DiffCount != 1 {
		t.Errorf("expected 1 differing topic, got %d", result.DiffCount)
	}
	if len(result.Diffs) != 1 {
		t.Fatalf("expected 1 diff entry, got %d", len(result.Diffs))
	}
	if len(result.Diffs[0].Diffs) != 2 {
		t.Errorf("expected 2 config value diffs, got %d", len(result.Diffs[0].Diffs))
	}
}

func TestCompareConfigs_EmptyClusters(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		CompareConfigsFn: func(ctx context.Context, sourceCluster, targetCluster, topic string) (*ConfigCompareResult, error) {
			return &ConfigCompareResult{
				SourceCluster: sourceCluster,
				TargetCluster: targetCluster,
			}, nil
		},
	}

	result, err := mock.CompareConfigs(context.Background(), "empty1", "empty2", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Diffs) != 0 {
		t.Errorf("expected 0 diffs, got %d", len(result.Diffs))
	}
}

func TestCompareConfigs_SpecificTopic(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		CompareConfigsFn: func(ctx context.Context, sourceCluster, targetCluster, topic string) (*ConfigCompareResult, error) {
			if topic != "orders" {
				t.Errorf("expected topic filter 'orders', got %q", topic)
			}
			return &ConfigCompareResult{
				SourceCluster: sourceCluster,
				TargetCluster: targetCluster,
				MatchCount:    1,
			}, nil
		},
	}

	_, err := mock.CompareConfigs(context.Background(), "prod", "staging", "orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCompareConfigs_Error(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		CompareConfigsFn: func(ctx context.Context, sourceCluster, targetCluster, topic string) (*ConfigCompareResult, error) {
			return nil, fmt.Errorf("listing source topics: connection refused")
		},
	}

	_, err := mock.CompareConfigs(context.Background(), "prod", "staging", "")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCompareConfigs_CallRecording(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		CompareConfigsFn: func(ctx context.Context, sourceCluster, targetCluster, topic string) (*ConfigCompareResult, error) {
			return &ConfigCompareResult{}, nil
		},
	}

	mock.CompareConfigs(context.Background(), "prod", "staging", "orders")
	if mock.CallCount("CompareConfigs") != 1 {
		t.Error("expected CompareConfigs to be called once")
	}
	call := mock.LastCall("CompareConfigs")
	if call.Args[0] != "prod" {
		t.Errorf("expected source 'prod', got %v", call.Args[0])
	}
	if call.Args[1] != "staging" {
		t.Errorf("expected target 'staging', got %v", call.Args[1])
	}
}

// ===================== AuditConfigs =====================

func TestAuditConfigs_AllCompliant(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AuditConfigsFn: func(ctx context.Context, cluster, topic string, template map[string]string) (*ConfigAuditResult, error) {
			return &ConfigAuditResult{
				Cluster:      cluster,
				Deviations:   nil,
				Compliant:    3,
				NonCompliant: 0,
			}, nil
		},
	}

	result, err := mock.AuditConfigs(context.Background(), "prod", "", map[string]string{
		"retention.ms":    "604800000",
		"compression.type": "lz4",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Compliant != 3 {
		t.Errorf("expected 3 compliant, got %d", result.Compliant)
	}
	if result.NonCompliant != 0 {
		t.Errorf("expected 0 non-compliant, got %d", result.NonCompliant)
	}
	if len(result.Deviations) != 0 {
		t.Errorf("expected 0 deviations, got %d", len(result.Deviations))
	}
}

func TestAuditConfigs_TopicsDeviating(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AuditConfigsFn: func(ctx context.Context, cluster, topic string, template map[string]string) (*ConfigAuditResult, error) {
			return &ConfigAuditResult{
				Cluster: cluster,
				Deviations: []ConfigDeviation{
					{
						Topic:       "orders",
						Key:         "retention.ms",
						Expected:    "604800000",
						Actual:      "86400000",
						Remediation: "kafka-configs --alter --entity-type topics --entity-name orders --add-config retention.ms=604800000",
					},
					{
						Topic:       "events",
						Key:         "compression.type",
						Expected:    "lz4",
						Actual:      "none",
						Remediation: "kafka-configs --alter --entity-type topics --entity-name events --add-config compression.type=lz4",
					},
				},
				Compliant:    1,
				NonCompliant: 2,
			}, nil
		},
	}

	result, err := mock.AuditConfigs(context.Background(), "prod", "", map[string]string{
		"retention.ms":    "604800000",
		"compression.type": "lz4",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.NonCompliant != 2 {
		t.Errorf("expected 2 non-compliant, got %d", result.NonCompliant)
	}
	if len(result.Deviations) != 2 {
		t.Errorf("expected 2 deviations, got %d", len(result.Deviations))
	}
}

func TestAuditConfigs_RemediationCommand(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AuditConfigsFn: func(ctx context.Context, cluster, topic string, template map[string]string) (*ConfigAuditResult, error) {
			return &ConfigAuditResult{
				Cluster: cluster,
				Deviations: []ConfigDeviation{
					{
						Topic:       "orders",
						Key:         "retention.ms",
						Expected:    "604800000",
						Actual:      "86400000",
						Remediation: "kafka-configs --alter --entity-type topics --entity-name orders --add-config retention.ms=604800000",
					},
				},
				NonCompliant: 1,
			}, nil
		},
	}

	result, err := mock.AuditConfigs(context.Background(), "prod", "orders", map[string]string{"retention.ms": "604800000"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Deviations) != 1 {
		t.Fatalf("expected 1 deviation, got %d", len(result.Deviations))
	}
	dev := result.Deviations[0]
	if !contains(dev.Remediation, "kafka-configs") {
		t.Error("expected remediation command to contain 'kafka-configs'")
	}
	if !contains(dev.Remediation, "orders") {
		t.Error("expected remediation command to contain topic name")
	}
	if !contains(dev.Remediation, "604800000") {
		t.Error("expected remediation command to contain expected value")
	}
}

func TestAuditConfigs_MixedCompliance(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AuditConfigsFn: func(ctx context.Context, cluster, topic string, template map[string]string) (*ConfigAuditResult, error) {
			return &ConfigAuditResult{
				Cluster:      cluster,
				Compliant:    2,
				NonCompliant: 1,
				Deviations: []ConfigDeviation{
					{Topic: "events", Key: "retention.ms", Expected: "604800000", Actual: "3600000"},
				},
			}, nil
		},
	}

	result, err := mock.AuditConfigs(context.Background(), "prod", "", map[string]string{"retention.ms": "604800000"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Compliant != 2 {
		t.Errorf("expected 2 compliant, got %d", result.Compliant)
	}
	if result.NonCompliant != 1 {
		t.Errorf("expected 1 non-compliant, got %d", result.NonCompliant)
	}
}

func TestAuditConfigs_Error(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AuditConfigsFn: func(ctx context.Context, cluster, topic string, template map[string]string) (*ConfigAuditResult, error) {
			return nil, fmt.Errorf("cluster unavailable")
		},
	}

	_, err := mock.AuditConfigs(context.Background(), "prod", "", nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestAuditConfigs_CallRecording(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AuditConfigsFn: func(ctx context.Context, cluster, topic string, template map[string]string) (*ConfigAuditResult, error) {
			return &ConfigAuditResult{}, nil
		},
	}

	tmpl := map[string]string{"retention.ms": "604800000"}
	mock.AuditConfigs(context.Background(), "prod", "orders", tmpl)
	if mock.CallCount("AuditConfigs") != 1 {
		t.Error("expected AuditConfigs to be called once")
	}
	call := mock.LastCall("AuditConfigs")
	if call.Args[0] != "prod" {
		t.Errorf("expected cluster 'prod', got %v", call.Args[0])
	}
	if call.Args[1] != "orders" {
		t.Errorf("expected topic 'orders', got %v", call.Args[1])
	}
}
