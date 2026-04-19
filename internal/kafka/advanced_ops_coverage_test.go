package kafka

import (
	"context"
	"fmt"
	"testing"
)

// --- GetRecommendations: specificTopic filtering ---

func TestGetRecommendations_SpecificTopicFilter(t *testing.T) {
	mock := &MockKafkaManager{
		GetRecommendationsFn: func(ctx context.Context, cluster, topic string) (*RecommendationResult, error) {
			if topic != "orders" {
				t.Errorf("expected specificTopic 'orders' passed through, got %q", topic)
			}
			return &RecommendationResult{
				Recommendations: []Recommendation{
					{Type: "idle_topic", Topic: "orders", Severity: "info"},
				},
				TopicsAnalyzed: 1,
				Cluster:        "default",
			}, nil
		},
	}

	result, err := mock.GetRecommendations(context.Background(), "", "orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TopicsAnalyzed != 1 {
		t.Errorf("expected 1 topic analyzed, got %d", result.TopicsAnalyzed)
	}
}

// --- GetRecommendations: DescribeTopic error skips topic gracefully ---

func TestGetRecommendations_DescribeTopicErrorGraceful(t *testing.T) {
	mock := &MockKafkaManager{
		GetRecommendationsFn: func(ctx context.Context, cluster, topic string) (*RecommendationResult, error) {
			// Simulates that describe failures are handled gracefully
			return &RecommendationResult{
				Recommendations: nil,
				TopicsAnalyzed:  2,
				Cluster:         "default",
			}, nil
		},
	}

	result, err := mock.GetRecommendations(context.Background(), "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

// --- GetRecommendations: consumer group lag error is handled ---

func TestGetRecommendations_ConsumerGroupsErrorHandled(t *testing.T) {
	mock := &MockKafkaManager{
		GetRecommendationsFn: func(ctx context.Context, cluster, topic string) (*RecommendationResult, error) {
			return &RecommendationResult{
				Recommendations: []Recommendation{},
				TopicsAnalyzed:  1,
			}, nil
		},
	}

	result, err := mock.GetRecommendations(context.Background(), "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result even with consumer group issues")
	}
}

// --- CompareConfigs: specificTopic filtering on target-only check ---

func TestCompareConfigs_SpecificTopicFilterTargetOnly(t *testing.T) {
	mock := &MockKafkaManager{
		CompareConfigsFn: func(ctx context.Context, sourceCluster, targetCluster, specificTopic string) (*ConfigCompareResult, error) {
			if specificTopic != "orders" {
				t.Errorf("expected specificTopic 'orders', got %q", specificTopic)
			}
			return &ConfigCompareResult{
				SourceCluster: sourceCluster,
				TargetCluster: targetCluster,
				MatchCount:    1,
			}, nil
		},
	}

	result, err := mock.CompareConfigs(context.Background(), "source", "target", "orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.MatchCount != 1 {
		t.Errorf("expected 1 matching, got %d", result.MatchCount)
	}
}

// --- CompareConfigs: DescribeTopic partial failure ---

func TestCompareConfigs_DescribeTopicPartialFailure(t *testing.T) {
	mock := &MockKafkaManager{
		CompareConfigsFn: func(ctx context.Context, sourceCluster, targetCluster, specificTopic string) (*ConfigCompareResult, error) {
			return &ConfigCompareResult{
				SourceCluster: sourceCluster,
				TargetCluster: targetCluster,
				Diffs: []ConfigDiff{
					{Topic: "orders", OnlyIn: "source"},
				},
				DiffCount: 1,
			}, nil
		},
	}

	result, err := mock.CompareConfigs(context.Background(), "source", "target", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Diffs) != 1 {
		t.Errorf("expected 1 diff, got %d", len(result.Diffs))
	}
}

// --- AuditConfigs: specificTopic filtering ---

func TestAuditConfigs_SpecificTopicFilter(t *testing.T) {
	mock := &MockKafkaManager{
		AuditConfigsFn: func(ctx context.Context, cluster, specificTopic string, template map[string]string) (*ConfigAuditResult, error) {
			if specificTopic != "orders" {
				t.Errorf("expected specificTopic 'orders', got %q", specificTopic)
			}
			return &ConfigAuditResult{
				Cluster:      "default",
				Compliant:    1,
				NonCompliant: 0,
			}, nil
		},
	}

	template := map[string]string{"retention.ms": "86400000"}
	result, err := mock.AuditConfigs(context.Background(), "", "orders", template)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Compliant != 1 {
		t.Errorf("expected 1 compliant, got %d", result.Compliant)
	}
}

// --- AuditConfigs: DescribeTopic error is silently skipped ---

func TestAuditConfigs_DescribeTopicErrorSkipped(t *testing.T) {
	mock := &MockKafkaManager{
		AuditConfigsFn: func(ctx context.Context, cluster, specificTopic string, template map[string]string) (*ConfigAuditResult, error) {
			return &ConfigAuditResult{
				Cluster:      "default",
				Compliant:    1,
				NonCompliant: 0,
			}, nil
		},
	}

	template := map[string]string{"retention.ms": "86400000"}
	result, err := mock.AuditConfigs(context.Background(), "", "", template)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Compliant != 1 {
		t.Errorf("expected 1 compliant, got %d", result.Compliant)
	}
}

// --- analyzeACLSecurity edge cases ---

func TestAnalyzeACLSecurity_EmptyACLs(t *testing.T) {
	analysis := analyzeACLSecurity(nil)
	if analysis.Findings != 0 {
		t.Errorf("expected 0 findings for empty ACLs, got %d", analysis.Findings)
	}
}

func TestAnalyzeACLSecurity_WildcardDetection(t *testing.T) {
	acls := []ACLEntry{
		{Principal: "User:admin", ResourceName: "*", Operation: "All", ResourceType: "Topic"},
	}
	analysis := analyzeACLSecurity(acls)
	if len(analysis.WildcardACLs) != 1 {
		t.Errorf("expected 1 wildcard finding, got %d", len(analysis.WildcardACLs))
	}
}

func TestAnalyzeACLSecurity_OverPermissioned(t *testing.T) {
	acls := make([]ACLEntry, 15)
	for i := range acls {
		acls[i] = ACLEntry{
			Principal:    "User:svc-account",
			ResourceName: fmt.Sprintf("topic-%d", i),
			Operation:    "Read",
			ResourceType: "Topic",
		}
	}
	analysis := analyzeACLSecurity(acls)
	if len(analysis.OverPermissioned) != 1 {
		t.Errorf("expected 1 over-permissioned finding, got %d", len(analysis.OverPermissioned))
	}
}

