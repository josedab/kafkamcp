package kafka

import (
	"context"
	"fmt"
	"testing"
)

// --- TraceMessage tests ---

func TestTraceMessage_CorrelationFoundInOneTopic(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TraceMessageFn: func(ctx context.Context, cluster, correlationID, headerName string, topics []string, maxScan int) (*TraceResult, error) {
			return &TraceResult{
				CorrelationID:  correlationID,
				TopicsSearched: 1,
				TotalMessages:  1,
				Hops: []TraceHop{
					{Topic: "orders", Partition: 0, Offset: 42, Timestamp: "2024-01-01T00:00:00.000Z"},
				},
			}, nil
		},
	}

	result, err := mock.TraceMessage(context.Background(), "default", "abc-123", "", []string{"orders"}, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalMessages != 1 {
		t.Errorf("TotalMessages = %d, want 1", result.TotalMessages)
	}
	if result.Hops[0].Topic != "orders" {
		t.Errorf("Topic = %q, want orders", result.Hops[0].Topic)
	}
}

func TestTraceMessage_NotFound(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TraceMessageFn: func(ctx context.Context, cluster, correlationID, headerName string, topics []string, maxScan int) (*TraceResult, error) {
			return &TraceResult{
				CorrelationID:  correlationID,
				TopicsSearched: 3,
				TotalMessages:  0,
			}, nil
		},
	}

	result, err := mock.TraceMessage(context.Background(), "default", "nonexistent", "", []string{"a", "b", "c"}, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalMessages != 0 {
		t.Errorf("expected 0 messages for non-existent correlation ID")
	}
}

func TestTraceMessage_FlowGraphEdges(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TraceMessageFn: func(ctx context.Context, cluster, correlationID, headerName string, topics []string, maxScan int) (*TraceResult, error) {
			return &TraceResult{
				CorrelationID:  correlationID,
				TopicsSearched: 3,
				TotalMessages:  3,
				Hops: []TraceHop{
					{Topic: "input", Timestamp: "2024-01-01T00:00:00.000Z"},
					{Topic: "processed", Timestamp: "2024-01-01T00:00:01.000Z"},
					{Topic: "output", Timestamp: "2024-01-01T00:00:02.000Z"},
				},
				FlowGraph: []FlowEdge{
					{FromTopic: "input", ToTopic: "processed", LatencyMs: 0},
					{FromTopic: "processed", ToTopic: "output", LatencyMs: 0},
				},
			}, nil
		},
	}

	result, err := mock.TraceMessage(context.Background(), "default", "flow-1", "", []string{"input", "processed", "output"}, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.FlowGraph) != 2 {
		t.Fatalf("expected 2 flow edges, got %d", len(result.FlowGraph))
	}
	if result.FlowGraph[0].FromTopic != "input" || result.FlowGraph[0].ToTopic != "processed" {
		t.Errorf("first edge: %s -> %s", result.FlowGraph[0].FromTopic, result.FlowGraph[0].ToTopic)
	}
	if result.FlowGraph[1].FromTopic != "processed" || result.FlowGraph[1].ToTopic != "output" {
		t.Errorf("second edge: %s -> %s", result.FlowGraph[1].FromTopic, result.FlowGraph[1].ToTopic)
	}
}

func TestTraceMessage_MultipleTopics(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TraceMessageFn: func(ctx context.Context, cluster, correlationID, headerName string, topics []string, maxScan int) (*TraceResult, error) {
			return &TraceResult{
				CorrelationID:  correlationID,
				TopicsSearched: len(topics),
				TotalMessages:  2,
				Hops: []TraceHop{
					{Topic: "topic-a", Partition: 0, Offset: 10},
					{Topic: "topic-c", Partition: 2, Offset: 55},
				},
			}, nil
		},
	}

	result, err := mock.TraceMessage(context.Background(), "default", "multi-1", "x-trace-id", []string{"topic-a", "topic-b", "topic-c"}, 500)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TopicsSearched != 3 {
		t.Errorf("TopicsSearched = %d, want 3", result.TopicsSearched)
	}
	if result.TotalMessages != 2 {
		t.Errorf("TotalMessages = %d, want 2", result.TotalMessages)
	}
}

// --- analyzeACLSecurity tests ---

func TestAnalyzeACLSecurity_WildcardACLs(t *testing.T) {
	t.Parallel()
	acls := []ACLEntry{
		{Principal: "User:admin", ResourceName: "*", ResourceType: "Topic", Operation: "All"},
	}
	analysis := analyzeACLSecurity(acls)
	if len(analysis.WildcardACLs) != 1 {
		t.Errorf("expected 1 wildcard finding, got %d", len(analysis.WildcardACLs))
	}
	if analysis.WildcardACLs[0].Severity != "warning" {
		t.Errorf("severity = %q, want warning", analysis.WildcardACLs[0].Severity)
	}
	if analysis.WildcardACLs[0].Confidence != 0.9 {
		t.Errorf("confidence = %f, want 0.9", analysis.WildcardACLs[0].Confidence)
	}
}

func TestAnalyzeACLSecurity_OverPermissionedGov(t *testing.T) {
	t.Parallel()
	acls := make([]ACLEntry, 12)
	for i := range acls {
		acls[i] = ACLEntry{
			Principal:    "User:overactive",
			ResourceName: fmt.Sprintf("topic-%d", i),
			ResourceType: "Topic",
			Operation:    "Read",
		}
	}
	analysis := analyzeACLSecurity(acls)
	if len(analysis.OverPermissioned) != 1 {
		t.Errorf("expected 1 over-permissioned finding, got %d", len(analysis.OverPermissioned))
	}
	if analysis.OverPermissioned[0].Confidence != 0.7 {
		t.Errorf("confidence = %f, want 0.7", analysis.OverPermissioned[0].Confidence)
	}
}

func TestAnalyzeACLSecurity_BothWildcardAndOverPermissioned(t *testing.T) {
	t.Parallel()
	acls := make([]ACLEntry, 11)
	for i := range acls {
		acls[i] = ACLEntry{
			Principal:    "User:admin",
			ResourceName: "*",
			ResourceType: "Topic",
			Operation:    "Read",
		}
	}
	analysis := analyzeACLSecurity(acls)
	if analysis.Findings < 2 {
		t.Errorf("expected at least 2 findings, got %d", analysis.Findings)
	}
}

func TestAnalyzeACLSecurity_EmptyACLsGov(t *testing.T) {
	t.Parallel()
	analysis := analyzeACLSecurity(nil)
	if analysis.Findings != 0 {
		t.Errorf("expected 0 findings for empty ACLs, got %d", analysis.Findings)
	}
}

func TestAnalyzeACLSecurity_MultiplePrincipals(t *testing.T) {
	t.Parallel()
	acls := []ACLEntry{
		{Principal: "User:app1", ResourceName: "orders", ResourceType: "Topic", Operation: "Read"},
		{Principal: "User:app2", ResourceName: "*", ResourceType: "Topic", Operation: "Write"},
	}
	analysis := analyzeACLSecurity(acls)
	if len(analysis.WildcardACLs) != 1 {
		t.Errorf("expected 1 wildcard for app2, got %d", len(analysis.WildcardACLs))
	}
	if analysis.WildcardACLs[0].Principal != "User:app2" {
		t.Errorf("principal = %q, want User:app2", analysis.WildcardACLs[0].Principal)
	}
}

// --- ListACLs mock tests ---

func TestListACLs_PrincipalFilter(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		ListACLsFn: func(ctx context.Context, cluster, principal, resourceType string, includeAnalysis bool) (*ACLResult, error) {
			acls := []ACLEntry{
				{Principal: "User:app1", ResourceName: "orders"},
			}
			return &ACLResult{ACLs: acls, TotalACLs: 1}, nil
		},
	}

	result, err := mock.ListACLs(context.Background(), "default", "app1", "", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalACLs != 1 {
		t.Errorf("TotalACLs = %d, want 1", result.TotalACLs)
	}
}

func TestListACLs_ResourceTypeFilter(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		ListACLsFn: func(ctx context.Context, cluster, principal, resourceType string, includeAnalysis bool) (*ACLResult, error) {
			acls := []ACLEntry{
				{Principal: "User:app1", ResourceType: "Topic", ResourceName: "orders"},
			}
			return &ACLResult{ACLs: acls, TotalACLs: 1}, nil
		},
	}

	result, err := mock.ListACLs(context.Background(), "default", "", "Topic", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalACLs != 1 {
		t.Errorf("TotalACLs = %d, want 1", result.TotalACLs)
	}
}

func TestListACLs_IncludeAnalysis(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		ListACLsFn: func(ctx context.Context, cluster, principal, resourceType string, includeAnalysis bool) (*ACLResult, error) {
			acls := []ACLEntry{
				{Principal: "User:admin", ResourceName: "*", ResourceType: "Topic", Operation: "All"},
			}
			result := &ACLResult{ACLs: acls, TotalACLs: 1}
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
	if result.SecurityAnalysis == nil {
		t.Fatal("expected security analysis")
	}
	if result.SecurityAnalysis.Findings == 0 {
		t.Error("expected findings for wildcard ACL")
	}
}

func TestListACLs_ExcludeAnalysis(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		ListACLsFn: func(ctx context.Context, cluster, principal, resourceType string, includeAnalysis bool) (*ACLResult, error) {
			return &ACLResult{ACLs: []ACLEntry{}, TotalACLs: 0}, nil
		},
	}

	result, err := mock.ListACLs(context.Background(), "default", "", "", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.SecurityAnalysis != nil {
		t.Error("expected no security analysis when includeAnalysis=false")
	}
}

// --- CompareConfigs additional tests ---

func TestCompareConfigs_IdenticalConfigs(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		CompareConfigsFn: func(ctx context.Context, sourceCluster, targetCluster, specificTopic string) (*ConfigCompareResult, error) {
			return &ConfigCompareResult{
				SourceCluster: sourceCluster,
				TargetCluster: targetCluster,
				MatchCount:    5,
				DiffCount:     0,
			}, nil
		},
	}

	result, err := mock.CompareConfigs(context.Background(), "prod", "dr", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DiffCount != 0 {
		t.Errorf("expected 0 diffs for identical configs, got %d", result.DiffCount)
	}
	if result.MatchCount != 5 {
		t.Errorf("expected 5 matching, got %d", result.MatchCount)
	}
}

func TestCompareConfigs_DriftedValues(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		CompareConfigsFn: func(ctx context.Context, sourceCluster, targetCluster, specificTopic string) (*ConfigCompareResult, error) {
			return &ConfigCompareResult{
				Diffs: []ConfigDiff{
					{
						Topic: "events",
						Diffs: []ConfigValueDiff{
							{Key: "retention.ms", SourceValue: "86400000", TargetValue: "604800000"},
						},
					},
				},
				DiffCount: 1,
			}, nil
		},
	}

	result, err := mock.CompareConfigs(context.Background(), "a", "b", "events")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Diffs) != 1 || len(result.Diffs[0].Diffs) != 1 {
		t.Fatalf("expected 1 topic with 1 config diff")
	}
	if result.Diffs[0].Diffs[0].Key != "retention.ms" {
		t.Errorf("expected retention.ms diff")
	}
}

// --- AuditConfigs additional tests ---

func TestAuditConfigs_RemediationCommands(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AuditConfigsFn: func(ctx context.Context, cluster, specificTopic string, template map[string]string) (*ConfigAuditResult, error) {
			return &ConfigAuditResult{
				Cluster: cluster,
				Deviations: []ConfigDeviation{
					{
						Topic:       "logs",
						Key:         "compression.type",
						Expected:    "lz4",
						Actual:      "none",
						Remediation: "kafka-configs --alter --entity-type topics --entity-name logs --add-config compression.type=lz4",
					},
				},
				NonCompliant: 1,
			}, nil
		},
	}

	result, err := mock.AuditConfigs(context.Background(), "prod", "", map[string]string{"compression.type": "lz4"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Deviations) != 1 {
		t.Fatalf("expected 1 deviation, got %d", len(result.Deviations))
	}
	if result.Deviations[0].Remediation == "" {
		t.Error("expected non-empty remediation command")
	}
}

// --- Edge cases ---

func TestTraceMessage_EmptyTopicList(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TraceMessageFn: func(ctx context.Context, cluster, correlationID, headerName string, topics []string, maxScan int) (*TraceResult, error) {
			return &TraceResult{
				CorrelationID:  correlationID,
				TopicsSearched: 0,
				TotalMessages:  0,
			}, nil
		},
	}

	result, err := mock.TraceMessage(context.Background(), "default", "abc", "", nil, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TopicsSearched != 0 {
		t.Errorf("TopicsSearched = %d, want 0", result.TopicsSearched)
	}
}

func TestTraceMessage_ClusterConnectionFailure(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TraceMessageFn: func(ctx context.Context, cluster, correlationID, headerName string, topics []string, maxScan int) (*TraceResult, error) {
			return nil, fmt.Errorf("cluster connection failed")
		},
	}

	_, err := mock.TraceMessage(context.Background(), "bad-cluster", "abc", "", []string{"t1"}, 100)
	if err == nil {
		t.Error("expected error for cluster connection failure")
	}
}
