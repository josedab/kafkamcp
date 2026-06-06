package server

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/josedab/kafkamcp/internal/approval"
	"github.com/josedab/kafkamcp/internal/auth"
	"github.com/josedab/kafkamcp/internal/config"
	"github.com/josedab/kafkamcp/internal/engine/backend"
	"github.com/josedab/kafkamcp/internal/kafka"
)

func governanceTestServer(t *testing.T) *Server {
	t.Helper()
	s := newTestServer(t)
	mgr, err := approval.NewManager(context.Background(), backend.NewInMemory(), approval.Config{
		DefaultTTL: time.Minute,
		MaxTTL:     time.Hour,
		SingleUse:  true,
	})
	if err != nil {
		t.Fatalf("approval manager: %v", err)
	}

	s.approvalManager = mgr
	s.approvalEnforced = true
	return s
}

func TestBlastRadiusPreview_MetadataUnavailable(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	s.kafkaManager = &kafka.MockKafkaManager{
		DescribeTopicFn: func(context.Context, string, string) (*kafka.TopicDetail, error) {
			return nil, errors.New("metadata unavailable")
		},
		DescribeConsumerGroupFn: func(context.Context, string, string) (*kafka.ConsumerGroupDetail, error) {
			return nil, errors.New("group unavailable")
		},
	}
	for _, tool := range []string{"kafka_delete_topic", "kafka_reset_offsets", "kafka_alter_topic"} {
		preview := s.blastRadiusPreview(context.Background(), tool, approval.Scope{}, nil)
		if preview["inspection_warning"] == nil {
			t.Errorf("%s should include an inspection warning: %v", tool, preview)
		}
		if preview["cluster"] != "default" || preview["resource"] != "(unspecified resource)" {
			t.Errorf("%s did not apply fallback scope: %v", tool, preview)
		}
	}
}

func TestHandleDiscoverTools_ReturnsPacks(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	s.toolPackFilter = NewToolPackFilter([]string{"core-read"})
	result, err := s.handleDiscoverTools(context.Background(), makeToolRequest(map[string]any{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	if !strings.Contains(resultText(t, result), "core-read") {
		t.Errorf("expected pack in result, got %s", resultText(t, result))
	}
}

func TestHandleCoreMetrics_Aggregates(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	result, err := s.handleCoreMetrics(context.Background(), makeToolRequest(nil), "agent-1", map[string]any{}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	if !strings.Contains(resultText(t, result), "aggregated") {
		t.Errorf("expected aggregated metrics, got %s", resultText(t, result))
	}
}

func TestHandlePolicyEvaluate_And_Simulate(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	args := map[string]any{"agent": "agent-1", "action": "read", "resource": "topic", "resource_name": "orders"}
	eval, err := s.handlePolicyEvaluate(context.Background(), makeToolRequest(args), "agent-1", args, time.Now())
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	assertNotError(t, eval)

	sim, err := s.handlePolicySimulate(context.Background(), makeToolRequest(args), "agent-1", args, time.Now())
	if err != nil {
		t.Fatalf("simulate: %v", err)
	}
	assertNotError(t, sim)
}

func TestHandlePolicyEvaluate_MissingArgs(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	args := map[string]any{"agent": "agent-1"}
	res, err := s.handlePolicyEvaluate(context.Background(), makeToolRequest(args), "agent-1", args, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, res)
}

func TestHandleApproval_Status(t *testing.T) {
	t.Parallel()
	s := governanceTestServer(t)
	res, err := s.handleApproval(context.Background(), makeToolRequest(map[string]any{"action": "status"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, res)
	if !strings.Contains(resultText(t, res), "write_tools") {
		t.Errorf("expected write_tools in status, got %s", resultText(t, res))
	}
}

func TestHandleApproval_ListOwnGrants(t *testing.T) {
	t.Parallel()
	s := governanceTestServer(t)
	res, err := s.handleApproval(context.Background(), makeToolRequest(map[string]any{"action": "list"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, res)
	if !strings.Contains(resultText(t, res), `"total": 0`) {
		t.Errorf("expected empty grant list, got %s", resultText(t, res))
	}
}

func TestIsAdmin(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	if s.isAdmin("agent-1") {
		t.Fatal("default test agent must not be an admin")
	}
	s.policyEngine = auth.NewPolicyEngine(config.PolicyConfig{
		Agents: []config.AgentPolicy{{ID: "root", Role: "admin"}},
	})
	if !s.isAdmin("root") {
		t.Fatal("admin policy role was not recognized")
	}
}

func TestHandleApproval_UnknownAction(t *testing.T) {
	t.Parallel()
	s := governanceTestServer(t)
	res, err := s.handleApproval(context.Background(), makeToolRequest(map[string]any{"action": "bogus"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, res)
}

func TestHandleGovernanceDecision_Simulate(t *testing.T) {
	t.Parallel()
	s := governanceTestServer(t)
	mock := &kafka.MockKafkaManager{
		DescribeTopicFn: func(ctx context.Context, cluster, topic string) (*kafka.TopicDetail, error) {
			return &kafka.TopicDetail{Name: topic}, nil
		},
	}
	s.kafkaManager = mock
	args := map[string]any{"action": "simulate", "tool": "kafka_delete_topic", "args": map[string]any{"name": "orders"}}
	res, err := s.handleGovernanceDecision(context.Background(), makeToolRequest(args))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, res)
	if !strings.Contains(resultText(t, res), "blast_radius") {
		t.Errorf("expected blast_radius in simulate, got %s", resultText(t, res))
	}
}

func TestRecoveryGuidance_CoversWriteTools(t *testing.T) {
	t.Parallel()
	for _, tool := range WriteToolNames() {
		if g := recoveryGuidance(tool); g == "" {
			t.Errorf("recoveryGuidance(%s) is empty", tool)
		}
	}
	if recoveryGuidance("kafka_unknown") == "" {
		t.Error("expected default guidance for unknown tool")
	}
}

func TestLeastPrivilegeRecommendation(t *testing.T) {
	t.Parallel()
	allowed := auth.Decision{Allowed: true}
	if !strings.Contains(leastPrivilegeRecommendation(allowed), "No policy change") {
		t.Error("expected allowed message")
	}
	denied := auth.Decision{ResourceType: auth.ResourceTopic, Action: auth.ActionWrite, ResourceName: "orders", AgentID: "a1"}
	if !strings.Contains(leastPrivilegeRecommendation(denied), "orders") {
		t.Error("expected topic-scoped recommendation")
	}
	for _, decision := range []auth.Decision{
		{ResourceType: auth.ResourceConsumerGroup, Action: auth.ActionReset, AgentID: "a1"},
		{ResourceType: auth.ResourceSchema, Action: auth.ActionRead, AgentID: "a1"},
		{ResourceType: auth.ResourceCluster, Action: auth.ActionAdmin, AgentID: "a1"},
		{ResourceType: auth.ResourceType("unknown"), Action: auth.ActionWrite, AgentID: "a1"},
	} {
		if got := leastPrivilegeRecommendation(decision); got == "" {
			t.Errorf("empty recommendation for %+v", decision)
		}
	}
}

func TestBlastRadiusPreview_ACL(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	s.kafkaManager = &kafka.MockKafkaManager{}
	scope := approval.Scope{Cluster: "default", Resource: "orders"}
	preview := s.blastRadiusPreview(context.Background(), "kafka_acl_manage", scope, map[string]any{"action": "apply", "rules": []any{map[string]any{}}})
	if preview["summary"] == "" {
		t.Error("expected non-empty summary")
	}
	if preview["desired_rules"] != 1 {
		t.Errorf("expected desired_rules=1, got %v", preview["desired_rules"])
	}
}

func TestBlastRadiusPreview_AllMutationTypes(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	s.kafkaManager = &kafka.MockKafkaManager{
		DescribeTopicFn: func(context.Context, string, string) (*kafka.TopicDetail, error) {
			return &kafka.TopicDetail{Partitions: []kafka.PartitionDetail{
				{EarliestOffset: 2, LatestOffset: 12},
				{EarliestOffset: 5, LatestOffset: 15},
			}}, nil
		},
		DescribeConsumerGroupFn: func(context.Context, string, string) (*kafka.ConsumerGroupDetail, error) {
			return &kafka.ConsumerGroupDetail{
				State: "Stable", TotalLag: 7,
				Members: []kafka.GroupMemberDetail{{MemberID: "m1"}},
			}, nil
		},
	}
	ctx := context.Background()
	cases := []struct {
		tool string
		args map[string]any
	}{
		{"kafka_delete_topic", nil},
		{"kafka_reset_offsets", nil},
		{"kafka_alter_topic", map[string]any{
			"partitions": 6,
			"configs":    map[string]any{"retention.ms": "1000", "cleanup.policy": "compact"},
		}},
		{"kafka_produce", map[string]any{"messages": []any{map[string]any{"value": "one"}}}},
		{"kafka_create_topic", map[string]any{"partitions": 3, "replication_factor": 2}},
		{"kafka_create_connector", nil},
		{"kafka_update_connector", nil},
		{"kafka_delete_connector", nil},
		{"kafka_restart_connector", nil},
		{"kafka_unknown", nil},
	}
	for _, tc := range cases {
		preview := s.blastRadiusPreview(ctx, tc.tool, approval.Scope{
			Cluster: "prod", Resource: "orders",
		}, tc.args)
		if preview["summary"] == "" {
			t.Errorf("%s returned empty summary", tc.tool)
		}
	}
	deletePreview := s.blastRadiusPreview(ctx, "kafka_delete_topic", approval.Scope{
		Cluster: "prod", Resource: "orders",
	}, nil)
	if deletePreview["estimated_retained_records"] != int64(20) {
		t.Fatalf("retained records = %v, want 20", deletePreview["estimated_retained_records"])
	}
	alterPreview := s.blastRadiusPreview(ctx, "kafka_alter_topic", approval.Scope{
		Cluster: "prod", Resource: "orders",
	}, cases[2].args)
	keys, _ := alterPreview["config_keys"].([]string)
	if len(keys) != 2 || keys[0] != "cleanup.policy" || keys[1] != "retention.ms" {
		t.Fatalf("config keys = %v", keys)
	}
}
