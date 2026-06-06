package server

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/josedab/kafkamcp/internal/aclmgmt"
	"github.com/josedab/kafkamcp/internal/kafka"
)

func aclTestServer(t *testing.T, mock *kafka.MockKafkaManager) *Server {
	t.Helper()
	s := newTestServerWithMock(t, mock)
	s.aclMgmtEngine = aclmgmt.NewEngine(s.logger)
	return s
}

func TestHandleACLManage_CreateFailureSkipsDeletes(t *testing.T) {
	t.Parallel()
	deletes := 0
	mock := &kafka.MockKafkaManager{
		ListACLsFn: func(context.Context, string, string, string, bool) (*kafka.ACLResult, error) {
			return &kafka.ACLResult{ACLs: []kafka.ACLEntry{{
				Principal: "User:app1", Host: "*", ResourceType: "TOPIC",
				ResourceName: "orders", Operation: "WRITE",
				Permission: "ALLOW", PatternType: "LITERAL",
			}}}, nil
		},
		CreateACLFn: func(context.Context, string, kafka.ACLEntry) error {
			return errors.New("broker rejected create")
		},
		DeleteACLFn: func(context.Context, string, kafka.ACLEntry) (int, error) {
			deletes++
			return 1, nil
		},
	}
	s := aclTestServer(t, mock)
	result, err := s.handleACLManage(context.Background(), makeToolRequest(map[string]any{
		"action": "apply", "principal": "User:app1",
		"rules": []any{map[string]any{
			"resource_type": "topic", "resource_name": "orders", "operation": "read",
		}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	assertIsError(t, result)
	if deletes != 0 {
		t.Fatalf("delete calls = %d, want 0 after create failure", deletes)
	}
	if !strings.Contains(resultText(t, result), "deletions skipped") {
		t.Fatalf("expected skipped-deletion details, got %s", resultText(t, result))
	}
}

func TestHandleACLManage_List(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListACLsFn: func(ctx context.Context, cluster, principal, resourceType string, includeAnalysis bool) (*kafka.ACLResult, error) {
			return &kafka.ACLResult{
				ACLs: []kafka.ACLEntry{
					{Principal: "User:app1", Host: "*", ResourceType: "TOPIC", ResourceName: "orders", Operation: "READ", Permission: "ALLOW", PatternType: "LITERAL"},
				},
				TotalACLs: 1,
			}, nil
		},
	}
	s := aclTestServer(t, mock)
	result, err := s.handleACLManage(context.Background(), makeToolRequest(map[string]any{"action": "list"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	if !strings.Contains(resultText(t, result), "orders") {
		t.Errorf("expected orders in result, got %s", resultText(t, result))
	}
	if mock.CallCount("ListACLs") != 1 {
		t.Errorf("expected ListACLs called once, got %d", mock.CallCount("ListACLs"))
	}
}

func TestHandleACLManage_Audit(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListACLsFn: func(ctx context.Context, cluster, principal, resourceType string, includeAnalysis bool) (*kafka.ACLResult, error) {
			return &kafka.ACLResult{
				ACLs: []kafka.ACLEntry{
					{Principal: "User:app1", Host: "*", ResourceType: "topic", ResourceName: "*", Operation: "all", Permission: "allow", PatternType: "literal"},
				},
				TotalACLs: 1,
			}, nil
		},
	}
	s := aclTestServer(t, mock)
	result, err := s.handleACLManage(context.Background(), makeToolRequest(map[string]any{"action": "audit"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	// The wildcard + all-operation rule should surface recommendations.
	if !strings.Contains(resultText(t, result), "wildcard") && !strings.Contains(resultText(t, result), "least-privilege") {
		t.Errorf("expected audit findings, got %s", resultText(t, result))
	}
}

func TestHandleACLManage_Diff(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListACLsFn: func(ctx context.Context, cluster, principal, resourceType string, includeAnalysis bool) (*kafka.ACLResult, error) {
			// Existing broker ACL: write on orders (will be obsolete).
			return &kafka.ACLResult{
				ACLs: []kafka.ACLEntry{
					{Principal: "User:app1", Host: "*", ResourceType: "topic", ResourceName: "orders", Operation: "write", Permission: "allow", PatternType: "literal"},
				},
			}, nil
		},
	}
	s := aclTestServer(t, mock)
	req := makeToolRequest(map[string]any{
		"action":    "diff",
		"principal": "User:app1",
		"rules": []any{
			map[string]any{"resource_type": "topic", "resource_name": "orders", "operation": "read"},
		},
	})
	result, err := s.handleACLManage(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "to_add") || !strings.Contains(text, "to_remove") {
		t.Errorf("expected diff sections, got %s", text)
	}
}

func TestHandleACLManage_Apply(t *testing.T) {
	t.Parallel()
	var created, deleted int
	mock := &kafka.MockKafkaManager{
		ListACLsFn: func(ctx context.Context, cluster, principal, resourceType string, includeAnalysis bool) (*kafka.ACLResult, error) {
			// Broker currently has a write ACL that is not in desired → obsolete.
			return &kafka.ACLResult{
				ACLs: []kafka.ACLEntry{
					{Principal: "User:app1", Host: "*", ResourceType: "topic", ResourceName: "orders", Operation: "write", Permission: "allow", PatternType: "literal"},
				},
			}, nil
		},
		CreateACLFn: func(ctx context.Context, cluster string, rule kafka.ACLEntry) error {
			created++
			return nil
		},
		DeleteACLFn: func(ctx context.Context, cluster string, rule kafka.ACLEntry) (int, error) {
			deleted++
			return 1, nil
		},
	}
	s := aclTestServer(t, mock)
	req := makeToolRequest(map[string]any{
		"action":    "apply",
		"principal": "User:app1",
		"rules": []any{
			map[string]any{"resource_type": "topic", "resource_name": "orders", "operation": "read"},
		},
	})
	result, err := s.handleACLManage(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	if created != 1 {
		t.Errorf("expected 1 CreateACL call, got %d", created)
	}
	if deleted != 1 {
		t.Errorf("expected 1 DeleteACL call, got %d", deleted)
	}
	text := resultText(t, result)
	if !strings.Contains(text, "\"created\": 1") || !strings.Contains(text, "\"deleted\": 1") {
		t.Errorf("expected created/deleted counts, got %s", text)
	}
}

func TestHandleACLManage_InvalidRule(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := aclTestServer(t, mock)
	req := makeToolRequest(map[string]any{
		"action": "diff",
		"rules": []any{
			// Missing principal and invalid principal format.
			map[string]any{"resource_type": "topic", "resource_name": "orders", "operation": "read"},
		},
	})
	result, err := s.handleACLManage(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	if !strings.Contains(resultText(t, result), "validation_failed") {
		t.Errorf("expected validation_failed, got %s", resultText(t, result))
	}
}

func TestHandleACLManage_UnknownAction(t *testing.T) {
	t.Parallel()
	s := aclTestServer(t, &kafka.MockKafkaManager{})
	result, err := s.handleACLManage(context.Background(), makeToolRequest(map[string]any{"action": "bogus"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestRequiresApproval_ACLManage(t *testing.T) {
	t.Parallel()
	cases := map[string]bool{
		"list":      false,
		"audit":     false,
		"diff":      false,
		"dry_run":   false,
		"apply":     true,
		"reconcile": true,
	}
	for action, want := range cases {
		got := requiresApproval("kafka_acl_manage", map[string]any{"action": action})
		if got != want {
			t.Errorf("requiresApproval(acl, %s) = %v, want %v", action, got, want)
		}
	}
}
