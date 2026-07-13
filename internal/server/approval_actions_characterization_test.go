package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/josedab/kafkamcp/internal/audit"
)

func TestApprovalAndGovernanceActionsCharacterization(t *testing.T) {
	s := governanceTestServer(t)
	auditLogger, err := audit.NewLogger(context.Background(), true, 100, "", s.logger)
	if err != nil {
		t.Fatalf("audit logger: %v", err)
	}
	t.Cleanup(func() { _ = auditLogger.Close() })
	s.auditLogger = auditLogger
	s.approvalManager.SetAudit(s.approvalAuditHook())

	requestArgs := map[string]any{
		"action":         "request",
		"tool":           "kafka_create_topic",
		"request_reason": "characterize current approval behavior",
		"args": map[string]any{
			"name":               "orders",
			"partitions":         1,
			"replication_factor": 1,
		},
	}
	requestResult := callApprovalAction(t, s, requestForAgent(requestArgs, "requester"))
	requestPayload := decodeToolPayload(t, requestResult)
	grant := requestPayload["grant"].(map[string]any)
	grantID := grant["id"].(string)
	if scope := grant["scope"].(map[string]any); scope["agent_id"] != "requester" {
		t.Fatalf("request grant scope = %#v", scope)
	}

	getPayload := decodeToolPayload(t, callApprovalAction(t, s, requestForAgent(map[string]any{
		"action": "get", "grant_id": grantID,
	}, "requester")))
	if getPayload["action"] != "get" {
		t.Fatalf("get payload = %#v", getPayload)
	}

	listPayload := decodeToolPayload(t, callApprovalAction(t, s, requestForAgent(map[string]any{
		"action": "list",
	}, "requester")))
	if listPayload["total"] != float64(1) {
		t.Fatalf("list payload = %#v", listPayload)
	}

	deniedList, err := s.handleApproval(context.Background(), requestForAgent(map[string]any{
		"action": "list", "agent_id": "other-agent",
	}, "requester"))
	if err != nil {
		t.Fatalf("list another agent: %v", err)
	}
	assertIsError(t, deniedList)

	approvePayload := decodeToolPayload(t, callApprovalAction(t, s, requestForAgent(map[string]any{
		"action": "approve", "grant_id": grantID, "ttl_seconds": 60,
	}, "operator")))
	if approvePayload["approval_token"] == "" {
		t.Fatalf("approve payload = %#v", approvePayload)
	}

	verifyPayload := decodeToolPayload(t, callGovernanceAction(t, s, requestForAgent(map[string]any{
		"action": "verify", "grant_id": grantID,
	}, "requester")))
	if verifyPayload["valid"] != true {
		t.Fatalf("verify payload = %#v", verifyPayload)
	}

	exportPayload := decodeToolPayload(t, callGovernanceAction(t, s, requestForAgent(map[string]any{
		"action": "export",
	}, "requester")))
	if exportPayload["total"] != float64(1) {
		t.Fatalf("export payload = %#v", exportPayload)
	}

	simulatePayload := decodeToolPayload(t, callGovernanceAction(t, s, requestForAgent(map[string]any{
		"action": "simulate",
		"tool":   "kafka_create_topic",
		"args": map[string]any{
			"name":               "payments",
			"partitions":         2,
			"replication_factor": 1,
		},
	}, "requester")))
	if simulatePayload["action"] != "simulate" || simulatePayload["approval_required"] != nil {
		t.Fatalf("simulate payload = %#v", simulatePayload)
	}

	revokePayload := decodeToolPayload(t, callApprovalAction(t, s, requestForAgent(map[string]any{
		"action": "revoke", "grant_id": grantID, "reason": "characterization complete",
	}, "requester")))
	if revokePayload["action"] != "revoke" {
		t.Fatalf("revoke payload = %#v", revokePayload)
	}

	statusPayload := decodeToolPayload(t, callApprovalAction(t, s, requestForAgent(map[string]any{
		"action": "status",
	}, "requester")))
	if statusPayload["enforced"] != true {
		t.Fatalf("status payload = %#v", statusPayload)
	}

	entries := s.auditLogger.RecentEntries(100)
	counts := make(map[string]int)
	for _, entry := range entries {
		counts[entry.Tool]++
	}
	wantCounts := map[string]int{
		"kafka_approval.request":    1,
		"kafka_approval.approve":    1,
		"kafka_approval.revoke":     1,
		"kafka_approval":            3,
		"kafka_governance_decision": 1,
	}
	for tool, want := range wantCounts {
		if got := counts[tool]; got != want {
			t.Fatalf("audit count for %s = %d, want %d; entries=%#v", tool, got, want, entries)
		}
	}
	if len(entries) != 7 {
		t.Fatalf("audit entries = %d, want 7; query/status/verify/export actions currently do not audit success", len(entries))
	}
}

func requestForAgent(args map[string]any, agentID string) mcp.CallToolRequest {
	request := makeToolRequest(args)
	request.Header = make(http.Header)
	request.Header.Set("X-Agent-ID", agentID)
	return request
}

func callApprovalAction(t *testing.T, s *Server, request mcp.CallToolRequest) *mcp.CallToolResult {
	t.Helper()
	result, err := s.handleApproval(context.Background(), request)
	if err != nil {
		t.Fatalf("handleApproval: %v", err)
	}
	if result == nil || result.IsError {
		t.Fatalf("handleApproval returned error result: %s", resultText(t, result))
	}
	return result
}

func callGovernanceAction(t *testing.T, s *Server, request mcp.CallToolRequest) *mcp.CallToolResult {
	t.Helper()
	result, err := s.handleGovernanceDecision(context.Background(), request)
	if err != nil {
		t.Fatalf("handleGovernanceDecision: %v", err)
	}
	if result == nil || result.IsError {
		t.Fatalf("handleGovernanceDecision returned error result: %s", resultText(t, result))
	}
	return result
}

func decodeToolPayload(t *testing.T, result *mcp.CallToolResult) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(resultText(t, result)), &payload); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	return payload
}
