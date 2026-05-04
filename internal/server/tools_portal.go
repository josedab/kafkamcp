package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/portal"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleGovernanceRequest(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_governance_request", resourceType: auth.ResourceCluster, action: auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "list")

		switch action {
		case "submit":
			reqType := portal.RequestType(request.GetString("type", ""))
			title := request.GetString("title", "")
			description := request.GetString("description", "")
			cluster := request.GetString("cluster", "")
			topic := request.GetString("topic", "")

			if reqType == "" || title == "" {
				return mcp.NewToolResultError("type and title are required"), nil
			}

			req, err := s.portalEngine.SubmitRequest(reqType, agentID, title, description, cluster, topic, nil)
			if err != nil {
				return s.toolError("failed to submit request", err), nil
			}
			s.logAudit(agentID, "kafka_governance_request", args, cluster, "success", 1, nil, start)
			return marshalResult(map[string]any{
				"request": req,
				"message": "Request submitted. Pending approval.",
			})

		case "review":
			requestID := request.GetString("request_id", "")
			decision := request.GetString("decision", "")
			comment := request.GetString("comment", "")
			if requestID == "" || decision == "" {
				return mcp.NewToolResultError("request_id and decision are required"), nil
			}

			req, err := s.portalEngine.ReviewRequest(requestID, agentID, decision, comment)
			if err != nil {
				return s.toolError("review failed", err), nil
			}
			s.logAudit(agentID, "kafka_governance_request", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"request": req, "message": "Review recorded."})

		case "get":
			requestID := request.GetString("request_id", "")
			if requestID == "" {
				return mcp.NewToolResultError("request_id is required"), nil
			}
			req, err := s.portalEngine.GetRequest(requestID)
			if err != nil {
				return s.toolError("request not found", err), nil
			}
			return marshalResult(req)

		default:
			status := portal.RequestStatus(request.GetString("status", ""))
			reqType := portal.RequestType(request.GetString("type", ""))
			reqs := s.portalEngine.ListRequests(status, reqType)
			return marshalResult(map[string]any{"requests": reqs, "total": len(reqs)})
		}
	})(ctx, request)
}

func (s *Server) handleGovernanceDashboard(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_governance_dashboard", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		dashboard := s.portalEngine.GetDashboard()
		s.logAudit(agentID, "kafka_governance_dashboard", args, "", "success", 1, nil, start)
		return marshalResult(dashboard)
	})(ctx, request)
}

func (s *Server) handleGovernanceWorkflows(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_governance_workflows", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		workflows := s.portalEngine.GetWorkflows()
		return marshalResult(map[string]any{"workflows": workflows, "total": len(workflows)})
	})(ctx, request)
}
