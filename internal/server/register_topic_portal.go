package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/topicportal"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerTopicPortalTools() {
	if s.topicPortalEngine == nil {
		return
	}

	s.mcpServer.AddTool(mcp.NewTool("kafka_topic_request",
		withCategory("provisioning"),
		mcp.WithDescription("Submit a self-service topic provisioning request with justification and approval chain."),
		mcp.WithString("topic_name", mcp.Required(), mcp.Description("Desired topic name")),
		mcp.WithString("justification", mcp.Required(), mcp.Description("Why this topic is needed")),
		mcp.WithString("owner", mcp.Description("Team or individual owning this topic")),
		mcp.WithNumber("partitions", mcp.Description("Number of partitions. Default: 1")),
		mcp.WithNumber("replication_factor", mcp.Description("Replication factor. Default: 3")),
		mcp.WithNumber("retention_ms", mcp.Description("Retention in milliseconds. Default: 7 days")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_topic_request",
		resourceType: "topic",
		action:       "create",
	}, s.handleTopicRequest))

	s.mcpServer.AddTool(mcp.NewTool("kafka_topic_approve",
		withCategory("provisioning"),
		mcp.WithDescription("Approve a pending topic provisioning request."),
		mcp.WithString("request_id", mcp.Required(), mcp.Description("Request ID to approve")),
		mcp.WithString("comment", mcp.Description("Approval comment")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_topic_approve",
		resourceType: "cluster",
		action:       "write",
	}, s.handleTopicApprove))

	s.mcpServer.AddTool(mcp.NewTool("kafka_topic_request_status",
		withCategory("provisioning"),
		mcp.WithDescription("Check the status of topic provisioning requests."),
		mcp.WithString("request_id", mcp.Description("Specific request ID (omit to list all)")),
		mcp.WithString("status", mcp.Description("Filter by status: pending, approved, rejected, created")),
		mcp.WithNumber("limit", mcp.Description("Max results. Default: 50")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_topic_request_status",
		resourceType: "cluster",
		action:       "read",
	}, s.handleTopicRequestStatus))

	s.mcpServer.AddTool(mcp.NewTool("kafka_topic_reject",
		withCategory("provisioning"),
		mcp.WithDescription("Reject a pending topic provisioning request."),
		mcp.WithString("request_id", mcp.Required(), mcp.Description("Request ID to reject")),
		mcp.WithString("reason", mcp.Required(), mcp.Description("Rejection reason")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_topic_reject",
		resourceType: "cluster",
		action:       "write",
	}, s.handleTopicReject))
}

func (s *Server) handleTopicRequest(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	topicName, _ := args["topic_name"].(string)
	justification, _ := args["justification"].(string)
	if topicName == "" || justification == "" {
		return mcp.NewToolResultError("topic_name and justification are required"), nil
	}

	req := topicportal.TopicRequest{
		TopicName:     topicName,
		RequestedBy:   agentID,
		Justification: justification,
	}
	if owner, ok := args["owner"].(string); ok {
		req.Owner = owner
	}
	if p, ok := args["partitions"].(float64); ok {
		req.Partitions = int(p)
	}
	if rf, ok := args["replication_factor"].(float64); ok {
		req.ReplicationFactor = int(rf)
	}
	if ret, ok := args["retention_ms"].(float64); ok {
		req.RetentionMs = int64(ret)
	}

	result, err := s.topicPortalEngine.SubmitRequest(req)
	if err != nil {
		s.logAudit(agentID, "kafka_topic_request", args, "", "error", 0, err, start)
		return s.toolError("failed to submit topic request", err), nil
	}

	s.logAudit(agentID, "kafka_topic_request", args, "", "success", 1, nil, start)
	return marshalResult(result)
}

func (s *Server) handleTopicApprove(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	requestID, _ := args["request_id"].(string)
	comment, _ := args["comment"].(string)
	if requestID == "" {
		return mcp.NewToolResultError("request_id is required"), nil
	}

	err := s.topicPortalEngine.Approve(requestID, agentID, comment)
	if err != nil {
		s.logAudit(agentID, "kafka_topic_approve", args, "", "error", 0, err, start)
		return s.toolError("failed to approve request", err), nil
	}

	s.logAudit(agentID, "kafka_topic_approve", args, "", "success", 1, nil, start)
	return marshalResult(map[string]any{
		"status":     "approved",
		"request_id": requestID,
	})
}

func (s *Server) handleTopicRequestStatus(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	requestID, _ := args["request_id"].(string)

	if requestID != "" {
		req, ok := s.topicPortalEngine.GetRequest(requestID)
		if !ok {
			return marshalResult(map[string]any{"request_id": requestID, "status": "not_found"})
		}
		s.logAudit(agentID, "kafka_topic_request_status", args, "", "success", 1, nil, start)
		return marshalResult(req)
	}

	status := topicportal.RequestStatus("")
	if s, ok := args["status"].(string); ok {
		status = topicportal.RequestStatus(s)
	}
	limit := 50
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}

	requests := s.topicPortalEngine.ListRequests(status, limit)
	s.logAudit(agentID, "kafka_topic_request_status", args, "", "success", len(requests), nil, start)
	return marshalResult(map[string]any{
		"requests": requests,
		"total":    len(requests),
	})
}

func (s *Server) handleTopicReject(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	requestID, _ := args["request_id"].(string)
	reason, _ := args["reason"].(string)
	if requestID == "" || reason == "" {
		return mcp.NewToolResultError("request_id and reason are required"), nil
	}

	err := s.topicPortalEngine.Reject(requestID, agentID, reason)
	if err != nil {
		s.logAudit(agentID, "kafka_topic_reject", args, "", "error", 0, err, start)
		return s.toolError("failed to reject request", err), nil
	}

	s.logAudit(agentID, "kafka_topic_reject", args, "", "success", 1, nil, start)
	return marshalResult(map[string]any{
		"status":     "rejected",
		"request_id": requestID,
	})
}
