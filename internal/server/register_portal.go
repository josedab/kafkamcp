package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerGovernancePortalTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_governance_request",
		withCategory("governance"),
		mcp.WithDescription("Submit, review, or manage governance requests for topic provisioning, schema changes, access grants, and more. Self-service workflow with approval chains."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: submit, review, get, list")),
		mcp.WithString("type", mcp.Description("Request type: topic_create, topic_modify, topic_delete, schema_change, access_grant, access_revoke")),
		mcp.WithString("title", mcp.Description("Request title (for submit)")),
		mcp.WithString("description", mcp.Description("Request description")),
		mcp.WithString("cluster", mcp.Description("Cluster name")),
		mcp.WithString("topic", mcp.Description("Topic name")),
		mcp.WithString("request_id", mcp.Description("Request ID (for review/get)")),
		mcp.WithString("decision", mcp.Description("Review decision: approved, rejected (for review)")),
		mcp.WithString("comment", mcp.Description("Review comment")),
		mcp.WithString("status", mcp.Description("Filter by status (for list): pending, approved, rejected, completed")),
	), s.handleGovernanceRequest)

	s.mcpServer.AddTool(mcp.NewTool("kafka_governance_dashboard",
		withCategory("governance"),
		mcp.WithDescription("Get the governance dashboard with request statistics, compliance score, cost summary, and recent activity."),
	), s.handleGovernanceDashboard)

	s.mcpServer.AddTool(mcp.NewTool("kafka_governance_workflows",
		withCategory("governance"),
		mcp.WithDescription("List configured governance workflow templates showing approval chains for each request type."),
	), s.handleGovernanceWorkflows)
}
