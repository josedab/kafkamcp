package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerControlPlaneTools() {
	s.addTool(mcp.NewTool("kafka_controlplane_org",
		withCategory("platform"),
		withTier(TierExperimental),
		mcp.WithDescription("Manage organizations in the multi-tenant SaaS control plane. Create, list, or get organization details including API keys and members."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: create, get, list, add_member, create_api_key")),
		mcp.WithString("org_id", mcp.Description("Organization ID (for get/add_member/create_api_key)")),
		mcp.WithString("name", mcp.Description("Organization name (for create) or API key name")),
		mcp.WithString("plan", mcp.Description("Plan: starter, pro, enterprise (for create)")),
		mcp.WithString("email", mcp.Description("Member email (for add_member)")),
		mcp.WithString("role", mcp.Description("Member role: admin, editor, viewer (for add_member)")),
	), s.handleControlPlaneOrg)

	s.addTool(mcp.NewTool("kafka_controlplane_instance",
		withCategory("platform"),
		withTier(TierExperimental),
		mcp.WithDescription("Manage KafkaMCP instances across organizations. Provision, start, stop, terminate instances with lifecycle management."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: provision, get, list, start, stop, terminate")),
		mcp.WithString("instance_id", mcp.Description("Instance ID (for get/start/stop/terminate)")),
		mcp.WithString("org_id", mcp.Description("Organization ID (for provision/list)")),
		mcp.WithString("name", mcp.Description("Instance name (for provision)")),
		mcp.WithString("tier", mcp.Description("Instance tier: standard, premium, enterprise")),
		mcp.WithString("region", mcp.Description("Deployment region")),
		mcp.WithString("bootstrap_servers", mcp.Description("Kafka bootstrap servers (for provision)")),
	), s.handleControlPlaneInstance)

	s.addTool(mcp.NewTool("kafka_controlplane_usage",
		withCategory("platform"),
		withTier(TierExperimental),
		mcp.WithDescription("View usage and billing metrics for an organization including tool calls, messages, instance hours, and estimated costs."),
		mcp.WithString("org_id", mcp.Required(), mcp.Description("Organization ID")),
		mcp.WithString("period", mcp.Description("Billing period: current, last_month. Default: current")),
	), s.handleControlPlaneUsage)
}
