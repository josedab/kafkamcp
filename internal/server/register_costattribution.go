package server

import (
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerCostAttributionTools() {
	if s.costAttrEngine == nil {
		return
	}

	s.addTool(mcp.NewTool("kafka_cost_live",
		withCategory("governance"),
		mcp.WithDescription("Get real-time cost metrics for a resource or all resources with per-hour/day/month breakdowns."),
		mcp.WithString("resource_type", mcp.Description("Resource type: topic, consumer_group, connector")),
		mcp.WithString("resource_name", mcp.Description("Resource name (omit for all)")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_cost_live",
		resourceType: "topic",
		action:       "read",
	}, s.handleCostLive))

	s.addTool(mcp.NewTool("kafka_cost_allocate",
		withCategory("governance"),
		mcp.WithDescription("Create or view cost allocation groups that map resources to teams, services, or projects."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: create, list")),
		mcp.WithString("name", mcp.Description("Allocation group name (for create)")),
		mcp.WithString("owner", mcp.Description("Team/project owner (for create)")),
		mcp.WithObject("resources", mcp.Description("Resource names to include (for create)")),
		mcp.WithObject("tags", mcp.Description("Tags for the allocation group")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_cost_allocate",
		resourceType: "topic",
		action:       "write",
	}, s.handleCostAllocate))

	s.addTool(mcp.NewTool("kafka_cost_budget",
		withCategory("governance"),
		mcp.WithDescription("Create spending budgets with alert thresholds or view budget status."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: create, list")),
		mcp.WithString("name", mcp.Description("Budget name (for create)")),
		mcp.WithNumber("monthly_limit", mcp.Description("Monthly spending limit in USD (for create)")),
		mcp.WithObject("resources", mcp.Description("Resources to track (for create)")),
		mcp.WithObject("alert_thresholds", mcp.Description("Alert thresholds as fractions, e.g. [0.5, 0.8, 0.9, 1.0]")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_cost_budget",
		resourceType: "topic",
		action:       "write",
	}, s.handleCostBudget))
}
