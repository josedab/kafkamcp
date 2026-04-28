package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerOrchestrationTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_orchestration_create",
		withCategory("ops"),
		mcp.WithDescription("Create a multi-agent orchestration that coordinates agents working together on Kafka operations with step dependencies, role-based assignment, and shared context."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Orchestration name")),
		mcp.WithString("description", mcp.Description("Description of the orchestration")),
	), s.handleOrchestrationCreate)

	s.mcpServer.AddTool(mcp.NewTool("kafka_orchestration_status",
		withCategory("ops"),
		mcp.WithDescription("Get status of an orchestration, list all orchestrations, or get ready steps for execution."),
		mcp.WithString("orchestration_id", mcp.Description("Orchestration ID (omit to list all)")),
		mcp.WithString("action", mcp.Description("Action: status, ready_steps, context. Default: status")),
	), s.handleOrchestrationStatus)

	s.mcpServer.AddTool(mcp.NewTool("kafka_orchestration_complete_step",
		withCategory("ops"),
		mcp.WithDescription("Complete a step in an orchestration with results, enabling dependent steps to proceed."),
		mcp.WithString("orchestration_id", mcp.Required(), mcp.Description("Orchestration ID")),
		mcp.WithString("step_id", mcp.Required(), mcp.Description("Step ID to complete")),
		mcp.WithString("status", mcp.Required(), mcp.Description("Step status: completed, failed, skipped")),
		mcp.WithString("error", mcp.Description("Error message if failed")),
	), s.handleOrchestrationCompleteStep)

	s.mcpServer.AddTool(mcp.NewTool("kafka_orchestration_handoff",
		withCategory("ops"),
		mcp.WithDescription("Create a task handoff between agents with context and message for collaborative operations."),
		mcp.WithString("from_agent", mcp.Required(), mcp.Description("Source agent ID")),
		mcp.WithString("to_agent", mcp.Required(), mcp.Description("Target agent ID")),
		mcp.WithString("message", mcp.Required(), mcp.Description("Handoff message describing the task")),
	), s.handleOrchestrationHandoff)

	s.mcpServer.AddTool(mcp.NewTool("kafka_orchestration_agents",
		withCategory("ops"),
		mcp.WithDescription("Register an agent for orchestration or list available agents."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: register, list, heartbeat")),
		mcp.WithString("agent_id", mcp.Description("Agent ID (required for register/heartbeat)")),
		mcp.WithString("name", mcp.Description("Agent name (for register)")),
		mcp.WithString("role", mcp.Description("Agent role: monitor, operator, analyst, security, pipeline, coordinator")),
	), s.handleOrchestrationAgents)
}
