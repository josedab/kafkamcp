package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerMultiAgentOrchestratorTools() {
	s.addTool(mcp.NewTool("kafka_multi_agent_orchestrate",
		withCategory("orchestration"),
		mcp.WithDescription("Multi-agent DAG workflow coordination. Assign steps to agents, manage handoffs, and track per-agent progress. Actions: assign, handoff, agent_steps, agent_ready, status."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: assign, handoff, agent_steps, agent_ready, status")),
		mcp.WithString("run_id", mcp.Required(), mcp.Description("Workflow run ID")),
		mcp.WithString("agent_id", mcp.Description("Agent ID")),
		mcp.WithObject("assignments", mcp.Description("Array of {step_id, agent_id, agent_role} (for assign action)")),
		mcp.WithString("from_agent", mcp.Description("Source agent (for handoff)")),
		mcp.WithString("to_agent", mcp.Description("Target agent (for handoff)")),
		mcp.WithString("step_id", mcp.Description("Step ID (for handoff)")),
		mcp.WithObject("data", mcp.Description("Handoff data payload")),
	), s.handleMultiAgentOrchestrate)
}
