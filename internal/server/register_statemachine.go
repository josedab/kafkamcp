package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerStateMachineTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_state_machine",
		withCategory("streaming"),
		mcp.WithDescription("Kafka state machine engine. Define topic-backed finite state machines, query current state, valid transitions, and execute saga patterns with compensation logic."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: list_machines, get_machine, create_machine, create_instance, get_instance, list_instances, transition, valid_transitions, start_saga, complete_saga_step")),
		mcp.WithString("id", mcp.Description("Machine ID or instance ID")),
		mcp.WithString("definition", mcp.Description("State machine definition as YAML (for create_machine)")),
		mcp.WithString("event", mcp.Description("Transition event (for transition)")),
		mcp.WithObject("data", mcp.Description("Instance data (for create_instance)")),
		mcp.WithBoolean("success", mcp.Description("Saga step success (for complete_saga_step)")),
		mcp.WithString("error_message", mcp.Description("Error message (for complete_saga_step failure)")),
	), s.handleStateMachine)
}
