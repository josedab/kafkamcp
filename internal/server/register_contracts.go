package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerContractTools() {
	s.addTool(mcp.NewTool("kafka_contract_violations",
		withCategory("governance"),
		mcp.WithDescription("View contract violations, enforce contracts on messages, set enforcement mode, or infer contracts from samples. Actions: violations (get violation summary), enforce (validate with enforcement), set_mode (set enforcement mode: warn/reject/quarantine), infer (generate contract from samples)."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: violations, enforce, set_mode, infer")),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
		mcp.WithObject("message", mcp.Description("Message to enforce (for enforce action)")),
		mcp.WithString("mode", mcp.Description("Enforcement mode: warn, reject, quarantine (for set_mode action)")),
		mcp.WithObject("samples", mcp.Description("Array of message samples for contract inference (for infer action)")),
	), s.handleContractViolations)
}
