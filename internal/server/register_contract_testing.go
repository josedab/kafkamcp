package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerContractTestingTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_contract_test",
		withCategory("governance"),
		mcp.WithDescription("Schema contract testing between producers and consumers. Actions: register_pair, register_schema, test_compatibility, create_suite, run_suite, detect_changes, list_pairs, list_suites."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action to perform")),
		mcp.WithString("topic", mcp.Description("Topic name")),
		mcp.WithString("producer_id", mcp.Description("Producer service ID")),
		mcp.WithObject("consumer_ids", mcp.Description("Consumer service IDs array")),
		mcp.WithString("service_id", mcp.Description("Service ID for schema registration")),
		mcp.WithString("role", mcp.Description("Role: producer or consumer")),
		mcp.WithObject("fields", mcp.Description("Schema field names array")),
		mcp.WithString("suite_id", mcp.Description("Test suite ID")),
		mcp.WithString("suite_name", mcp.Description("Test suite name")),
		mcp.WithObject("test_cases", mcp.Description("Array of test cases")),
		mcp.WithObject("old_fields", mcp.Description("Previous field set for change detection")),
		mcp.WithObject("new_fields", mcp.Description("New field set for change detection")),
	), s.handleContractTest)
}
