package server

import (
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerDAGWorkflowTools() {
	if s.dagWorkflowEngine == nil {
		return
	}

	s.mcpServer.AddTool(mcp.NewTool("kafka_workflow_create",
		withCategory("ops"),
		mcp.WithDescription("Create a reusable DAG-based workflow template for multi-step Kafka operations with conditional branching, parallel fan-out, and error recovery."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Workflow name")),
		mcp.WithString("description", mcp.Description("Workflow description")),
		mcp.WithObject("steps", mcp.Required(), mcp.Description("Array of DAG steps: [{id, name, tool, args, depends_on, condition, on_error, max_retries}]")),
		mcp.WithObject("parameters", mcp.Description("Array of input parameters: [{name, description, type, required, default}]")),
		mcp.WithObject("tags", mcp.Description("Tags for categorization")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_workflow_create",
		resourceType: "topic",
		action:       "write",
	}, s.handleDAGWorkflowCreate))

	s.mcpServer.AddTool(mcp.NewTool("kafka_workflow_run",
		withCategory("ops"),
		mcp.WithDescription("Start a workflow execution from a template. Returns the run ID and initial ready steps for the agent to execute."),
		mcp.WithString("template_id", mcp.Required(), mcp.Description("Workflow template ID")),
		mcp.WithObject("parameters", mcp.Description("Input parameters for the workflow")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_workflow_run",
		resourceType: "topic",
		action:       "write",
	}, s.handleDAGWorkflowRun))

	s.mcpServer.AddTool(mcp.NewTool("kafka_workflow_status",
		withCategory("ops"),
		mcp.WithDescription("Get workflow status, manage workflows, or advance a running workflow. Actions: status, list, versions, cancel, complete_step, ready_steps."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: status, list, versions, cancel, complete_step, ready_steps")),
		mcp.WithString("run_id", mcp.Description("Run ID (for status, cancel, complete_step, ready_steps)")),
		mcp.WithString("template_id", mcp.Description("Template ID (for versions)")),
		mcp.WithString("step_id", mcp.Description("Step ID (for complete_step)")),
		mcp.WithString("step_status", mcp.Description("Step status: completed, failed, skipped (for complete_step)")),
		mcp.WithObject("step_output", mcp.Description("Step output data (for complete_step)")),
		mcp.WithString("step_error", mcp.Description("Error message (for complete_step with failed status)")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_workflow_status",
		resourceType: "topic",
		action:       "read",
	}, s.handleDAGWorkflowStatus))
}
