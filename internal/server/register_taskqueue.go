package server

import (
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerTaskQueueTools() {
	if s.taskQueueEngine == nil {
		return
	}

	s.mcpServer.AddTool(mcp.NewTool("kafka_task_publish",
		mcp.WithDescription("Publish a task to a priority queue for other agents to claim and execute."),
		mcp.WithString("queue", mcp.Required(), mcp.Description("Queue name")),
		mcp.WithString("type", mcp.Required(), mcp.Description("Task type")),
		mcp.WithNumber("priority", mcp.Description("Priority: 0=critical, 1=high, 2=medium, 3=low (default: 2)")),
		mcp.WithObject("payload", mcp.Description("Task payload data")),
		mcp.WithNumber("max_retries", mcp.Description("Max retry attempts (default: 3)")),
		mcp.WithNumber("deadline_sec", mcp.Description("Deadline in seconds from now")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_task_publish",
		resourceType: "topic",
		action:       "write",
	}, s.handleTaskPublish))

	s.mcpServer.AddTool(mcp.NewTool("kafka_task_claim",
		mcp.WithDescription("Claim the highest-priority pending task from a queue."),
		mcp.WithString("queue", mcp.Required(), mcp.Description("Queue name")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_task_claim",
		resourceType: "topic",
		action:       "write",
	}, s.handleTaskClaim))

	s.mcpServer.AddTool(mcp.NewTool("kafka_task_complete",
		mcp.WithDescription("Mark a claimed task as completed or failed."),
		mcp.WithString("task_id", mcp.Required(), mcp.Description("Task ID")),
		mcp.WithString("status", mcp.Required(), mcp.Description("Status: completed or failed")),
		mcp.WithObject("result", mcp.Description("Task result data")),
		mcp.WithString("error", mcp.Description("Error message (for failed status)")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_task_complete",
		resourceType: "topic",
		action:       "write",
	}, s.handleTaskComplete))

	s.mcpServer.AddTool(mcp.NewTool("kafka_task_status",
		mcp.WithDescription("Get task status, queue statistics, or list tasks. Actions: get, stats, list, heartbeat."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: get, stats, list, heartbeat")),
		mcp.WithString("task_id", mcp.Description("Task ID (for get/heartbeat)")),
		mcp.WithString("queue", mcp.Description("Queue name (for stats/list)")),
		mcp.WithString("filter_status", mcp.Description("Filter by status (for list)")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_task_status",
		resourceType: "topic",
		action:       "read",
	}, s.handleTaskStatus))
}
