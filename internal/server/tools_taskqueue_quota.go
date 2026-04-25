package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/adaptiverate"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleTaskPublish(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	queue, _ := args["queue"].(string)
	taskType, _ := args["type"].(string)

	if queue == "" || taskType == "" {
		return mcp.NewToolResultError("queue and type are required"), nil
	}

	priority := 2
	if v, ok := args["priority"].(float64); ok {
		priority = int(v)
	}

	maxRetries := 3
	if v, ok := args["max_retries"].(float64); ok {
		maxRetries = int(v)
	}

	var deadlineSec int64
	if v, ok := args["deadline_sec"].(float64); ok {
		deadlineSec = int64(v)
	}

	payload, _ := args["payload"].(map[string]any)

	task, err := s.taskQueueEngine.Publish(queue, taskType, agentID, priority, maxRetries, payload, deadlineSec)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	s.logAudit(agentID, "kafka_task_publish", args, "", "success", 1, nil, start)
	return marshalResult(task)
}

func (s *Server) handleTaskClaim(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	queue, _ := args["queue"].(string)
	if queue == "" {
		return mcp.NewToolResultError("queue is required"), nil
	}

	task, err := s.taskQueueEngine.Claim(agentID, queue)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	s.logAudit(agentID, "kafka_task_claim", args, "", "success", 1, nil, start)
	return marshalResult(task)
}

func (s *Server) handleTaskComplete(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	taskID, _ := args["task_id"].(string)
	status, _ := args["status"].(string)
	result := args["result"]
	errMsg, _ := args["error"].(string)

	if taskID == "" || status == "" {
		return mcp.NewToolResultError("task_id and status are required"), nil
	}

	task, err := s.taskQueueEngine.Complete(agentID, taskID, status, result, errMsg)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	s.logAudit(agentID, "kafka_task_complete", args, "", "success", 1, nil, start)
	return marshalResult(task)
}

func (s *Server) handleTaskStatus(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	action, _ := args["action"].(string)

	switch action {
	case "get":
		taskID, _ := args["task_id"].(string)
		if taskID == "" {
			return mcp.NewToolResultError("task_id is required"), nil
		}
		task, err := s.taskQueueEngine.GetTask(taskID)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		s.logAudit(agentID, "kafka_task_status", args, "", "success", 1, nil, start)
		return marshalResult(task)

	case "stats":
		queue, _ := args["queue"].(string)
		stats := s.taskQueueEngine.Stats(queue)
		s.logAudit(agentID, "kafka_task_status", args, "", "success", len(stats), nil, start)
		return marshalResult(map[string]any{"queues": stats})

	case "list":
		queue, _ := args["queue"].(string)
		filterStatus, _ := args["filter_status"].(string)
		tasks := s.taskQueueEngine.ListTasks(queue, filterStatus, 50)
		s.logAudit(agentID, "kafka_task_status", args, "", "success", len(tasks), nil, start)
		return marshalResult(map[string]any{"tasks": tasks, "total": len(tasks)})

	case "heartbeat":
		taskID, _ := args["task_id"].(string)
		if taskID == "" {
			return mcp.NewToolResultError("task_id is required"), nil
		}
		if err := s.taskQueueEngine.Heartbeat(agentID, taskID); err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		s.logAudit(agentID, "kafka_task_status", args, "", "success", 1, nil, start)
		return marshalResult(map[string]string{"status": "heartbeat_recorded"})

	default:
		return mcp.NewToolResultError("action must be: get, stats, list, or heartbeat"), nil
	}
}

func (s *Server) handleQuotaStatus(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	targetAgent, _ := args["agent_id"].(string)

	if targetAgent != "" {
		status := s.adaptiveRateEngine.GetStatus(targetAgent)
		s.logAudit(agentID, "kafka_quota_status", args, "", "success", 1, nil, start)
		return marshalResult(status)
	}

	allStatus := s.adaptiveRateEngine.AllStatus()
	s.logAudit(agentID, "kafka_quota_status", args, "", "success", len(allStatus), nil, start)
	return marshalResult(map[string]any{"agents": allStatus, "total": len(allStatus)})
}

func (s *Server) handleQuotaRequest(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	action, _ := args["action"].(string)
	targetAgent, _ := args["agent_id"].(string)
	reason, _ := args["reason"].(string)

	if targetAgent == "" {
		targetAgent = agentID
	}

	var amount int
	if v, ok := args["amount"].(float64); ok {
		amount = int(v)
	}

	req := adaptiverate.QuotaRequest{
		AgentID: targetAgent,
		Action:  action,
		Amount:  amount,
		Reason:  reason,
	}

	resp, err := s.adaptiveRateEngine.AdjustQuota(req)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	s.logAudit(agentID, "kafka_quota_request", args, "", "success", 1, nil, start)
	return marshalResult(resp)
}
